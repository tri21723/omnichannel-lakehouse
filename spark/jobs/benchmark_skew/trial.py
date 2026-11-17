"""Run one benchmark trial in its own Spark application (ADR-0012 §7).

Phases: setup (session, pinned inputs, dimension check), query (full materialization into
the ``noop`` sink, the only timed phase used for speedup), verify (write result table and
compare with the B0 reference). Metrics come from this application's event log, limited to
jobs described ``bench:query``.
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from pathlib import Path

from spark.lib.benchmark_events import event_log_plans, plan_features, summarize_event_log
from spark.lib.benchmark_skew import (
    SCHEMA_VERSION,
    VARIANTS,
    dataset_manifest_path,
    experiment_dir,
    redact_conf,
    table_name,
    variant_conf,
    write_json_atomic,
)
from spark.lib.bronze import event

QUERY_DESCRIPTION = "bench:query"


def run_trial(args, timings: dict, result: dict) -> None:
    from spark.lib.benchmark_workload import (
        build_workload,
        check_dimension_unique,
        check_equivalence,
        check_totals,
        unmatched_lines,
        workload_keys,
    )
    from spark.lib.session import get_spark
    from spark.lib.silver_publish import frame_at, summary

    started = time.monotonic()
    exp_dir = experiment_dir(args.experiment)
    log_dir = exp_dir / "eventlogs"
    log_dir.mkdir(parents=True, exist_ok=True)
    conf = {
        **variant_conf(args.variant),
        "spark.eventLog.enabled": "true",
        "spark.eventLog.compress": "false",
        "spark.eventLog.dir": f"file://{log_dir}",
        "spark.speculation": "false",
    }
    manifest = json.loads(dataset_manifest_path(args.dataset).read_text())
    spark = get_spark(app_name=f"bench-{args.trial_id}", extra_conf=conf)
    app_id = spark.sparkContext.applicationId
    result["app_id"] = app_id
    try:
        sc = spark.sparkContext
        sc.setJobDescription("bench:setup")
        frames = {}
        for kind in ("items", "products"):
            evidence = manifest["tables"][kind]
            frames[kind] = frame_at(spark, evidence["table"], evidence, None)
        check_dimension_unique(frames["products"])
        spec = VARIANTS[args.variant]
        query = build_workload(args.workload, frames["items"], frames["products"], spec)
        result["effective_conf"] = redact_conf(sc.getConf().getAll())
        result["effective_conf"]["spark.sql.shuffle.partitions"] = spark.conf.get(
            "spark.sql.shuffle.partitions"
        )
        result["effective_conf"]["spark.sql.adaptive.enabled"] = spark.conf.get(
            "spark.sql.adaptive.enabled"
        )
        timings["setup_s"] = round(time.monotonic() - started, 3)

        sc.setJobDescription(QUERY_DESCRIPTION)
        query_start = time.monotonic()
        query.write.format("noop").mode("overwrite").save()
        timings["query_s"] = round(time.monotonic() - query_start, 3)

        sc.setJobDescription("bench:verify")
        verify_start = time.monotonic()
        keys = workload_keys(args.workload)
        res_table = table_name("res", args.experiment, args.dataset, args.workload, args.variant)
        ref_table = table_name("ref", args.experiment, args.dataset, args.workload)
        query.writeTo(res_table).using("iceberg").tableProperty(
            "format-version", "2"
        ).createOrReplace()
        if args.make_reference:
            if args.variant != "B0":
                raise ValueError("only B0 may create the reference result")
            spark.table(res_table).writeTo(ref_table).using("iceberg").tableProperty(
                "format-version", "2"
            ).createOrReplace()
        res = spark.table(res_table)
        ref = spark.table(ref_table)
        equivalence = check_equivalence(res, ref, keys)
        totals = check_totals(res, frames["items"])
        unmatched = (
            unmatched_lines(frames["items"], frames["products"])
            if args.workload == "w1_join_agg"
            else 0
        )
        result["correctness"] = {
            "equivalence": equivalence,
            "totals": totals,
            "unmatched_lines": unmatched,
            "result_content": summary(res),
            "reference_content": summary(ref),
        }
        result["correct"] = bool(equivalence["passed"] and totals["passed"] and unmatched == 0)
        timings["verify_s"] = round(time.monotonic() - verify_start, 3)
    finally:
        spark.stop()
    timings["total_s"] = round(time.monotonic() - started, 3)

    log_path = log_dir / app_id
    result["eventlog"] = str(log_path)
    summary_metrics = summarize_event_log(log_path, QUERY_DESCRIPTION)
    plans = event_log_plans(log_path, QUERY_DESCRIPTION)
    plan_dir = exp_dir / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    if plans:
        last = plans[-1]
        (plan_dir / f"{args.trial_id}.initial.txt").write_text(last["initial"])
        (plan_dir / f"{args.trial_id}.final.txt").write_text(last["final"])
        result["plan"] = {
            "initial": plan_features(last["initial"]),
            "final": plan_features(last["final"]),
        }
    result["metrics"] = {
        **summary_metrics["totals"],
        "eventlog_complete": summary_metrics["complete"],
        "matched_jobs": summary_metrics["matched_jobs"],
        "query_stages": len(summary_metrics["stages"]),
    }
    result["stages"] = summary_metrics["stages"]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--workload", required=True, choices=("w1_join_agg", "w2_agg"))
    parser.add_argument("--variant", required=True, choices=sorted(VARIANTS))
    parser.add_argument("--phase", required=True, choices=("warmup", "measure"))
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--make-reference", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    timings: dict = {}
    result: dict = {
        "schema_version": SCHEMA_VERSION,
        "trial_id": args.trial_id,
        "experiment": args.experiment,
        "dataset": args.dataset,
        "workload": args.workload,
        "variant": args.variant,
        "phase": args.phase,
        "repetition": args.repetition,
        "commit": os.environ.get("BENCH_COMMIT", "unknown"),
        "image_id": os.environ.get("BENCH_IMAGE_ID", "unknown"),
        "started_at_epoch": round(time.time(), 3),
        "timings": timings,
        "correct": False,
    }
    manifest_path = dataset_manifest_path(args.dataset)
    status = 0
    try:
        manifest = json.loads(manifest_path.read_text())
        result["dataset_tables"] = {
            kind: {"table": t["table"], "snapshot": t["snapshot"]}
            for kind, t in manifest["tables"].items()
        }
        run_trial(args, timings, result)
        result["status"] = "ok"
        if not result["correct"]:
            result["status"] = "correctness_failed"
            status = 3
    except Exception as exc:  # recorded in the artifact, never silently dropped
        result["status"] = "error"
        result["error"] = {
            "type": type(exc).__name__,
            "trace_tail": traceback.format_exc().splitlines()[-1][:300],
        }
        status = 2
    out = experiment_dir(args.experiment) / "trials" / f"{args.trial_id}.json"
    write_json_atomic(Path(out), result)
    event(
        "benchmark_trial_finished",
        trial=args.trial_id,
        status=result["status"],
        query_s=timings.get("query_s"),
        correct=result["correct"],
    )
    return status


if __name__ == "__main__":
    sys.exit(main())
