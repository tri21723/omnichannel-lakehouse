"""Export redacted benchmark results (summary CSV/JSON + Markdown table) for docs/benchmarks.

Only allowlisted numbers, dataset distributions, plan booleans and redacted config are
exported; raw event logs, plans and result tables stay under /runtime/benchmark.
"""

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

from spark.lib.benchmark_skew import (
    SCHEMA_VERSION,
    VARIANTS,
    dataset_manifest_path,
    experiment_dir,
    summarize_trials,
)

PLAN_KEYS = (
    "sort_merge_join",
    "broadcast_hash_join",
    "skew_join",
    "aqe_shuffle_read",
)


def load(experiment: str) -> tuple[dict, list[dict]]:
    exp_dir = experiment_dir(experiment)
    meta = json.loads((exp_dir / "experiment.json").read_text())
    trials = [
        json.loads(p.read_text())
        for p in sorted((exp_dir / "trials").glob("*.json"))
        if ".attempt" not in p.name
    ]
    retries = len(list((exp_dir / "trials").glob("*.attempt*.json")))
    meta["retried_artifacts"] = retries
    return meta, trials


def plan_evidence(trials: list[dict]) -> dict[tuple[str, str, str], dict]:
    """Most common final-plan features per cell over ok measured trials."""
    votes: dict[tuple[str, str, str], Counter] = {}
    for t in trials:
        if t.get("phase") != "measure" or t.get("status") != "ok" or "plan" not in t:
            continue
        key = (t["dataset"], t["workload"], t["variant"])
        features = tuple(t["plan"]["final"].get(k, False) for k in PLAN_KEYS)
        votes.setdefault(key, Counter())[features] += 1
    return {
        key: dict(zip(PLAN_KEYS, counter.most_common(1)[0][0], strict=True))
        for key, counter in votes.items()
    }


def fmt(value, digits=2):
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def short_commit(commit: str | None) -> str:
    commit = commit or "unknown"
    return commit[:12] + ("-dirty" if commit.endswith("-dirty") else "")


def secs(value):
    return "n/a" if value is None else f"{value / 1000:.2f}"


def mib(value):
    return "n/a" if value is None else f"{value / 1024**2:.1f}"


def markdown(meta: dict, rows: list[dict], plans: dict) -> str:
    out = [
        f"### Experiment `{meta['config']['experiment_id']}`",
        "",
        f"- Commit `{short_commit(meta.get('commit'))}`, image "
        f"`{meta.get('image_id', 'unknown')[:19]}`, config hash `{meta['config_hash'][:12]}`.",
        f"- Repetitions {meta['config']['repetitions']} (+{meta['config']['warmups']} warm-up), "
        f"timeout {meta['config']['timeout_seconds']} s, retried artifacts "
        f"{meta['retried_artifacts']}.",
        "",
        "| Dataset | Workload | Variant | ok/total | Query s median (min–max) | Speedup vs B0 "
        "| Paired speedup median (min–max) "
        "| Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB "
        "| Spill MiB (mem/disk) | Final plan |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        plan = plans.get((r["dataset"], r["workload"], r["variant"]), {})
        plan_text = ", ".join(k for k, v in plan.items() if v) or "n/a"
        out.append(
            f"| {r['dataset']} | {r['workload']} | {r['variant']} | "
            f"{r['ok_trials']}/{r['trials']} | {fmt(r['query_s_median'])} "
            f"({fmt(r['query_s_min'])}–{fmt(r['query_s_max'])}) | {fmt(r['speedup_vs_b0'])} | "
            f"{fmt(r['paired_speedup_median'])} "
            f"({fmt(r['paired_speedup_min'])}–{fmt(r['paired_speedup_max'])}) | "
            f"{fmt(r['shuffle_task_ratio_median'])} | {secs(r['max_shuffle_task_ms_median'])} | "
            f"{mib(r['shuffle_bytes_written_median'])} | "
            f"{mib(r['memory_bytes_spilled_median'])}/{mib(r['disk_bytes_spilled_median'])} | "
            f"{plan_text} |"
        )
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)

    meta, trials = load(args.experiment)
    rows = summarize_trials(trials)
    plans = plan_evidence(trials)
    datasets = {}
    for name in meta["config"]["datasets"]:
        manifest = json.loads(dataset_manifest_path(name).read_text())
        datasets[name] = {
            "spec": manifest["spec"],
            "kind": manifest["kind"],
            "gold_publication_id": manifest["gold_publication_id"],
            "distribution": manifest["distribution"],
        }
    redacted_conf = {}
    for t in trials:
        if t.get("effective_conf") and t["variant"] not in redacted_conf:
            redacted_conf[t["variant"]] = t["effective_conf"]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "experiment": meta["config"],
        "commit": meta.get("commit"),
        "image_id": meta.get("image_id"),
        "config_hash": meta["config_hash"],
        "environment": {k: v for k, v in meta["environment"].items() if k != "datasets"},
        "datasets": datasets,
        "variants": {k: VARIANTS[k]["description"] for k in meta["config"]["variants"]},
        "effective_conf": redacted_conf,
        "trials": {
            "planned": meta.get("planned_trials"),
            "recorded": len(trials),
            "statuses": dict(Counter(t.get("status", "unknown") for t in trials)),
            "retried_artifacts": meta["retried_artifacts"],
        },
        "cells": rows,
        "plans": {"|".join(k): v for k, v in plans.items()},
    }
    args.out.mkdir(parents=True, exist_ok=True)
    stem = f"skew-{meta['config']['experiment_id']}"
    (args.out / f"{stem}.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    with (args.out / f"{stem}.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {k: (";".join(v) if isinstance(v, list) else v) for k, v in row.items()}
            )
    (args.out / f"{stem}.md").write_text(markdown(meta, rows, plans))
    print(markdown(meta, rows, plans))
    return 0


if __name__ == "__main__":
    sys.exit(main())
