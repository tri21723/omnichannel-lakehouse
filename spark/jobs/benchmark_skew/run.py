"""Benchmark orchestrator: preflight, then one spark-submit per trial (ADR-0012 §7).

Runs inside the spark-submit container with plain ``python``. Never runs trials in
parallel. Each trial writes its own JSON artifact; retries keep earlier attempts.
"""

import argparse
import csv
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from spark.lib.benchmark_skew import (
    SCHEMA_VERSION,
    config_hash,
    dataset_manifest_path,
    experiment_dir,
    preserve_previous_attempt,
    summarize_trials,
    trial_plan,
    validate_experiment,
    write_json_atomic,
)
from spark.lib.bronze import event

CONFIG_DIR = Path(__file__).with_name("configs")
TRIAL_SCRIPT = Path(__file__).with_name("trial.py")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
MIN_FREE_BYTES = 10 * 1024**3
MAX_EXPERIMENT_BYTES = 2 * 1024**3


def load_config(name: str) -> dict:
    path = CONFIG_DIR / f"{name}.json"
    if not path.is_file():
        raise ValueError(f"unknown experiment config: {name}")
    return validate_experiment(json.loads(path.read_text()))


def dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def preflight(config: dict) -> dict:
    """Fail closed before any trial: datasets pinned, disk budget, no stray driver."""
    problems = []
    datasets = {}
    for name in config["datasets"]:
        path = dataset_manifest_path(name)
        if not path.exists():
            problems.append(f"dataset manifest missing: {name} (run make benchmark-dataset)")
            continue
        manifest = json.loads(path.read_text())
        datasets[name] = {
            "rows": manifest["distribution"]["rows"],
            "top_key_share": manifest["distribution"]["top_key_share"],
            "gold_publication_id": manifest["gold_publication_id"],
            "snapshots": {k: v["snapshot"] for k, v in manifest["tables"].items()},
        }
    root = experiment_dir(config["experiment_id"]).parents[1]
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free < MIN_FREE_BYTES:
        problems.append(f"free disk {free} < {MIN_FREE_BYTES}")
    if problems:
        raise RuntimeError("; ".join(problems))
    return {
        "datasets": datasets,
        "free_disk_bytes": free,
        "cpu_count": os.cpu_count(),
    }


def run_trial(config: dict, trial: dict, make_reference: bool) -> dict:
    exp_dir = experiment_dir(config["experiment_id"])
    out = exp_dir / "trials" / f"{trial['trial_id']}.json"
    preserve_previous_attempt(out)
    log_path = exp_dir / "logs" / f"{trial['trial_id']}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        SPARK_SUBMIT,
        str(TRIAL_SCRIPT),
        "--experiment",
        config["experiment_id"],
        "--dataset",
        trial["dataset"],
        "--workload",
        trial["workload"],
        "--variant",
        trial["variant"],
        "--phase",
        trial["phase"],
        "--repetition",
        str(trial["repetition"]),
        "--trial-id",
        trial["trial_id"],
    ]
    if make_reference:
        cmd.append("--make-reference")
    started = time.monotonic()
    with log_path.open("w") as log:
        try:
            proc = subprocess.run(
                cmd, stdout=log, stderr=subprocess.STDOUT, timeout=config["timeout_seconds"]
            )
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            rc = None
    wall = round(time.monotonic() - started, 3)
    if rc is None or not out.exists():
        artifact = {
            "schema_version": SCHEMA_VERSION,
            **trial,
            "experiment": config["experiment_id"],
            "status": "timeout" if rc is None else "error",
            "returncode": rc,
            "correct": False,
            "timings": {},
        }
        write_json_atomic(out, artifact)
    artifact = json.loads(out.read_text())
    artifact["wall_s"] = wall
    artifact["returncode"] = rc
    write_json_atomic(out, artifact)
    return artifact


def load_trials(exp_dir: Path) -> list[dict]:
    trials = []
    for path in sorted((exp_dir / "trials").glob("*.json")):
        if ".attempt" in path.name:
            continue
        trials.append(json.loads(path.read_text()))
    return trials


def write_summary(config: dict) -> list[dict]:
    exp_dir = experiment_dir(config["experiment_id"])
    rows = summarize_trials(load_trials(exp_dir))
    write_json_atomic(exp_dir / "summary.json", {"schema_version": SCHEMA_VERSION, "cells": rows})
    if rows:
        with (exp_dir / "summary.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {k: (";".join(v) if isinstance(v, list) else v) for k, v in row.items()}
                )
    return rows


REQUIRED_METRICS = (
    "shuffle_bytes_written",
    "executor_run_time_ms",
    "worst_shuffle_stage_ratio",
)


def verify_experiment(config: dict) -> list[str]:
    """Strict gate: every planned trial recorded, ok, correct, with metrics and plans."""
    exp_dir = experiment_dir(config["experiment_id"])
    trials = {t["trial_id"]: t for t in load_trials(exp_dir)}
    problems = []
    for planned in trial_plan(config):
        trial = trials.get(planned["trial_id"])
        if trial is None:
            problems.append(f"{planned['trial_id']}: missing artifact")
            continue
        if trial.get("status") != "ok" or trial.get("correct") is not True:
            problems.append(f"{planned['trial_id']}: status={trial.get('status')}")
            continue
        metrics = trial.get("metrics") or {}
        if not metrics.get("eventlog_complete") or not metrics.get("matched_jobs"):
            problems.append(f"{planned['trial_id']}: incomplete event log")
        missing = [m for m in REQUIRED_METRICS if metrics.get(m) is None]
        if missing:
            problems.append(f"{planned['trial_id']}: metrics unavailable {missing}")
        if "plan" not in trial:
            problems.append(f"{planned['trial_id']}: no query plan")
    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="config name under configs/")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--resume", action="store_true", help="skip trials already ok")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--verify", action="store_true", help="check artifacts, run nothing")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = load_config(args.config)
    exp_dir = experiment_dir(config["experiment_id"])
    if args.summary_only:
        write_summary(config)
        return 0
    if args.verify:
        problems = verify_experiment(config)
        event(
            "benchmark_verified",
            experiment=config["experiment_id"],
            problems=len(problems),
            details=problems[:20],
        )
        return 1 if problems else 0
    environment = preflight(config)
    event("benchmark_preflight_ok", experiment=config["experiment_id"], **environment["datasets"])
    if args.preflight_only:
        return 0

    plan = trial_plan(config)
    write_json_atomic(
        exp_dir / "experiment.json",
        {
            "schema_version": SCHEMA_VERSION,
            "config": config,
            "config_hash": config_hash(config),
            "commit": os.environ.get("BENCH_COMMIT", "unknown"),
            "image_id": os.environ.get("BENCH_IMAGE_ID", "unknown"),
            "environment": environment,
            "planned_trials": len(plan),
            "started_at_epoch": round(time.time(), 3),
        },
    )
    references: set[tuple[str, str]] = set()
    failures = 0
    for index, trial in enumerate(plan, 1):
        group = (trial["dataset"], trial["workload"])
        out = exp_dir / "trials" / f"{trial['trial_id']}.json"
        if args.resume and out.exists():
            previous = json.loads(out.read_text())
            if previous.get("status") == "ok":
                if trial["variant"] == "B0":
                    references.add(group)
                continue
        make_reference = trial["variant"] == "B0" and group not in references
        if trial["variant"] != "B0" and group not in references:
            raise RuntimeError(f"no B0 reference for {group}; B0 must run first")
        artifact = run_trial(config, trial, make_reference)
        if make_reference and artifact.get("status") == "ok":
            references.add(group)
        if artifact.get("status") != "ok":
            failures += 1
        event(
            "benchmark_progress",
            done=index,
            total=len(plan),
            trial=trial["trial_id"],
            status=artifact.get("status"),
            query_s=artifact.get("timings", {}).get("query_s"),
        )
        used = dir_bytes(exp_dir)
        if used > MAX_EXPERIMENT_BYTES:
            event("benchmark_budget_exceeded", bytes=used)
            failures += 1
            break
    write_summary(config)
    event("benchmark_finished", experiment=config["experiment_id"], failures=failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
