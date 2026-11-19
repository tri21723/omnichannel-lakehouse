"""Sample Flink and Kafka while a stream run is processed (ADR-0016 §9, Phase 7 benchmark).

Host-side, stdlib only. Every `--interval` seconds it records, per running job: source records
per second, the maximum backpressured / busy time per second over all operators (and which
operator), and the total Kafka consumer lag of the `flink-*` groups. It stops once publishing is
done (`--until-lag-zero`) and the lag is 0, then adds checkpoint statistics. Writes one JSON file
and prints a summary event; no payloads or credentials are read.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

FLINK = os.environ.get("FLINK_REST", "http://127.0.0.1:8082")


def get(path: str):
    with urllib.request.urlopen(FLINK + path, timeout=10) as response:
        return json.load(response)


def kafka_lag(compose: str) -> int | None:
    command = (
        f"{compose} exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh "
        "--bootstrap-server kafka:29092 --describe --all-groups"
    )
    try:
        out = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=60).stdout
    except subprocess.TimeoutExpired:
        return None
    lag = 0
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0].startswith("flink-") and len(parts) > 5 and parts[5].isdigit():
            lag += int(parts[5])
    return lag


def metric(jid: str, vid: str, names: str) -> dict[str, float]:
    rows = get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get={names}")
    return {row["id"]: float(row.get("max", 0) or 0) for row in rows}


def sample_job(job: dict) -> dict:
    detail = get(f"/jobs/{job['jid']}")
    worst_bp, worst_busy, source_rate = ("", 0.0), ("", 0.0), 0.0
    for vertex in detail["vertices"]:
        values = metric(
            job["jid"],
            vertex["id"],
            "backPressuredTimeMsPerSecond,busyTimeMsPerSecond,numRecordsOutPerSecond",
        )
        name = vertex["name"].split(" -> ")[0][:40]
        if values.get("backPressuredTimeMsPerSecond", 0) > worst_bp[1]:
            worst_bp = (name, values["backPressuredTimeMsPerSecond"])
        if values.get("busyTimeMsPerSecond", 0) > worst_busy[1]:
            worst_busy = (name, values["busyTimeMsPerSecond"])
        if vertex["name"].startswith("Source"):
            rows = get(
                f"/jobs/{job['jid']}/vertices/{vertex['id']}/subtasks/metrics?get=numRecordsOutPerSecond"
            )
            source_rate = sum(float(r.get("sum", 0) or 0) for r in rows)
    return {
        "source_records_per_s": round(source_rate, 1),
        "max_backpressured_ms_per_s": round(worst_bp[1], 1),
        "backpressured_operator": worst_bp[0],
        "max_busy_ms_per_s": round(worst_busy[1], 1),
        "busiest_operator": worst_busy[0],
    }


def checkpoint_stats(jid: str) -> dict:
    data = get(f"/jobs/{jid}/checkpoints")
    summary = data["summary"]
    return {
        "completed": data["counts"]["completed"],
        "failed": data["counts"]["failed"],
        "duration_ms": {k: summary["end_to_end_duration"][k] for k in ("min", "avg", "max")},
        "checkpointed_size_bytes": {
            k: summary["checkpointed_size"][k] for k in ("min", "avg", "max")
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--until-lag-zero", help="file that exists once publishing has finished")
    parser.add_argument("--max-seconds", type=float, default=1800)
    args = parser.parse_args(argv)
    compose = os.environ["STREAM_COMPOSE"]
    started, samples = time.monotonic(), []
    while time.monotonic() - started < args.max_seconds:
        jobs = [j for j in get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        lag = kafka_lag(compose)
        sample = {"t": round(time.monotonic() - started, 1), "kafka_lag": lag, "jobs": {}}
        for job in jobs:
            try:
                sample["jobs"][job["name"]] = sample_job(job)
            except Exception as exc:  # a vertex can vanish while a job restarts
                sample["jobs"][job["name"]] = {"error": type(exc).__name__}
        samples.append(sample)
        published = args.until_lag_zero is None or Path(args.until_lag_zero).exists()
        if published and lag == 0:
            break
        time.sleep(args.interval)
    elapsed = round(time.monotonic() - started, 1)
    jobs = {j["name"]: j["jid"] for j in get("/jobs/overview")["jobs"] if j["state"] == "RUNNING"}
    summary = {"elapsed_s": elapsed, "samples": len(samples), "jobs": {}}
    for name, jid in jobs.items():
        rows = [s["jobs"].get(name, {}) for s in samples]
        rates = [r["source_records_per_s"] for r in rows if "source_records_per_s" in r]
        bps = [r["max_backpressured_ms_per_s"] for r in rows if "max_backpressured_ms_per_s" in r]
        busy = [r["max_busy_ms_per_s"] for r in rows if "max_busy_ms_per_s" in r]
        worst = max(rows, key=lambda r: r.get("max_backpressured_ms_per_s", -1), default={})
        summary["jobs"][name] = {
            "peak_source_records_per_s": max(rates, default=0),
            "peak_backpressured_ms_per_s": max(bps, default=0),
            "samples_backpressured_over_100ms": sum(1 for v in bps if v > 100),
            "backpressured_operator_at_peak": worst.get("backpressured_operator"),
            "peak_busy_ms_per_s": max(busy, default=0),
            "checkpoints": checkpoint_stats(jid),
        }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps({"summary": summary, "samples": samples}, indent=1))
    print(json.dumps({"event": "stream_bench_sampled", **summary}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
