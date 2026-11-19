"""Read-only reconciliation of Flink output with the stream ground truth (ADR-0016 §9).

Inputs: the ground truth in S3, committed output files under `raw/stream/<channel>/<branch>/
run=<id>/` and the DLQ topics (read_committed). Hidden (in-progress) files must not exist.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime

from generators.stream.config import (
    EVENT_TIME,
    TOPICS,
    WATERMARK_BOUND_MS,
    StreamConfig,
    classify,
)
from generators.stream.events import digest, generate, iso_ms

BRANCHES = ("accepted", "late", "duplicates", "windows")


def ts_ms(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def read_json(s3, bucket: str, key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    try:
        return json.loads(body.read())
    finally:
        body.close()


def read_branch(s3, bucket: str, channel: str, branch: str, run_id: str) -> tuple[list[dict], dict]:
    prefix = f"stream/{channel}/{branch}/run={run_id}/"
    lines, files, hidden = [], 0, 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].rsplit("/", 1)[-1].startswith("."):
                hidden += 1
                continue
            files += 1
            body = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"]
            try:
                lines.extend(json.loads(line) for line in body.read().splitlines() if line.strip())
            finally:
                body.close()
    return lines, {"files": files, "hidden_files": hidden, "rows": len(lines)}


def final_windows(rows: list[dict]) -> list[dict]:
    """Last firing per window (late firings replace earlier ones)."""
    latest: dict[str, dict] = {}
    for row in rows:
        start = row["window_start"]
        if start not in latest or row["firing_seq"] > latest[start]["firing_seq"]:
            latest[start] = row
    return [
        {"window_start": iso_ms(ts_ms(start)), "events": row["events"], "amount": row["amount"]}
        for start, row in sorted(latest.items(), key=lambda kv: ts_ms(kv[0]))
    ]


def rule_violations(rows: list[dict], channel: str, branch: str) -> int:
    window_ms, lateness_ms = EVENT_TIME[channel]
    bad = 0
    for row in rows:
        expected = classify(ts_ms(row["event_ts"]), row["_watermark_ms"], window_ms, lateness_ms)
        if branch == "late":
            bad += expected != "too_late" or row.get("_delay_class") != "too_late"
        else:
            bad += expected == "too_late" or row.get("_delay_class") != expected
    return bad


def check_channel(channel: str, truth: dict, branches: dict, dlq_count: int) -> list[dict]:
    accepted, late, dups, windows = (branches[b][0] for b in BRANCHES)
    stats = {b: branches[b][1] for b in BRANCHES}
    by_class: dict[str, list[str]] = defaultdict(list)
    for row in accepted:
        by_class[row["_delay_class"]].append(row["event_id"])
    accepted_ids = [row["event_id"] for row in accepted]
    expect = truth["event_id_sha256"]
    checks = [
        ("committed_files_only", all(s["hidden_files"] == 0 for s in stats.values())),
        ("accepted_unique", len(accepted_ids) == len(set(accepted_ids))),
        ("on_time_ids", digest(by_class["on_time"]) == expect.get("on_time", digest([]))),
        (
            "late_accepted_ids",
            digest(by_class["late_accepted"]) == expect.get("late_accepted", digest([])),
        ),
        (
            "too_late_ids",
            digest([r["event_id"] for r in late]) == expect.get("too_late", digest([])),
        ),
        ("duplicates", len(dups) == truth["retries"]),
        (
            "duplicate_ids",
            digest([r["event_id"] for r in dups]) == truth["retry_event_id_sha256"],
        ),
        ("rule_per_record", rule_violations(accepted, channel, "accepted") == 0),
        ("rule_per_late_record", rule_violations(late, channel, "late") == 0),
        ("windows_final", final_windows(windows) == truth["windows"]),
        ("dlq", dlq_count == truth["malformed"]),
        ("nothing_lost", len(accepted) + len(late) + len(dups) == truth["valid_records"]),
    ]
    counts = {
        "accepted": len(accepted),
        "late": len(late),
        "duplicates": len(dups),
        "window_firings": len(windows),
        "dlq": dlq_count,
        "on_time": len(by_class["on_time"]),
        "late_accepted": len(by_class["late_accepted"]),
    }
    return [{"channel": channel, "check": name, "ok": bool(ok)} for name, ok in checks] + [
        {"channel": channel, "check": "counts", "ok": True, **counts}
    ]


def dlq_counts(bootstrap: str, run_id: str) -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        isolation_level="read_committed",
        consumer_timeout_ms=10_000,
    )
    try:
        counts = {}
        for channel, topic in TOPICS.items():
            partitions = [
                TopicPartition(f"{topic}.dlq", p)
                for p in consumer.partitions_for_topic(f"{topic}.dlq")
            ]
            consumer.assign(partitions)
            consumer.seek_to_beginning(*partitions)
            end = consumer.end_offsets(partitions)
            n = 0
            while any(consumer.position(tp) < end[tp] for tp in partitions):
                for tp, batch in consumer.poll(timeout_ms=1000).items():
                    n += sum(
                        1
                        for r in batch
                        if r.offset < end[tp] and (r.key or b"").decode().startswith(f"{run_id}-")
                    )
                if not consumer.assignment():
                    break
            counts[channel] = n
        return counts
    finally:
        consumer.close()


def watermark_lag(rows: list[dict], send_ms: dict[str, int]) -> dict:
    """Seconds the recorded watermark trailed the ideal one (send time - bound); diagnostic."""
    lags = sorted(
        send_ms[r["event_id"]] - WATERMARK_BOUND_MS - r["_watermark_ms"]
        for r in rows
        if r["_watermark_ms"] > -(2**62) and r["event_id"] in send_ms
    )
    if not lags:
        return {"watermark_lag_p50_s": None, "watermark_lag_max_s": None}
    return {
        "watermark_lag_p50_s": round(lags[len(lags) // 2] / 1000, 1),
        "watermark_lag_max_s": round(lags[-1] / 1000, 1),
    }


def verify_stream(run_id: str, s3, bucket: str, bootstrap: str) -> dict:
    truth = read_json(s3, bucket, f"manifests/{run_id}/stream_ground_truth.json")
    sends, regenerated = generate(StreamConfig.model_validate(truth["config"]))
    if regenerated != truth:
        raise ValueError("ground truth in S3 differs from the deterministic generator output")
    send_ms = {s.event_id: s.send_ms for s in sends if s.kind == "event"}
    dlq = dlq_counts(bootstrap, run_id)
    results = []
    for channel, channel_truth in sorted(truth["channels"].items()):
        branches = {b: read_branch(s3, bucket, channel, b, run_id) for b in BRANCHES}
        checks = check_channel(channel, channel_truth, branches, dlq.get(channel, 0))
        checks[-1].update(watermark_lag(branches["accepted"][0] + branches["late"][0], send_ms))
        results.extend(checks)
    return {"stream_run_id": run_id, "ok": all(r["ok"] for r in results), "checks": results}
