"""Publish a generated stream to Kafka exactly once per stream run, and read it back.

Order of effects: ground truth (immutable S3 object) → Kafka records in send order → the
`stream_published.json` marker. A rerun with the marker present sends nothing. Ground truth
without a marker means an interrupted publish: Kafka may hold a prefix of the run, so the run is
refused (resending would duplicate records); use a new seed instead.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from generators.batch.dataset import fingerprint
from generators.common.storage import put_immutable, remote_fingerprint
from generators.stream.config import TOPICS, StreamConfig
from generators.stream.events import Send, digest, generate, wire_value


def manifest_keys(run_id: str) -> tuple[str, str]:
    return (
        f"manifests/{run_id}/stream_ground_truth.json",
        f"manifests/{run_id}/stream_published.json",
    )


def write_json(path: Path, document: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n")
    return path


def kafka_producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        enable_idempotence=True,
        max_in_flight_requests_per_connection=5,
        linger_ms=5,
        retries=10,
    )


def publish_stream(
    config: StreamConfig,
    output: Path,
    s3,
    bucket: str,
    producer_factory: Callable[[], object],
    rate: float | None = None,
    clock: Callable[[], float] = time.time,
) -> dict:
    """Publish once; returns a summary. `rate` paces sends (records per wall second)."""
    sends, truth = generate(config)
    run_id = config.stream_run_id
    root = output / "stream" / run_id
    truth_path = write_json(root / "stream_ground_truth.json", truth)
    truth_key, marker_key = manifest_keys(run_id)
    if remote_fingerprint(s3, bucket, marker_key) is not None:
        return {"stream_run_id": run_id, "skipped": True, "records": len(sends)}
    if remote_fingerprint(s3, bucket, truth_key) is not None:
        raise RuntimeError(
            "ground truth exists without a publish marker: an earlier publish was interrupted "
            "and Kafka may hold part of this run; generate a new run (different seed) instead"
        )
    put_immutable(s3, bucket, truth_key, truth_path, fingerprint(truth_path))

    producer = producer_factory()
    errors: list[str] = []
    started = clock()
    try:
        for index, send in enumerate(sends):
            if rate:
                delay = started + index / rate - clock()
                if delay > 0:
                    time.sleep(delay)
            future = producer.send(
                send.topic,
                key=send.key.encode(),
                value=wire_value(send, int(clock() * 1000)),
            )
            future.add_errback(lambda exc: errors.append(type(exc).__name__))
        producer.flush()
    finally:
        producer.close()
    if errors:
        raise RuntimeError(f"{len(errors)} Kafka sends failed: {sorted(set(errors))}")
    finished = clock()
    marker = {
        "stream_run_id": run_id,
        "records": len(sends),
        "per_topic": {TOPICS[c]: truth["channels"][c]["records_sent"] for c in config.channels},
        "started_ms": int(started * 1000),
        "finished_ms": int(finished * 1000),
        "rate": rate,
    }
    marker_path = write_json(root / "stream_published.json", marker)
    put_immutable(s3, bucket, marker_key, marker_path, fingerprint(marker_path))
    return {
        "stream_run_id": run_id,
        "skipped": False,
        "records": len(sends),
        "seconds": round(finished - started, 3),
    }


def summarize(records: list[tuple[str, bytes | None, bytes]], run_id: str) -> dict:
    """Count one run's records per topic, from (topic, key, value) as read from Kafka."""
    stats: dict[str, dict] = defaultdict(
        lambda: {"valid_records": 0, "malformed": 0, "event_ids": set(), "retries": []}
    )
    for topic, key, value in records:
        key_text = key.decode() if key else ""
        if key_text.startswith(f"{run_id}-") and "-malformed-" in key_text:
            stats[topic]["malformed"] += 1
            continue
        try:
            event = json.loads(value)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("stream_run_id") != run_id:
            continue
        entry = stats[topic]
        entry["valid_records"] += 1
        if event["event_id"] in entry["event_ids"]:
            entry["retries"].append(event["event_id"])
        entry["event_ids"].add(event["event_id"])
    return {
        topic: {
            "valid_records": entry["valid_records"],
            "malformed": entry["malformed"],
            "unique_events": len(entry["event_ids"]),
            "retries": len(entry["retries"]),
            "retry_event_id_sha256": digest(entry["retries"]),
        }
        for topic, entry in stats.items()
    }


def expected_summary(truth: dict) -> dict:
    return {
        data["topic"]: {
            "valid_records": data["valid_records"],
            "malformed": data["malformed"],
            "unique_events": data["unique_events"],
            "retries": data["retries"],
            "retry_event_id_sha256": data["retry_event_id_sha256"],
        }
        for data in truth["channels"].values()
    }


def read_topics(bootstrap: str, topics: list[str], timeout_s: float = 60.0) -> list[tuple]:
    """Read every record of `topics` from the beginning up to the end offsets at call time."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
    )
    try:
        partitions = [
            TopicPartition(topic, p)
            for topic in topics
            for p in consumer.partitions_for_topic(topic)
        ]
        consumer.assign(partitions)
        consumer.seek_to_beginning(*partitions)
        end = consumer.end_offsets(partitions)
        records, deadline = [], time.monotonic() + timeout_s
        while any(consumer.position(tp) < end[tp] for tp in partitions):
            if time.monotonic() > deadline:
                raise TimeoutError("Kafka read did not reach the end offsets")
            for tp, batch in consumer.poll(timeout_ms=1000).items():
                records.extend((tp.topic, r.key, r.value) for r in batch if r.offset < end[tp])
        return records
    finally:
        consumer.close()


def check_stream(config: StreamConfig, bootstrap: str) -> dict:
    """Compare what Kafka holds for this run with the ground truth (read-only)."""
    _, truth = generate(config)
    topics = sorted(TOPICS[c] for c in config.channels)
    actual = summarize(read_topics(bootstrap, topics), config.stream_run_id)
    expected = expected_summary(truth)
    return {"ok": actual == expected, "expected": expected, "actual": actual}


__all__ = ["Send", "check_stream", "publish_stream", "summarize", "kafka_producer"]
