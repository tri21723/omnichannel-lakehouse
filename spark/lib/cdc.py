"""Debezium envelope parsing and the Bronze CDC contract (ADR-0017 §4). Pure Python; Spark only
at runtime. The same function runs as a Spark UDF, so tests cover exactly what the job does."""

from __future__ import annotations

import json
from typing import Any

BRONZE_TABLE = "lh.bronze.walmart_orders_cdc"
TOPIC = "platform_cdc.walmart.orders"
OPS = ("c", "u", "d", "r")
BRONZE_SCHEMA = (
    "_kafka_topic string, _kafka_partition int, _kafka_offset bigint, _kafka_ts timestamp, "
    "_key string, _raw string, order_id string, op string, ts_ms bigint, lsn bigint, "
    "tx_id bigint, before string, after string, _parse_status string, _error_reason string, "
    "_ingest_ts timestamp, _batch_id string, _channel string, _source_type string, _run_date date"
)
PARSED_SCHEMA = (
    "order_id string, op string, ts_ms bigint, lsn bigint, tx_id bigint, before string, "
    "after string, _parse_status string, _error_reason string"
)


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _row_json(value: Any) -> str | None:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) if value is not None else None


def parse_envelope(key: str | None, value: str | None) -> dict:
    """Classify one Kafka record. Never raises: problems become `_parse_status` + reason."""
    result = dict.fromkeys(
        ("order_id", "op", "ts_ms", "lsn", "tx_id", "before", "after", "_error_reason")
    )
    try:
        key_doc = json.loads(key) if key else None
        result["order_id"] = key_doc.get("order_id") if isinstance(key_doc, dict) else None
    except ValueError:
        result.update(_parse_status="invalid", _error_reason="invalid_key")
        return result
    if value is None:
        result["_parse_status"] = "tombstone"  # follows a delete; kept, never applied
        return result
    try:
        event = json.loads(value)
    except ValueError:
        result.update(_parse_status="invalid", _error_reason="invalid_json")
        return result
    if not isinstance(event, dict) or event.get("op") not in OPS:
        result.update(_parse_status="invalid", _error_reason="bad_op")
        return result
    source = event.get("source") if isinstance(event.get("source"), dict) else {}
    result.update(
        op=event["op"],
        ts_ms=_int(event.get("ts_ms")),
        lsn=_int(source.get("lsn")),
        tx_id=_int(source.get("txId")),
        before=_row_json(event.get("before")),
        after=_row_json(event.get("after")),
    )
    image = event.get("before") if event["op"] == "d" else event.get("after")
    if result["lsn"] is None:
        result.update(_parse_status="invalid", _error_reason="missing_lsn")
    elif not isinstance(image, dict) or image.get("order_id") != result["order_id"]:
        result.update(_parse_status="invalid", _error_reason="key_image_mismatch")
    else:
        result["_parse_status"] = "valid"
    return result


def merge_sql(view: str) -> str:
    """Idempotent by Kafka coordinates: a retried or replayed micro-batch inserts nothing new."""
    return (
        f"MERGE INTO {BRONZE_TABLE} t USING {view} s "
        "ON t._kafka_topic = s._kafka_topic AND t._kafka_partition = s._kafka_partition "
        "AND t._kafka_offset = s._kafka_offset WHEN NOT MATCHED THEN INSERT *"
    )


def contiguous(
    offsets_by_partition: dict[int, tuple[int, int, int]], earliest: dict[int, int]
) -> list:
    """Partitions whose Bronze offsets are not one gap-free run starting at Kafka's earliest.

    `offsets_by_partition[p] = (min, max, count)` from Bronze."""
    problems = []
    for partition, (low, high, count) in sorted(offsets_by_partition.items()):
        if count != high - low + 1 or low != earliest.get(partition, low):
            problems.append(partition)
    return problems
