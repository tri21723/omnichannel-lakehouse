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


# --- Silver current state (ADR-0017 §5) -------------------------------------------------------
SILVER_TABLE = "lh.silver.walmart_orders_cdc_current"
QUARANTINE_TABLE = "lh.silver.quarantine_walmart_orders_cdc"
TYPED_SCHEMA = (
    "order_id string, customer_id string, order_ts string, updated_ts string, status string, "
    "currency string, total_amount string, shipping_country string, _error_reason string"
)
SILVER_SCHEMA = (
    "order_id string, customer_id string, order_ts timestamp, updated_ts timestamp, "
    "status string, currency string, total_amount decimal(18,2), shipping_country string, "
    "is_deleted boolean, _cdc_op string, _cdc_lsn bigint, _cdc_ts_ms bigint, "
    "_kafka_partition int, _kafka_offset bigint, _updated_at timestamp"
)
QUARANTINE_SCHEMA = (
    "_kafka_topic string, _kafka_partition int, _kafka_offset bigint, order_id string, "
    "op string, _raw string, _error_reason string, _quarantined_at timestamp"
)
REQUIRED = ("order_id", "customer_id", "order_ts", "updated_ts", "status", "currency")


def _utc(value: Any) -> str | None:
    from datetime import UTC, datetime

    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def typed_row(op: str | None, before: str | None, after: str | None) -> dict:
    """Business columns of the row image a change leaves behind (`before` for deletes).

    Values stay strings so Spark casts them exactly (DECIMAL(18,2), UTC timestamp); any problem
    is returned as `_error_reason` for quarantine instead of a null that looks valid."""
    out = dict.fromkeys(TYPED_SCHEMA.replace(" string", "").split(", "))
    image_text = before if op == "d" else after
    try:
        image = json.loads(image_text) if image_text else None
    except ValueError:
        image = None
    if not isinstance(image, dict):
        out["_error_reason"] = "missing_row_image"
        return out
    for name in ("order_id", "customer_id", "status", "currency", "shipping_country"):
        out[name] = image.get(name) if isinstance(image.get(name), str) else None
    out["order_ts"], out["updated_ts"] = _utc(image.get("order_ts")), _utc(image.get("updated_ts"))
    amount = image.get("total_amount")
    try:
        from decimal import Decimal, InvalidOperation

        value = Decimal(amount) if isinstance(amount, str) else None
        if (
            value is None
            or value != value.quantize(Decimal("0.01"))
            or value < 0
            or abs(value) >= 10**16
        ):
            raise InvalidOperation
        out["total_amount"] = f"{value:.2f}"
    except (InvalidOperation, ValueError):
        out["_error_reason"] = "bad_amount"
        return out
    missing = [name for name in REQUIRED if out[name] is None]
    if missing:
        out["_error_reason"] = f"bad_{missing[0]}"
    return out


def newer(candidate: tuple[int, int], current: tuple[int, int] | None) -> bool:
    """Ordering key (lsn, kafka offset), the same rule as `silver_merge_sql`: only a strictly newer
    event may replace the stored state. All events of one key share a partition, so the offset
    breaks LSN ties (duplicate deliveries of the same change)."""
    return current is None or candidate > current


def silver_merge_sql(view: str) -> str:
    return (
        f"MERGE INTO {SILVER_TABLE} t USING {view} s ON t.order_id = s.order_id "
        "WHEN MATCHED AND (s._cdc_lsn > t._cdc_lsn OR (s._cdc_lsn = t._cdc_lsn "
        "AND s._kafka_offset > t._kafka_offset)) THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def scenario_checksum(rows) -> str:
    """Same algorithm as generators.cdc.scenario.checksum (cross-tested): SHA-256 over
    `order_id|status|total_amount|updated_ts` (UTC, microseconds, `Z`) in order_id order."""
    import hashlib
    from datetime import UTC, datetime

    def ts(value) -> str:
        if not isinstance(value, datetime):
            value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)  # Spark session time zone is UTC
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    lines = [
        f"{r['order_id']}|{r['status']}|{r['total_amount']}|{ts(r['updated_ts'])}"
        for r in sorted(rows, key=lambda r: r["order_id"])
    ]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()
