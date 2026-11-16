"""Pinned Gold star-schema reader and 30-day sliding window slicer for Phase 4E."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from spark.lib.dp3_input import (
    DEFAULT_GOLD_ROOT,
    GOLD_TABLES,
    compute_publication_id,
    load_gold_publication,
    read_gold_entities,
    validate_gold_run_membership,
)

EXPORT_TABLES = tuple(GOLD_TABLES)
WINDOW_DAYS = 30


def parse_run_date(value: str | date | datetime) -> date:
    """Parse an ISO YYYY-MM-DD date string or date/datetime object into a UTC date."""
    if isinstance(value, datetime):
        utc_dt = value.astimezone(UTC) if value.tzinfo else value
        return utc_dt.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or len(value) != 10:
        raise ValueError(f"Expected ISO YYYY-MM-DD run_date, got {value!r}")
    return date.fromisoformat(value)


def normalize_utc_timestamp(ts: datetime | str) -> datetime:
    """Normalize a datetime or ISO timestamp string to an explicit UTC datetime."""
    if isinstance(ts, str):
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def compute_export_window(run_date: str | date | datetime) -> tuple[datetime, datetime]:
    """Compute the half-open 30-day UTC window [run_date - 29d, run_date + 1d)."""
    resolved = parse_run_date(run_date)
    w_end = datetime(resolved.year, resolved.month, resolved.day, tzinfo=UTC) + timedelta(days=1)
    w_start = w_end - timedelta(days=WINDOW_DAYS)
    return w_start, w_end


def is_in_export_window(ts: datetime | str, w_start: datetime, w_end: datetime) -> bool:
    """Return True iff timestamp falls inside half-open UTC interval [w_start, w_end)."""
    utc_ts = normalize_utc_timestamp(ts)
    return w_start <= utc_ts < w_end


def read_gold_star_schema(
    spark: Any,
    run_id: str,
    gold_root: str | Path = DEFAULT_GOLD_ROOT,
    publication_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]:
    """Load, verify, and isolate all 8 Gold star-schema tables for run_id."""
    ledger_path = Path(gold_root) / "ledger.json"
    ledger, pub_id = load_gold_publication(ledger_path, publication_id=publication_id)
    run_meta = validate_gold_run_membership(ledger, run_id)
    frames, snapshots, verified_pub_id = read_gold_entities(
        spark, ledger, run_id, publication_id=pub_id, include_deleted=True
    )
    return frames, snapshots, run_meta, verified_pub_id


def resolve_run_date(frames: dict[str, Any], run_date: str | date | datetime | None = None) -> date:
    """Resolve explicit run_date or derive the max UTC order/return date in Gold for this run."""
    if run_date is not None:
        return parse_run_date(run_date)

    from pyspark.sql import functions as F

    max_order_date = frames["fact_orders"].select(F.max(F.to_date("order_ts"))).first()[0]
    max_return_date = frames["fact_returns"].select(F.max(F.to_date("return_ts"))).first()[0]
    candidates = [d for d in (max_order_date, max_return_date) if d is not None]
    if not candidates:
        raise ValueError("Cannot derive run_date: both fact_orders and fact_returns are empty")
    return max(candidates)


def slice_warehouse_window(
    frames: dict[str, Any],
    run_date: str | date | datetime,
) -> tuple[dict[str, Any], datetime, datetime]:
    """Slice fact tables to [run_date - 29d, run_date + 1d) UTC while preserving parent FKs."""
    from pyspark.sql import functions as F

    w_start, w_end = compute_export_window(run_date)
    start_lit = F.lit(w_start.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    end_lit = F.lit(w_end.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    fact_orders = frames["fact_orders"].filter(
        (F.col("order_ts") >= start_lit) & (F.col("order_ts") < end_lit)
    )
    fact_order_items = (
        frames["fact_order_items"]
        .filter((F.col("order_ts") >= start_lit) & (F.col("order_ts") < end_lit))
        .join(fact_orders.select("order_key"), on="order_key", how="left_semi")
    )
    fact_returns = (
        frames["fact_returns"]
        .filter((F.col("return_ts") >= start_lit) & (F.col("return_ts") < end_lit))
        .join(fact_orders.select("order_key"), on="order_key", how="left_semi")
        .join(fact_order_items.select("order_item_key"), on="order_item_key", how="left_semi")
    )

    sliced = {
        "dim_date": frames["dim_date"],
        "dim_channel": frames["dim_channel"],
        "dim_customer": frames["dim_customer"],
        "dim_seller": frames["dim_seller"],
        "dim_product": frames["dim_product"],
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "fact_returns": fact_returns,
    }
    return sliced, w_start, w_end


__all__ = [
    "DEFAULT_GOLD_ROOT",
    "EXPORT_TABLES",
    "WINDOW_DAYS",
    "compute_export_window",
    "compute_publication_id",
    "is_in_export_window",
    "load_gold_publication",
    "normalize_utc_timestamp",
    "parse_run_date",
    "read_gold_star_schema",
    "resolve_run_date",
    "slice_warehouse_window",
    "validate_gold_run_membership",
]
