"""Trino ↔ Spark parity helpers for Phase 6 reconciliation (ADR-0015). Pure Python.

The same sql/trino files run in both engines: for Spark, catalog table references become temp
views registered at the ledger snapshots; for Trino they become ``FOR VERSION AS OF <id>``.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from spark.lib.sql_params import literal

PARITY_FILES = (
    "01_revenue_by_channel_day.sql",
    "02_top_sellers.sql",
    "03_return_rate_by_category.sql",
    "04_federated_walmart_orders.sql",
    "08_label_maturity.sql",
)
GOLD_REF = re.compile(r"\biceberg\.gold\.([a-z_]+)\b(?!\")")
WALMART_REF = re.compile(r"\bpostgresql\.walmart\.([a-z_]+)\b")
AS_OF_DATE = "2026-01-03"

# Warehouse monetary controls (ledger `monetary`), per table: columns summed per channel|currency.
MONETARY = {
    "fact_orders": ("discount_amount", "gross_amount", "net_amount"),
    "fact_order_items": ("gross_line_amount", "net_line_amount"),
    "fact_returns": ("refund_amount",),
}


def gold_tables(sql: str) -> set[str]:
    return set(GOLD_REF.findall(sql))


def for_spark(sql: str) -> str:
    """Catalog references → temp views ``gold_<table>`` / ``walmart_<table>``."""
    sql = GOLD_REF.sub(lambda m: f"gold_{m.group(1)}", sql)
    return WALMART_REF.sub(lambda m: f"walmart_{m.group(1)}", sql)


def for_trino(sql: str, snapshots: dict[str, int]) -> str:
    """Pin every Gold reference to the snapshot recorded in the ledger."""

    def pin(match: re.Match) -> str:
        table = match.group(1)
        if table not in snapshots:
            raise KeyError(f"no pinned snapshot for iceberg.gold.{table}")
        return f"iceberg.gold.{table} FOR VERSION AS OF {int(snapshots[table])}"

    return GOLD_REF.sub(pin, sql)


def canonical(value):
    """Engine-independent value: numbers as normalized decimal strings, dates as ISO text."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, int | float | Decimal):
        number = Decimal(str(value))
    else:
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return str(value)
    text = format(number.normalize(), "f")
    return "0" if text in ("-0", "0") else text


def canonical_rows(rows) -> list[tuple]:
    return sorted((tuple(canonical(v) for v in row) for row in rows), key=repr)


def warehouse_monetary_sql(
    table: str, snapshots: dict[str, int], run_id: str, start: str, end: str
) -> str:
    """Trino SQL reproducing the warehouse slice (ADR-0011) for one table's money columns."""
    columns = ", ".join(f"SUM(t.{c}) AS {c}" for c in MONETARY[table])
    run = literal("dataset_run_id", run_id)
    lo, hi = literal("window_start", start), literal("window_end", end)

    def ref(name: str) -> str:
        return f"iceberg.gold.{name} FOR VERSION AS OF {int(snapshots[name])}"

    orders = (
        f"SELECT order_key FROM {ref('fact_orders')} WHERE _dataset_run_id = {run} "
        f"AND COALESCE(is_deleted, FALSE) = FALSE AND order_ts >= TIMESTAMP {lo} "
        f"AND order_ts < TIMESTAMP {hi}"
    )
    items = (
        f"SELECT order_item_key FROM {ref('fact_order_items')} WHERE _dataset_run_id = {run} "
        f"AND order_ts >= TIMESTAMP {lo} AND order_ts < TIMESTAMP {hi} "
        f"AND order_key IN ({orders})"
    )
    if table == "fact_orders":
        source, ts = ref("fact_orders"), "order_ts"
        extra = " AND COALESCE(t.is_deleted, FALSE) = FALSE"
    elif table == "fact_order_items":
        source, ts, extra = ref("fact_order_items"), "order_ts", f" AND t.order_key IN ({orders})"
    else:
        source, ts = ref("fact_returns"), "return_ts"
        extra = f" AND t.order_key IN ({orders}) AND t.order_item_key IN ({items})"
    return (
        f"SELECT t.channel, t.currency, COUNT(*) AS row_count, {columns} FROM {source} AS t "
        f"WHERE t._dataset_run_id = {run} AND t.{ts} >= TIMESTAMP {lo} "
        f"AND t.{ts} < TIMESTAMP {hi}{extra} GROUP BY t.channel, t.currency"
    )


def warehouse_mirror_sql(table: str, run_id: str) -> str:
    columns = ", ".join(f"SUM(t.{c}) AS {c}" for c in MONETARY[table])
    return (
        f"SELECT t.channel, t.currency, COUNT(*) AS row_count, {columns} "
        f"FROM postgresql.warehouse.{table} AS t "
        f"WHERE t._dataset_run_id = {literal('dataset_run_id', run_id)} "
        f"GROUP BY t.channel, t.currency"
    )


def monetary_from_rows(table: str, rows: list[dict]) -> dict[str, dict[str, str]]:
    """Rows from the SQL above → ledger-shaped ``{"channel|currency": {column: amount}}``."""
    out = {}
    for row in rows:
        out[f"{row['channel']}|{row['currency']}"] = {
            c: canonical(row[c] if row[c] is not None else 0) for c in MONETARY[table]
        }
    return dict(sorted(out.items()))


def canonical_monetary(monetary: dict) -> dict:
    return {
        key: {c: canonical(v) for c, v in sorted(values.items())}
        for key, values in sorted(monetary.items())
    }
