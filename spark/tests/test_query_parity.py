"""Unit tests for Trino ↔ Spark parity helpers (ADR-0015)."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from spark.lib.query_parity import (
    PARITY_FILES,
    canonical,
    canonical_rows,
    for_spark,
    for_trino,
    gold_tables,
    monetary_from_rows,
    warehouse_mirror_sql,
    warehouse_monetary_sql,
)
from spark.lib.sql_params import ParamError

SQL = (
    "SELECT * FROM iceberg.gold.fact_orders AS f JOIN iceberg.gold.dim_product AS p ON 1=1 "
    'LEFT JOIN postgresql.walmart.orders AS o ON 1=1, iceberg.gold."fact_orders$snapshots"'
)


def test_rewrites_tables_for_each_engine_but_not_metadata_tables():
    assert gold_tables(SQL) == {"fact_orders", "dim_product"}
    spark = for_spark(SQL)
    assert "gold_fact_orders AS f" in spark and "walmart_orders AS o" in spark
    trino = for_trino(SQL, {"fact_orders": 11, "dim_product": 22})
    assert "iceberg.gold.fact_orders FOR VERSION AS OF 11 AS f" in trino
    assert "iceberg.gold.dim_product FOR VERSION AS OF 22" in trino
    assert 'iceberg.gold."fact_orders$snapshots"' in trino
    with pytest.raises(KeyError):
        for_trino(SQL, {"fact_orders": 11})


def test_canonical_values_are_engine_independent():
    assert canonical(Decimal("100.10")) == canonical("100.1") == "100.1"
    assert canonical(1500) == canonical(Decimal("1500.00")) == "1500"
    assert canonical(Decimal("-0.00")) == "0"
    assert canonical(date(2026, 1, 3)) == canonical("2026-01-03") == "2026-01-03"
    assert canonical(None) is None and canonical(True) is True
    assert canonical("amazon-s00") == "amazon-s00"
    assert canonical_rows([("b", Decimal("2.0")), ("a", 1)]) == [("a", "1"), ("b", "2")]


def test_warehouse_sql_validates_values_and_follows_slicing_rules():
    snaps = {"fact_orders": 1, "fact_order_items": 2, "fact_returns": 3}
    run = "5f552019ee41c24af95f"
    sql = warehouse_monetary_sql(
        "fact_returns", snaps, run, "2025-12-05T00:00:00+00:00", "2026-01-04T00:00:00+00:00"
    )
    assert "FOR VERSION AS OF 3 AS t" in sql and "t.return_ts >= TIMESTAMP" in sql
    assert "t.order_key IN" in sql and "t.order_item_key IN" in sql
    assert "COALESCE(is_deleted, FALSE) = FALSE" in sql
    assert "'2025-12-05 00:00:00.000000 UTC'" in sql
    with pytest.raises(ParamError):
        warehouse_monetary_sql("fact_orders", snaps, "x' OR 1=1 --", "2025-12-05", "2026-01-04")
    with pytest.raises(ParamError):
        warehouse_mirror_sql("fact_orders", "not-a-run")


def test_monetary_rows_match_ledger_shape():
    rows = [
        {
            "channel": "lazada",
            "currency": "VND",
            "row_count": 3,
            "gross_line_amount": Decimal("75000.00"),
            "net_line_amount": None,
        }
    ]
    assert monetary_from_rows("fact_order_items", rows) == {
        "lazada|VND": {"gross_line_amount": "75000", "net_line_amount": "0"}
    }


def test_parity_files_exist():
    for name in PARITY_FILES:
        assert (Path("sql/trino") / name).is_file()
