"""Unit tests for Phase 4E Gold Iceberg <-> PostgreSQL reconciliation & DQ checks."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark.lib.warehouse_jdbc import INSERT_ORDER
from spark.lib.warehouse_reconcile import (
    PROHIBITED_PII_COLUMNS,
    check_postgres_constraints,
    compare_key_sets,
    compare_monetary_maps,
)


def test_compare_key_sets_detects_missing_and_extra_keys():
    compare_key_sets("fact_orders", {("k1",), ("k2",)}, {("k1",), ("k2",)})

    with pytest.raises(ValueError, match="missing_in_pg=1"):
        compare_key_sets("fact_orders", {("k1",), ("k2",)}, {("k1",)})

    with pytest.raises(ValueError, match="extra_in_pg=1"):
        compare_key_sets("fact_orders", {("k1",)}, {("k1",), ("k_stale",)})


def test_compare_monetary_maps_enforces_exact_cents():
    base = {
        "fact_orders": {
            "walmart|USD": {
                "gross_amount": "100.50",
                "discount_amount": "5.00",
                "net_amount": "95.50",
            }
        },
        "fact_order_items": {
            "walmart|USD": {"gross_line_amount": "100.50", "net_line_amount": "95.50"}
        },
        "fact_returns": {"walmart|USD": {"refund_amount": "10.00"}},
    }
    compare_monetary_maps(base, base)

    off_by_one_cent = {
        **base,
        "fact_returns": {"walmart|USD": {"refund_amount": "10.01"}},
    }
    with pytest.raises(ValueError, match="Monetary mismatch"):
        compare_monetary_maps(base, off_by_one_cent)


def test_check_postgres_constraints_rejects_prohibited_pii_columns():
    assert "full_name" in PROHIBITED_PII_COLUMNS
    assert "email" in PROHIBITED_PII_COLUMNS

    # Build a fake ResultSet returning prohibited column `email` in warehouse.dim_customer
    rows = [("dim_customer", "email", "text", None, None)]
    state = {"idx": -1}

    def next_row():
        state["idx"] += 1
        return state["idx"] < len(rows)

    rs = MagicMock()
    rs.next.side_effect = next_row
    rs.getString.side_effect = lambda col: rows[state["idx"]][col - 1]
    rs.getObject.side_effect = lambda col: rows[state["idx"]][col - 1]

    stmt = MagicMock()
    stmt.executeQuery.return_value = rs
    conn = SimpleNamespace(createStatement=lambda: stmt)

    with pytest.raises(ValueError, match="Prohibited clear PII column email"):
        check_postgres_constraints(conn)


def test_check_postgres_constraints_requires_trino_ro_select_only(monkeypatch):
    # Provide valid 8-table schema rows including exact NUMERIC(18,2) money columns
    schema_rows = []
    for tbl in INSERT_ORDER:
        schema_rows.append((tbl, "_dataset_run_id", "text", None, None))
    for col in ("gross_amount", "discount_amount", "net_amount"):
        schema_rows.append(("fact_orders", col, "numeric", 18, 2))
    for col in ("unit_price", "discount", "gross_line_amount", "net_line_amount"):
        schema_rows.append(("fact_order_items", col, "numeric", 18, 2))
    schema_rows.append(("fact_returns", "refund_amount", "numeric", 18, 2))

    # Build stateful mock ResultSets for each sequential query
    def make_schema_rs():
        idx = {"v": -1}

        def advance():
            idx["v"] += 1
            return idx["v"] < len(schema_rows)

        rs = MagicMock()
        rs.next.side_effect = advance
        rs.getString.side_effect = lambda c: schema_rows[idx["v"]][c - 1]
        rs.getObject.side_effect = lambda c: schema_rows[idx["v"]][c - 1]
        rs.getInt.side_effect = lambda c: int(schema_rows[idx["v"]][c - 1])
        return rs

    def make_bool_rs(vals):
        rs = MagicMock()
        rs.getBoolean.side_effect = lambda c: vals[c - 1]
        return rs

    responses = [
        make_schema_rs(),
        make_bool_rs([True]),  # schema USAGE for trino_ro
        make_bool_rs([True, True, False, False]),  # ERROR: trino_ro has INSERT on dim_date!
    ]
    stmt = MagicMock()
    stmt.executeQuery.side_effect = responses
    conn = SimpleNamespace(createStatement=lambda: stmt)

    with pytest.raises(ValueError, match="Role trino_ro invalid permissions"):
        check_postgres_constraints(conn)
