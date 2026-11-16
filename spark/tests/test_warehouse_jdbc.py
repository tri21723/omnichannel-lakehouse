"""Unit tests for Phase 4E single-connection transactional JDBC warehouse writer."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark.lib.warehouse_jdbc import (
    DEFAULT_DDL_PATH,
    DELETE_ORDER,
    INSERT_ORDER,
    TABLE_COLUMNS,
    WarehouseFaultInjectionError,
    bind_jdbc_value,
    build_insert_sql,
    parse_ddl_statements,
    replace_run_slice,
    to_utc_micros,
)


def _fake_jvm():
    sql_types = SimpleNamespace(
        VARCHAR=12,
        INTEGER=4,
        BIGINT=-5,
        BOOLEAN=16,
        NUMERIC=2,
        DATE=91,
        TIMESTAMP_WITH_TIMEZONE=2014,
    )
    return SimpleNamespace(
        java=SimpleNamespace(
            sql=SimpleNamespace(
                Types=sql_types,
                Date=SimpleNamespace(valueOf=lambda s: ("DATE", s)),
            ),
            math=SimpleNamespace(BigDecimal=lambda s: ("DECIMAL", s)),
            time=SimpleNamespace(
                ZoneOffset=SimpleNamespace(UTC="UTC"),
                Instant=SimpleNamespace(ofEpochSecond=lambda sec, nanos: ("INSTANT", sec, nanos)),
                OffsetDateTime=SimpleNamespace(ofInstant=lambda inst, tz: ("ODT", inst, tz)),
            ),
        )
    )


def test_fk_table_order_and_ddl_alignment():
    # Parent dimensions must precede child dimensions and fact tables
    assert INSERT_ORDER.index("dim_seller") < INSERT_ORDER.index("dim_product")
    assert INSERT_ORDER.index("dim_product") < INSERT_ORDER.index("fact_order_items")
    assert INSERT_ORDER.index("fact_orders") < INSERT_ORDER.index("fact_order_items")
    assert INSERT_ORDER.index("fact_order_items") < INSERT_ORDER.index("fact_returns")
    assert DELETE_ORDER == tuple(reversed(INSERT_ORDER))

    statements = parse_ddl_statements(DEFAULT_DDL_PATH)
    assert len(statements) >= 17  # 8 CREATE TABLE + 9 CREATE INDEX statements

    sql_text = build_insert_sql("fact_orders")
    assert sql_text.startswith("INSERT INTO warehouse.fact_orders (")
    assert sql_text.count("?") == len(TABLE_COLUMNS["fact_orders"])


def test_bind_jdbc_value_preserves_decimal_date_bool_and_utc_timestamp():
    ps = MagicMock()
    jvm = _fake_jvm()

    bind_jdbc_value(ps, jvm, 1, Decimal("123.4"), "decimal")
    ps.setBigDecimal.assert_called_once_with(1, ("DECIMAL", "123.40"))

    bind_jdbc_value(ps, jvm, 2, date(2026, 1, 17), "date")
    ps.setDate.assert_called_once_with(2, ("DATE", "2026-01-17"))

    bind_jdbc_value(ps, jvm, 3, True, "bool")
    ps.setBoolean.assert_called_once_with(3, True)

    bind_jdbc_value(ps, jvm, 4, None, "str")
    ps.setNull.assert_called_once_with(4, 12)

    dt = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    micros = to_utc_micros(dt)
    bind_jdbc_value(ps, jvm, 5, micros, "timestamptz")
    expected_sec, expected_usec = divmod(micros, 1_000_000)
    ps.setObject.assert_called_once_with(
        5, ("ODT", ("INSTANT", expected_sec, expected_usec * 1000), "UTC")
    )


def _sample_payload(run_id: str) -> dict[str, list[tuple]]:
    return {
        "dim_date": [(run_id, 20260101, "2026-01-01", 2026, 1, 1, 1, 4, False)],
        "dim_channel": [(run_id, "ck", "walmart", "Walmart", "batch_jdbc")],
        "dim_customer": [
            (
                "custk",
                "walmart",
                "c1",
                "nh",
                "eh",
                "US",
                1767225600000000,
                1767225600000000,
                "local-v1",
                run_id,
                "spub",
                1,
                "rh",
                "gold-policy-v1",
            )
        ],
        "dim_seller": [("sellk", "walmart", "s1", run_id, "spub", "rh", "gold-policy-v1")],
        "dim_product": [
            (
                "prodk",
                "sellk",
                "walmart",
                "p1",
                "s1",
                "Widget",
                "Tools",
                None,
                1767225600000000,
                run_id,
                "spub",
                1,
                "rh",
                "gold-policy-v1",
            )
        ],
        "fact_orders": [
            (
                "ordk",
                20260101,
                "ck",
                "custk",
                "walmart",
                "o1",
                "c1",
                1767225600000000,
                1767225600000000,
                "paid",
                "USD",
                "10.00",
                "0.00",
                "10.00",
                1,
                1,
                "US",
                False,
                None,
                run_id,
                "spub",
                1,
                "rh",
                "gold-policy-v1",
            )
        ],
        "fact_order_items": [
            (
                "oik",
                "ordk",
                20260101,
                "ck",
                "custk",
                "prodk",
                "sellk",
                "walmart",
                "o1",
                1,
                "p1",
                "s1",
                1767225600000000,
                1767225600000000,
                "paid",
                "USD",
                1,
                "10.00",
                "0.00",
                "10.00",
                "10.00",
                run_id,
                "spub",
                1,
                "rh",
                "gold-policy-v1",
            )
        ],
        "fact_returns": [],
    }


def test_replace_run_slice_commits_on_success_and_rollbacks_on_fault(monkeypatch, tmp_path: Path):
    import spark.lib.warehouse_jdbc as wj

    ddl = tmp_path / "ddl.sql"
    ddl.write_text("CREATE TABLE IF NOT EXISTS warehouse.t (id int);", encoding="utf-8")

    conn = MagicMock()
    conn.isClosed.return_value = False
    rs = MagicMock()
    # Count order matches INSERT_ORDER: 1 for first 7 tables, 0 for fact_returns
    rs.getLong.side_effect = [1, 1, 1, 1, 1, 1, 1, 0]
    ps = MagicMock()
    ps.executeQuery.return_value = rs
    conn.prepareStatement.return_value = ps

    spark = SimpleNamespace(sparkContext=SimpleNamespace(_jvm=_fake_jvm()))
    monkeypatch.setattr(wj, "writer_connection", lambda *_args, **_kwargs: conn)

    payload = _sample_payload("run_a")
    counts = replace_run_slice(spark, payload, "run_a", ddl_path=ddl)
    assert counts["fact_orders"] == 1
    assert counts["fact_returns"] == 0
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()

    # Now inject fault after dimensions: must rollback and never commit
    conn.reset_mock()
    conn.isClosed.return_value = False
    with pytest.raises(WarehouseFaultInjectionError, match="after dimensions"):
        replace_run_slice(spark, payload, "run_a", fail_after="dimensions", ddl_path=ddl)
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()
    conn.close.assert_called_once()
