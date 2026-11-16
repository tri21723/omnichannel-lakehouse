"""Bi-directional Gold Iceberg <-> PostgreSQL warehouse.* reconciliation and DQ checks."""

from decimal import Decimal
from pathlib import Path
from typing import Any

from spark.lib.bronze_jdbc import rollback_close
from spark.lib.warehouse_input import (
    DEFAULT_GOLD_ROOT,
    read_gold_star_schema,
    slice_warehouse_window,
)
from spark.lib.warehouse_jdbc import (
    INSERT_ORDER,
    MONETARY_GROUP_EXPRESSIONS,
    collect_slice_payload,
    query_run_monetary_totals,
    writer_connection,
)

PROHIBITED_PII_COLUMNS = frozenset(
    {"full_name", "email", "_raw", "phone", "phone_number", "address", "street_address"}
)

PK_COLUMNS: dict[str, tuple[str, ...]] = {
    "dim_date": ("_dataset_run_id", "date_key"),
    "dim_channel": ("_dataset_run_id", "channel_key"),
    "dim_customer": ("customer_key",),
    "dim_seller": ("seller_key",),
    "dim_product": ("product_key",),
    "fact_orders": ("order_key",),
    "fact_order_items": ("order_item_key",),
    "fact_returns": ("return_key",),
}

EXPECTED_NUMERIC_COLUMNS: dict[str, tuple[str, ...]] = {
    "fact_orders": ("gross_amount", "discount_amount", "net_amount"),
    "fact_order_items": ("unit_price", "discount", "gross_line_amount", "net_line_amount"),
    "fact_returns": ("refund_amount",),
}

ORPHAN_FK_CHECKS: tuple[tuple[str, str], ...] = (
    (
        "dim_product->dim_seller",
        "SELECT COUNT(*) FROM warehouse.dim_product p "
        "LEFT JOIN warehouse.dim_seller s ON s.seller_key = p.seller_key "
        "WHERE s.seller_key IS NULL",
    ),
    (
        "fact_orders->dim_customer",
        "SELECT COUNT(*) FROM warehouse.fact_orders o "
        "LEFT JOIN warehouse.dim_customer c ON c.customer_key = o.customer_key "
        "WHERE c.customer_key IS NULL",
    ),
    (
        "fact_orders->dim_date",
        "SELECT COUNT(*) FROM warehouse.fact_orders o "
        "LEFT JOIN warehouse.dim_date d "
        "ON d._dataset_run_id = o._dataset_run_id AND d.date_key = o.order_date_key "
        "WHERE d.date_key IS NULL",
    ),
    (
        "fact_orders->dim_channel",
        "SELECT COUNT(*) FROM warehouse.fact_orders o "
        "LEFT JOIN warehouse.dim_channel ch "
        "ON ch._dataset_run_id = o._dataset_run_id AND ch.channel_key = o.channel_key "
        "WHERE ch.channel_key IS NULL",
    ),
    (
        "fact_order_items->fact_orders",
        "SELECT COUNT(*) FROM warehouse.fact_order_items i "
        "LEFT JOIN warehouse.fact_orders o ON o.order_key = i.order_key "
        "WHERE o.order_key IS NULL",
    ),
    (
        "fact_order_items->dim_product",
        "SELECT COUNT(*) FROM warehouse.fact_order_items i "
        "LEFT JOIN warehouse.dim_product p ON p.product_key = i.product_key "
        "WHERE p.product_key IS NULL",
    ),
    (
        "fact_order_items->dim_seller",
        "SELECT COUNT(*) FROM warehouse.fact_order_items i "
        "LEFT JOIN warehouse.dim_seller s ON s.seller_key = i.seller_key "
        "WHERE s.seller_key IS NULL",
    ),
    (
        "fact_returns->fact_orders",
        "SELECT COUNT(*) FROM warehouse.fact_returns r "
        "LEFT JOIN warehouse.fact_orders o ON o.order_key = r.order_key "
        "WHERE o.order_key IS NULL",
    ),
    (
        "fact_returns->fact_order_items",
        "SELECT COUNT(*) FROM warehouse.fact_returns r "
        "LEFT JOIN warehouse.fact_order_items i ON i.order_item_key = r.order_item_key "
        "WHERE i.order_item_key IS NULL",
    ),
    (
        "fact_returns->dim_date_return",
        "SELECT COUNT(*) FROM warehouse.fact_returns r "
        "LEFT JOIN warehouse.dim_date d "
        "ON d._dataset_run_id = r._dataset_run_id AND d.date_key = r.return_date_key "
        "WHERE d.date_key IS NULL",
    ),
)


def compare_key_sets(
    table_name: str, expected_keys: set[tuple[Any, ...]], actual_keys: set[tuple[Any, ...]]
) -> None:
    """Assert exact primary-key set parity between Gold Iceberg slice and PostgreSQL."""
    missing = expected_keys - actual_keys
    extra = actual_keys - expected_keys
    if missing or extra:
        raise ValueError(
            f"Key mismatch for warehouse.{table_name}: "
            f"missing_in_pg={len(missing)} (sample={sorted(missing)[:3]}), "
            f"extra_in_pg={len(extra)} (sample={sorted(extra)[:3]})"
        )


def compare_monetary_maps(
    expected: dict[str, dict[str, dict[str, str]]],
    actual: dict[str, dict[str, dict[str, str]]],
) -> None:
    """Assert exact cent-level NUMERIC(18,2) parity across every (table, channel, currency)."""
    for table_name in MONETARY_GROUP_EXPRESSIONS:
        exp_table = expected.get(table_name, {})
        act_table = actual.get(table_name, {})
        if set(exp_table.keys()) != set(act_table.keys()):
            raise ValueError(
                f"Currency/channel group mismatch in {table_name}: "
                f"expected {sorted(exp_table.keys())}, got {sorted(act_table.keys())}"
            )
        for group_key, exp_cols in exp_table.items():
            act_cols = act_table[group_key]
            for col_name, exp_val in exp_cols.items():
                exp_dec = Decimal(str(exp_val)).quantize(Decimal("0.01"))
                act_dec = Decimal(str(act_cols[col_name])).quantize(Decimal("0.01"))
                if exp_dec != act_dec:
                    raise ValueError(
                        f"Monetary mismatch in {table_name}[{group_key}].{col_name}: "
                        f"expected {exp_dec}, got {act_dec}"
                    )


def check_postgres_constraints(conn: Any) -> dict[str, Any]:
    """Validate warehouse.* physical schema, PII absence, trino_ro privileges, and zero orphans."""
    stmt = conn.createStatement()
    try:
        # 1. All 8 tables exist and zero prohibited PII columns appear
        rs = stmt.executeQuery(
            "SELECT table_name, column_name, data_type, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            "WHERE table_schema = 'warehouse' "
            "ORDER BY table_name, ordinal_position"
        )
        schema_cols: dict[str, dict[str, tuple[str, int | None, int | None]]] = {}
        try:
            while rs.next():
                tname = rs.getString(1)
                cname = rs.getString(2)
                if cname in PROHIBITED_PII_COLUMNS:
                    raise ValueError(f"Prohibited clear PII column {cname} in warehouse.{tname}")
                prec = rs.getInt(4) if rs.getObject(4) is not None else None
                scale = rs.getInt(5) if rs.getObject(5) is not None else None
                schema_cols.setdefault(tname, {})[cname] = (rs.getString(3), prec, scale)
        finally:
            rs.close()

        missing_tables = set(INSERT_ORDER) - set(schema_cols.keys())
        if missing_tables:
            raise ValueError(f"Missing tables in PostgreSQL warehouse schema: {missing_tables}")

        for table_name, money_cols in EXPECTED_NUMERIC_COLUMNS.items():
            for col_name in money_cols:
                dtype, prec, scale = schema_cols[table_name].get(col_name, (None, None, None))
                if dtype != "numeric" or prec != 18 or scale != 2:
                    raise ValueError(
                        f"Column warehouse.{table_name}.{col_name} must be NUMERIC(18,2), "
                        f"got {dtype}({prec},{scale})"
                    )

        # 2. Validate trino_ro least-privilege schema and table permissions
        rs = stmt.executeQuery("SELECT has_schema_privilege('trino_ro', 'warehouse', 'USAGE')")
        try:
            rs.next()
            if not rs.getBoolean(1):
                raise ValueError("Role trino_ro lacks USAGE on schema warehouse")
        finally:
            rs.close()

        for table_name in INSERT_ORDER:
            rs = stmt.executeQuery(
                f"SELECT has_table_privilege('trino_ro', 'warehouse.{table_name}', 'SELECT'), "
                f"has_table_privilege('trino_ro', 'warehouse.{table_name}', 'INSERT'), "
                f"has_table_privilege('trino_ro', 'warehouse.{table_name}', 'UPDATE'), "
                f"has_table_privilege('trino_ro', 'warehouse.{table_name}', 'DELETE')"
            )
            try:
                rs.next()
                can_select = rs.getBoolean(1)
                can_write = rs.getBoolean(2) or rs.getBoolean(3) or rs.getBoolean(4)
                if not can_select or can_write:
                    raise ValueError(
                        f"Role trino_ro invalid permissions on warehouse.{table_name}: "
                        f"select={can_select}, write={can_write}"
                    )
            finally:
                rs.close()

        # 3. Validate 0 orphan foreign keys
        for check_name, sql in ORPHAN_FK_CHECKS:
            rs = stmt.executeQuery(sql)
            try:
                rs.next()
                orphans = int(rs.getLong(1))
                if orphans != 0:
                    raise ValueError(f"Orphan foreign keys detected in {check_name}: {orphans}")
            finally:
                rs.close()

        # 4. Validate zero blank/clear PII values in dim_customer hashes
        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM warehouse.dim_customer "
            "WHERE length(name_hash) <> 64 OR length(email_hash) <> 64 "
            "OR coalesce(_pii_key_version, '') = ''"
        )
        try:
            rs.next()
            invalid_pii = int(rs.getLong(1))
            if invalid_pii != 0:
                raise ValueError(f"Invalid PII hashes found in dim_customer: {invalid_pii}")
        finally:
            rs.close()
    finally:
        stmt.close()

    return {
        "tables_verified": len(INSERT_ORDER),
        "fk_checks_passed": len(ORPHAN_FK_CHECKS),
        "trino_ro_verified": True,
        "pii_guard_passed": True,
    }


def query_postgres_key_sets(conn: Any, run_id: str) -> dict[str, set[tuple[Any, ...]]]:
    """Fetch exact primary-key sets from PostgreSQL warehouse.* for run_id."""
    key_sets: dict[str, set[tuple[Any, ...]]] = {}
    for table_name in INSERT_ORDER:
        pk_cols = PK_COLUMNS[table_name]
        select_list = ", ".join(pk_cols)
        ps = conn.prepareStatement(
            f"SELECT {select_list} FROM warehouse.{table_name} WHERE _dataset_run_id = ?"
        )
        rows: set[tuple[Any, ...]] = set()
        try:
            ps.setString(1, run_id)
            rs = ps.executeQuery()
            try:
                while rs.next():
                    vals = []
                    for idx, col_name in enumerate(pk_cols, start=1):
                        vals.append(
                            int(rs.getLong(idx)) if col_name == "date_key" else rs.getString(idx)
                        )
                    rows.add(tuple(vals))
            finally:
                rs.close()
        finally:
            ps.close()
        key_sets[table_name] = rows
    return key_sets


def reconcile_run(
    spark: Any,
    run_id: str,
    export_meta: dict[str, Any],
    *,
    gold_root: str | Path = DEFAULT_GOLD_ROOT,
) -> dict[str, Any]:
    """Reconcile PostgreSQL warehouse.* against pinned Gold Iceberg window slice."""
    frames, _snapshots, _run_meta, gold_pub_id = read_gold_star_schema(
        spark,
        run_id,
        gold_root=gold_root,
        publication_id=export_meta["gold_publication_id"],
    )
    sliced, _w_start, _w_end = slice_warehouse_window(frames, export_meta["run_date"])

    # Expected PK sets and monetary sums computed straight from pinned Gold Iceberg slice
    gold_key_sets: dict[str, set[tuple[Any, ...]]] = {}
    gold_counts: dict[str, int] = {}
    for table_name in INSERT_ORDER:
        pk_cols = PK_COLUMNS[table_name]
        collected = sliced[table_name].select(*pk_cols).collect()
        gold_key_sets[table_name] = {tuple(row[c] for c in pk_cols) for row in collected}
        gold_counts[table_name] = len(collected)

    payload, gold_monetary = collect_slice_payload(sliced)

    conn = writer_connection(spark, read_only=True)
    try:
        from spark.lib.warehouse_content import payload_content, query_content

        if query_content(conn, run_id) != payload_content(payload):
            raise ValueError("Full-row Gold/PostgreSQL content mismatch")
        pg_key_sets = query_postgres_key_sets(conn, run_id)
        for table_name in INSERT_ORDER:
            compare_key_sets(table_name, gold_key_sets[table_name], pg_key_sets[table_name])
        pg_monetary = query_run_monetary_totals(conn, run_id)
        compare_monetary_maps(gold_monetary, pg_monetary)
        constraints = check_postgres_constraints(conn)
    finally:
        rollback_close(conn)

    return {
        "run_id": run_id,
        "gold_publication_id": gold_pub_id,
        "run_date": export_meta["run_date"],
        "row_counts": gold_counts,
        "monetary": gold_monetary,
        "constraints": constraints,
    }
