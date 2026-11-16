"""Single-connection transactional JVM PostgreSQL JDBC writer for Phase 4E."""

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from spark.lib.bronze_jdbc import rollback_close

DEFAULT_DDL_PATH = Path(__file__).resolve().parents[2] / "sql" / "postgres" / "003_warehouse.sql"
DEFAULT_JDBC_URL = "jdbc:postgresql://postgres:5432/sources"
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)

DIMENSION_TABLES = ("dim_date", "dim_channel", "dim_customer", "dim_seller", "dim_product")
FACT_TABLES = ("fact_orders", "fact_order_items", "fact_returns")
INSERT_ORDER = DIMENSION_TABLES + FACT_TABLES
DELETE_ORDER = tuple(reversed(INSERT_ORDER))

TABLE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "dim_date": (
        ("_dataset_run_id", "str"),
        ("date_key", "int"),
        ("calendar_date", "date"),
        ("year", "int"),
        ("quarter", "int"),
        ("month", "int"),
        ("day_of_month", "int"),
        ("day_of_week", "int"),
        ("is_weekend", "bool"),
    ),
    "dim_channel": (
        ("_dataset_run_id", "str"),
        ("channel_key", "str"),
        ("channel", "str"),
        ("channel_name", "str"),
        ("ingestion_mode", "str"),
    ),
    "dim_customer": (
        ("customer_key", "str"),
        ("channel", "str"),
        ("customer_id", "str"),
        ("name_hash", "str"),
        ("email_hash", "str"),
        ("country", "str"),
        ("first_order_ts", "timestamptz"),
        ("updated_ts", "timestamptz"),
        ("_pii_key_version", "str"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_silver_snapshot", "bigint"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
    "dim_seller": (
        ("seller_key", "str"),
        ("channel", "str"),
        ("seller_id", "str"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
    "dim_product": (
        ("product_key", "str"),
        ("seller_key", "str"),
        ("channel", "str"),
        ("product_id", "str"),
        ("seller_id", "str"),
        ("title", "str"),
        ("category", "str"),
        ("brand", "str"),
        ("updated_ts", "timestamptz"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_silver_snapshot", "bigint"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
    "fact_orders": (
        ("order_key", "str"),
        ("order_date_key", "int"),
        ("channel_key", "str"),
        ("customer_key", "str"),
        ("channel", "str"),
        ("order_id", "str"),
        ("customer_id", "str"),
        ("order_ts", "timestamptz"),
        ("updated_ts", "timestamptz"),
        ("status", "str"),
        ("currency", "str"),
        ("gross_amount", "decimal"),
        ("discount_amount", "decimal"),
        ("net_amount", "decimal"),
        ("item_count", "int"),
        ("total_qty", "int"),
        ("shipping_country", "str"),
        ("is_deleted", "bool"),
        ("fulfillment_type", "str"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_silver_snapshot", "bigint"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
    "fact_order_items": (
        ("order_item_key", "str"),
        ("order_key", "str"),
        ("order_date_key", "int"),
        ("channel_key", "str"),
        ("customer_key", "str"),
        ("product_key", "str"),
        ("seller_key", "str"),
        ("channel", "str"),
        ("order_id", "str"),
        ("line_no", "int"),
        ("product_id", "str"),
        ("seller_id", "str"),
        ("order_ts", "timestamptz"),
        ("updated_ts", "timestamptz"),
        ("status", "str"),
        ("currency", "str"),
        ("qty", "int"),
        ("unit_price", "decimal"),
        ("discount", "decimal"),
        ("gross_line_amount", "decimal"),
        ("net_line_amount", "decimal"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_silver_snapshot", "bigint"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
    "fact_returns": (
        ("return_key", "str"),
        ("order_key", "str"),
        ("order_item_key", "str"),
        ("order_date_key", "int"),
        ("return_date_key", "int"),
        ("channel_key", "str"),
        ("customer_key", "str"),
        ("product_key", "str"),
        ("seller_key", "str"),
        ("channel", "str"),
        ("return_id", "str"),
        ("return_line_no", "int"),
        ("order_id", "str"),
        ("line_no", "int"),
        ("reason", "str"),
        ("return_ts", "timestamptz"),
        ("updated_ts", "timestamptz"),
        ("currency", "str"),
        ("refund_amount", "decimal"),
        ("_dataset_run_id", "str"),
        ("_silver_publication_id", "str"),
        ("_silver_snapshot", "bigint"),
        ("_record_hash", "str"),
        ("_policy_version", "str"),
    ),
}

MONETARY_GROUP_EXPRESSIONS: dict[str, tuple[str, ...]] = {
    "fact_orders": ("gross_amount", "discount_amount", "net_amount"),
    "fact_order_items": ("gross_line_amount", "net_line_amount"),
    "fact_returns": ("refund_amount",),
}


class WarehouseFaultInjectionError(RuntimeError):
    """Raised intentionally by fault injection before JDBC transaction commit."""


def writer_connection(spark: Any, *, read_only: bool = False) -> Any:
    """Open a non-autocommit UTC PostgreSQL JDBC connection as role spark_writer."""
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    props = jvm.java.util.Properties()
    props.setProperty("user", "spark_writer")
    props.setProperty("password", os.environ["SPARK_WRITER_DB_PASSWORD"])
    props.setProperty("connectTimeout", "10")
    props.setProperty("socketTimeout", "120")
    jdbc_url = os.environ.get("WAREHOUSE_JDBC_URL", DEFAULT_JDBC_URL)
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, props)
    try:
        conn.setAutoCommit(False)
        if read_only:
            conn.setReadOnly(True)
        stmt = conn.createStatement()
        try:
            stmt.execute("SET TIME ZONE 'UTC'")
        finally:
            stmt.close()
        return conn
    except Exception:
        rollback_close(conn)
        raise


def parse_ddl_statements(ddl_path: str | Path = DEFAULT_DDL_PATH) -> list[str]:
    """Read and split SQL DDL statements while stripping full-line SQL comments."""
    raw_lines = [
        line
        for line in Path(ddl_path).read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    ]
    cleaned = "\n".join(raw_lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def execute_ddl(conn: Any, ddl_path: str | Path = DEFAULT_DDL_PATH) -> None:
    """Execute idempotent PostgreSQL warehouse DDL inside the active transaction."""
    statements = parse_ddl_statements(ddl_path)
    stmt = conn.createStatement()
    try:
        for sql in statements:
            stmt.execute(sql)
    finally:
        stmt.close()


def build_insert_sql(table_name: str) -> str:
    """Build parameterized INSERT statement preserving exact schema column ordering."""
    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unsupported warehouse table: {table_name}")
    cols = [col for col, _dtype in TABLE_COLUMNS[table_name]]
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    return f"INSERT INTO warehouse.{table_name} ({col_list}) VALUES ({placeholders})"


def to_utc_micros(value: int | datetime | str) -> int:
    """Convert epoch micros, datetime, or ISO timestamp string into UTC epoch microseconds."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TypeError(f"Unsupported timestamp representation: {type(value)!r}")
    utc_dt = parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    delta = utc_dt - EPOCH_UTC
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def bind_jdbc_value(
    ps: Any,
    jvm: Any,
    index: int,
    value: Any,
    dtype: str,
) -> None:
    """Bind one typed column value to a JDBC PreparedStatement parameter."""
    sql_types = jvm.java.sql.Types
    if value is None:
        null_map = {
            "str": sql_types.VARCHAR,
            "int": sql_types.INTEGER,
            "bigint": sql_types.BIGINT,
            "bool": sql_types.BOOLEAN,
            "decimal": sql_types.NUMERIC,
            "date": sql_types.DATE,
            "timestamptz": sql_types.TIMESTAMP_WITH_TIMEZONE,
        }
        ps.setNull(index, null_map[dtype])
        return

    if dtype == "str":
        ps.setString(index, str(value))
    elif dtype == "int":
        ps.setInt(index, int(value))
    elif dtype == "bigint":
        ps.setLong(index, int(value))
    elif dtype == "bool":
        ps.setBoolean(index, bool(value))
    elif dtype == "decimal":
        quantized = str(Decimal(str(value)).quantize(Decimal("0.01")))
        ps.setBigDecimal(index, jvm.java.math.BigDecimal(quantized))
    elif dtype == "date":
        iso_date = value.isoformat() if isinstance(value, date) else str(value)
        ps.setDate(index, jvm.java.sql.Date.valueOf(iso_date))
    elif dtype == "timestamptz":
        micros = to_utc_micros(value)
        sec, usec = divmod(micros, 1_000_000)
        instant = jvm.java.time.Instant.ofEpochSecond(sec, usec * 1000)
        odt = jvm.java.time.OffsetDateTime.ofInstant(instant, jvm.java.time.ZoneOffset.UTC)
        ps.setObject(index, odt)
    else:
        raise ValueError(f"Unsupported JDBC column type {dtype!r}")


def collect_slice_payload(
    sliced_frames: dict[str, Any],
) -> tuple[dict[str, list[tuple[Any, ...]]], dict[str, dict[str, dict[str, str]]]]:
    """Collect driver-side typed row tuples and monetary summaries for the 8 sliced tables."""
    from pyspark.sql import functions as F

    payload: dict[str, list[tuple[Any, ...]]] = {}
    monetary: dict[str, dict[str, dict[str, str]]] = {}

    for table_name in INSERT_ORDER:
        df = sliced_frames[table_name]
        spec = TABLE_COLUMNS[table_name]
        select_exprs = []
        for col_name, dtype in spec:
            if dtype == "timestamptz":
                select_exprs.append(F.unix_micros(F.col(col_name)).alias(col_name))
            elif dtype == "decimal":
                select_exprs.append(F.col(col_name).cast("string").alias(col_name))
            elif dtype == "date":
                select_exprs.append(F.col(col_name).cast("string").alias(col_name))
            else:
                select_exprs.append(F.col(col_name))
        collected = df.select(*select_exprs).collect()
        rows = [tuple(row[col_name] for col_name, _ in spec) for row in collected]
        payload[table_name] = rows

        if table_name in MONETARY_GROUP_EXPRESSIONS:
            money_cols = MONETARY_GROUP_EXPRESSIONS[table_name]
            agg_exprs = [
                F.coalesce(F.sum(F.col(c)), F.lit(0).cast("decimal(18,2)")).cast("string").alias(c)
                for c in money_cols
            ]
            grouped = df.groupBy("channel", "currency").agg(*agg_exprs).collect()
            table_totals: dict[str, dict[str, str]] = {}
            for g in grouped:
                group_key = f"{g['channel']}|{g['currency']}"
                table_totals[group_key] = {
                    c: str(Decimal(str(g[c])).quantize(Decimal("0.01"))) for c in money_cols
                }
            monetary[table_name] = dict(sorted(table_totals.items()))

    return payload, monetary


def delete_run_slice(conn: Any, run_id: str) -> None:
    """Delete all existing warehouse rows for run_id in reverse FK order."""
    for table_name in DELETE_ORDER:
        sql = f"DELETE FROM warehouse.{table_name} WHERE _dataset_run_id = ?"
        ps = conn.prepareStatement(sql)
        try:
            ps.setString(1, run_id)
            ps.executeUpdate()
        finally:
            ps.close()


def insert_table_rows(
    conn: Any,
    jvm: Any,
    table_name: str,
    rows: list[tuple[Any, ...]],
    *,
    batch_size: int = 1000,
) -> int:
    """Batch-insert typed tuples for a single table using a PreparedStatement."""
    if not rows:
        return 0
    spec = TABLE_COLUMNS[table_name]
    sql = build_insert_sql(table_name)
    ps = conn.prepareStatement(sql)
    inserted = 0
    try:
        for row_idx, row in enumerate(rows, start=1):
            for col_idx, ((_col, dtype), value) in enumerate(zip(spec, row, strict=True), start=1):
                bind_jdbc_value(ps, jvm, col_idx, value, dtype)
            ps.addBatch()
            inserted += 1
            if row_idx % batch_size == 0:
                ps.executeBatch()
        if inserted % batch_size != 0:
            ps.executeBatch()
    finally:
        ps.close()
    return inserted


def count_run_rows(conn: Any, run_id: str) -> dict[str, int]:
    """Query exact per-table row counts for run_id inside the open transaction."""
    counts: dict[str, int] = {}
    for table_name in INSERT_ORDER:
        ps = conn.prepareStatement(
            f"SELECT COUNT(*) FROM warehouse.{table_name} WHERE _dataset_run_id = ?"
        )
        try:
            ps.setString(1, run_id)
            rs = ps.executeQuery()
            try:
                rs.next()
                counts[table_name] = int(rs.getLong(1))
            finally:
                rs.close()
        finally:
            ps.close()
    return counts


def query_run_monetary_totals(conn: Any, run_id: str) -> dict[str, dict[str, dict[str, str]]]:
    """Query per-(channel, currency) NUMERIC(18,2) sums from PostgreSQL for run_id."""
    totals: dict[str, dict[str, dict[str, str]]] = {}
    for table_name, money_cols in MONETARY_GROUP_EXPRESSIONS.items():
        sum_sql = ", ".join(f"COALESCE(SUM({c}), 0)::text" for c in money_cols)
        sql = (
            f"SELECT channel, currency, {sum_sql} "
            f"FROM warehouse.{table_name} "
            "WHERE _dataset_run_id = ? "
            "GROUP BY channel, currency "
            "ORDER BY channel, currency"
        )
        ps = conn.prepareStatement(sql)
        table_totals: dict[str, dict[str, str]] = {}
        try:
            ps.setString(1, run_id)
            rs = ps.executeQuery()
            try:
                while rs.next():
                    group_key = f"{rs.getString(1)}|{rs.getString(2)}"
                    table_totals[group_key] = {
                        c: str(Decimal(rs.getString(idx)).quantize(Decimal("0.01")))
                        for idx, c in enumerate(money_cols, start=3)
                    }
            finally:
                rs.close()
        finally:
            ps.close()
        totals[table_name] = table_totals
    return totals


def replace_run_slice(
    spark: Any,
    payload: dict[str, list[tuple[Any, ...]]],
    run_id: str,
    *,
    fail_after: str | None = None,
    batch_size: int = 1000,
    ddl_path: str | Path = DEFAULT_DDL_PATH,
) -> dict[str, int]:
    """Atomically replace all 8 warehouse tables for run_id in one PostgreSQL transaction."""
    if fail_after not in (None, "dimensions", "facts", "completion"):
        raise ValueError(f"Unsupported fail_after stage: {fail_after!r}")

    conn = writer_connection(spark, read_only=False)
    jvm = spark.sparkContext._jvm
    try:
        execute_ddl(conn, ddl_path=ddl_path)
        delete_run_slice(conn, run_id)

        for table_name in DIMENSION_TABLES:
            insert_table_rows(conn, jvm, table_name, payload[table_name], batch_size=batch_size)

        if fail_after == "dimensions":
            raise WarehouseFaultInjectionError("Injected transactional failure after dimensions")

        for table_name in FACT_TABLES:
            insert_table_rows(conn, jvm, table_name, payload[table_name], batch_size=batch_size)

        if fail_after == "facts":
            raise WarehouseFaultInjectionError("Injected transactional failure after facts")

        actual_counts = count_run_rows(conn, run_id)
        expected_counts = {name: len(payload[name]) for name in INSERT_ORDER}
        if actual_counts != expected_counts:
            raise ValueError(
                f"PostgreSQL in-transaction row verification failed: "
                f"expected {expected_counts}, got {actual_counts}"
            )

        if fail_after == "completion":
            raise WarehouseFaultInjectionError("Injected transactional failure right before commit")

        conn.commit()
        return actual_counts
    except Exception:
        rollback_close(conn)
        raise
    else:
        conn.close()
