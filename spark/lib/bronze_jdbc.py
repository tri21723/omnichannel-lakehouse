"""JVM PostgreSQL JDBC snapshot export, bounded to shared JSONL staging.

All database reads happen eagerly while the exported snapshot transaction is alive;
Spark later reads immutable staged rows. No Spark task reconnects to a newer snapshot.
"""

import hashlib
import os
import re
from pathlib import Path
from typing import Any

from spark.lib.bronze_input import canonical

TABLES = ("orders", "order_items", "customers", "products", "returns")


def connection(spark: Any):
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    props = jvm.java.util.Properties()
    props.setProperty("user", "spark_reader")
    props.setProperty("password", os.environ["SPARK_READER_DB_PASSWORD"])
    props.setProperty("connectTimeout", "10")
    props.setProperty("socketTimeout", "60")
    conn = jvm.java.sql.DriverManager.getConnection(os.environ["BRONZE_JDBC_URL"], props)
    try:
        conn.setAutoCommit(False)
        conn.setTransactionIsolation(4)  # TRANSACTION_REPEATABLE_READ
        conn.setReadOnly(True)
        return conn
    except Exception:
        conn.close()
        raise


def rollback_close(conn: Any) -> None:
    """Release a connection even when rollback fails after a network failure."""
    try:
        if not conn.isClosed():
            conn.rollback()
    finally:
        conn.close()


def scalar(conn: Any, sql: str) -> str:
    statement = conn.createStatement()
    try:
        result = statement.executeQuery(sql)
        try:
            result.next()
            return result.getString(1)
        finally:
            result.close()
    finally:
        statement.close()


def queries(run_id: str, run_date: str) -> dict[str, str]:
    # Caller validates date and run identity; bind values again in every JDBC statement.
    scope = (
        "SELECT * FROM walmart.orders WHERE starts_with(order_id, ?) "
        "AND order_ts >= ?::date AND order_ts < ?::date + interval '1 day'"
    )
    return {
        "orders": scope,
        "order_items": f"SELECT t.* FROM walmart.order_items t JOIN ({scope}) o USING (order_id)",
        "returns": f"SELECT t.* FROM walmart.returns t JOIN ({scope}) o USING (order_id)",
        "customers": (
            "SELECT t.* FROM walmart.customers t WHERE customer_id IN "
            f"(SELECT customer_id FROM ({scope}) o)"
        ),
        "products": (
            "SELECT t.* FROM walmart.products t WHERE product_id IN "
            f"(SELECT i.product_id FROM walmart.order_items i "
            f"JOIN ({scope}) o USING (order_id))"
        ),
    }


def stage_jdbc(spark: Any, args: Any, root: Path, limit: int) -> tuple[dict, dict]:
    coordinator = connection(spark)
    outputs, evidence, total = {}, {}, 0
    try:
        token = scalar(coordinator, "SELECT pg_export_snapshot()")
        if not re.fullmatch(r"[0-9A-Fa-f-]+", token):
            raise ValueError("Unexpected PostgreSQL snapshot token")
        for name, query in queries(args.run_id, args.run_date).items():
            reader = connection(spark)
            try:
                statement = reader.createStatement()
                try:
                    statement.execute(f"SET TRANSACTION SNAPSHOT '{token}'")
                    statement.execute("SET LOCAL TIME ZONE 'UTC'")
                finally:
                    statement.close()
                if scalar(reader, "SHOW transaction_isolation") != "repeatable read":
                    raise ValueError("JDBC isolation mismatch")
                if scalar(reader, "SHOW transaction_read_only") != "on":
                    raise ValueError("JDBC connection must be read only")
                statement = reader.prepareStatement(f"SELECT row_to_json(t)::text FROM ({query}) t")
                try:
                    statement.setString(1, args.run_id + "-walmart-")
                    statement.setString(2, args.run_date)
                    statement.setString(3, args.run_date)
                    statement.setFetchSize(1000)
                    result = statement.executeQuery()
                    digest, rows = hashlib.sha256(), 0
                    path = root / f"jdbc-{name}.jsonl"
                    try:
                        with path.open("w", encoding="utf-8") as target:
                            while result.next():
                                raw = result.getString(1)
                                wrapper = (
                                    canonical(
                                        {
                                            "_raw": raw,
                                            "_source_table": "walmart." + name,
                                            "_source_file": None,
                                            "_parse_status": "valid",
                                        }
                                    )
                                    + "\n"
                                )
                                total += len(wrapper.encode())
                                if total > limit:
                                    raise ValueError("JDBC staging byte limit exceeded")
                                digest.update(raw.encode())
                                target.write(wrapper)
                                rows += 1
                    finally:
                        result.close()
                    outputs[name] = path
                    evidence[name] = {"rows": rows, "sha256_stream": digest.hexdigest()}
                finally:
                    statement.close()
            finally:
                rollback_close(reader)
    finally:
        rollback_close(coordinator)
    # Do not return/log the snapshot token. This proves import/isolation for each reader.
    return outputs, {
        "isolation": "repeatable read",
        "read_only": True,
        "tables": evidence,
        "snapshot_fingerprint": hashlib.sha256(token.encode()).hexdigest(),
    }
