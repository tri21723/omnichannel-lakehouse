"""Real PostgreSQL concurrent mutation and connection-failure integration tests.

Only snapshot-probe-v1 fixture IDs are modified; no generator dataset is changed.
Writer credentials are injected solely by compose.bronze-test.yml.
"""

import argparse
import json
import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from spark.lib import bronze_jdbc as jdbc
from spark.lib.bronze import event
from spark.lib.session import get_spark

PREFIX = "snapshot-probe-v1-walmart-"


def writer_connection(spark):
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", "generator")
    props.setProperty("password", os.environ["GENERATOR_DB_PASSWORD"])
    props.setProperty("connectTimeout", "10")
    props.setProperty("socketTimeout", "30")
    conn = jvm.java.sql.DriverManager.getConnection(os.environ["BRONZE_JDBC_URL"], props)
    conn.setAutoCommit(False)
    return conn


def execute(conn, sql):
    statement = conn.createStatement()
    try:
        statement.execute(sql)
    finally:
        statement.close()


def clean(conn):
    for table, key in (
        ("returns", "return_id"),
        ("order_items", "order_id"),
        ("orders", "order_id"),
        ("customers", "customer_id"),
        ("products", "product_id"),
    ):
        execute(conn, f"DELETE FROM walmart.{table} WHERE starts_with({key}, '{PREFIX}')")
    conn.commit()


def setup(conn):
    clean(conn)
    execute(
        conn,
        f"""INSERT INTO walmart.customers VALUES
        ('{PREFIX}c', 'before', 'fixture@example.invalid', 'US', '2026-01-01T00:00:00Z')""",
    )
    execute(
        conn,
        f"""INSERT INTO walmart.products VALUES
        ('{PREFIX}p', 'fixture-seller', 'before', 'fixture', NULL, '2026-01-01T00:00:00Z')""",
    )
    execute(
        conn,
        f"""INSERT INTO walmart.orders VALUES
        ('{PREFIX}o', '{PREFIX}c', '2026-01-01T12:00:00Z', '2026-01-01T00:00:00Z',
         'before', 'USD', 10.25, 'US')""",
    )
    execute(
        conn,
        f"""INSERT INTO walmart.order_items VALUES
        ('{PREFIX}o', 1, '{PREFIX}p', 'fixture-seller', 1, 10.25, 0.00)""",
    )
    execute(
        conn,
        f"""INSERT INTO walmart.returns VALUES
        ('{PREFIX}r', 1, '{PREFIX}o', 1, '2026-01-15T00:00:00Z', 'fixture', 10.25)""",
    )
    conn.commit()


def run():
    spark = get_spark()
    writer = writer_connection(spark)
    original = jdbc.connection
    args = argparse.Namespace(run_id="snapshot-probe-v1", run_date="2026-01-01")
    try:
        setup(writer)
        connections = []

        def connect(_):
            conn = original(spark)
            connections.append(conn)
            if len(connections) == 2:  # Snapshot exported, before first reader.
                execute(
                    writer, f"UPDATE walmart.orders SET status='after' WHERE order_id='{PREFIX}o'"
                )
                execute(
                    writer,
                    f"""INSERT INTO walmart.orders
                    SELECT '{PREFIX}o2', customer_id, order_ts, updated_ts, status,
                           currency, total_amount, shipping_country
                    FROM walmart.orders WHERE order_id='{PREFIX}o'""",
                )
                execute(
                    writer,
                    f"""INSERT INTO walmart.order_items
                    SELECT '{PREFIX}o2', line_no, product_id, seller_id, qty, unit_price, discount
                    FROM walmart.order_items WHERE order_id='{PREFIX}o'""",
                )
                writer.commit()
            if len(connections) == 3:  # After orders, before related readers.
                execute(writer, f"UPDATE walmart.order_items SET qty=2 WHERE order_id='{PREFIX}o'")
                execute(
                    writer,
                    f"UPDATE walmart.customers SET full_name='after' WHERE customer_id='{PREFIX}c'",
                )
                execute(
                    writer,
                    f"UPDATE walmart.products SET title='after' WHERE product_id='{PREFIX}p'",
                )
                execute(
                    writer,
                    f"UPDATE walmart.returns SET refund_amount=1.50 WHERE return_id='{PREFIX}r'",
                )
                writer.commit()
            return conn

        with tempfile.TemporaryDirectory(dir=os.environ["BRONZE_STAGE_DIR"]) as directory:
            with patch.object(jdbc, "connection", connect):
                files, evidence = jdbc.stage_jdbc(spark, args, Path(directory), 1024 * 1024)
            rows = {
                name: [
                    json.loads(json.loads(line)["_raw"]) for line in path.read_text().splitlines()
                ]
                for name, path in files.items()
            }
            assert all(len(value) == 1 for value in rows.values())
            assert rows["orders"][0]["status"] == "before"
            assert rows["order_items"][0]["qty"] == 1
            assert rows["customers"][0]["full_name"] == "before"
            assert rows["products"][0]["title"] == "before"
            assert rows["products"][0]["brand"] is None
            assert rows["returns"][0]["refund_amount"] == 10.25
            assert all(conn.isClosed() for conn in connections)
            assert evidence["read_only"]
            # A fresh snapshot must see the committed changes and the second order.
            files, _ = jdbc.stage_jdbc(spark, args, Path(directory), 1024 * 1024)
            assert len(files["orders"].read_text().splitlines()) == 2
        event("bronze_snapshot_mutation_passed", readers=5)

        for failure in ("reader", "coordinator"):
            opened = []

            def broken(_, opened=opened, failure=failure):
                conn = original(spark)
                opened.append(conn)
                if len(opened) == 2:
                    opened[1 if failure == "reader" else 0].close()
                return conn

            with tempfile.TemporaryDirectory(dir=os.environ["BRONZE_STAGE_DIR"]) as directory:
                with patch.object(jdbc, "connection", broken):
                    try:
                        jdbc.stage_jdbc(spark, args, Path(directory), 1024 * 1024)
                    except Exception:
                        pass
                    else:
                        raise AssertionError("Closed connection did not fail ingestion")
                assert all(conn.isClosed() for conn in opened)
                # Recovery must not reuse an expired exported token.
                jdbc.stage_jdbc(spark, args, Path(directory), 1024 * 1024)
            event("bronze_snapshot_failure_recovered", failure=failure)
    finally:
        writer.rollback()
        try:
            clean(writer)
        finally:
            writer.close()
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
