"""Real mutable JDBC fixtures for correction, quarantine, shared state and failures.

Only reserved '-silver-fixture-' source IDs are modified. Immutable raw exports stay.
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from spark.jobs.dp1_bronze.main import main as bronze_job
from spark.jobs.dp1_bronze.snapshot_test import execute, writer_connection
from spark.jobs.dp2_silver.integration import release
from spark.jobs.dp2_silver.main import ROOT, key_settings
from spark.lib.bronze import event, writer_lock
from spark.lib.bronze_input import canonical, identity
from spark.lib.session import get_spark
from spark.lib.silver_input import load_pin, pin_completion, read_pinned
from spark.lib.silver_pipeline import candidates
from spark.lib.silver_publish import frame_at, publish, verify


def raw_fixture():
    config = dict(
        seed=998733,
        start_date="2026-01-01",
        days=2,
        orders_per_day_per_channel=1,
        channels=["walmart"],
        dup_rate=0.0,
        skew_top_seller_share=0.5,
        schema_drift_at_day=2,
        return_rate=1.0,
        rows_per_file=10,
        late_rate=0.05,
        late_max_minutes=60,
        burst_multiplier=50,
    )
    run = hashlib.sha256(canonical(dict(contract="batch-v1", config=config)).encode()).hexdigest()[
        :20
    ]
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["GENERATOR_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["GENERATOR_S3_SECRET_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(s3={"addressing_style": "path"}),
    )
    prefix = run + "-walmart-silver-fixture-"
    files = []

    def put(key, body):
        bucket = os.environ["S3_RAW_BUCKET"]
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "PreconditionFailed":
                raise
            with client.get_object(Bucket=bucket, Key=key)["Body"] as response:
                assert response.read() == body

    for day in (1, 2):
        body = (canonical(dict(order_id=prefix + f"o{day}", fixture=True)) + "\n").encode()
        key = f"walmart/dt=2026-01-0{day}/run_id={run}/part-00000.jsonl"
        files.append(
            dict(path=key, bytes=len(body), rows=1, sha256=hashlib.sha256(body).hexdigest())
        )
        put(key, body)
    manifest = dict(
        contract="batch-v1",
        config=config,
        run_id=run,
        channels={"walmart": dict(rows=2, unique_orders=2, dup_rows=0, files=files)},
    )
    put(f"manifests/{run}/stream_manifest.json", canonical(manifest).encode())
    return run, prefix


def clean(conn, prefix):
    for table, key in (
        ("returns", "return_id"),
        ("order_items", "order_id"),
        ("orders", "order_id"),
        ("customers", "customer_id"),
        ("products", "product_id"),
    ):
        execute(conn, f"DELETE FROM walmart.{table} WHERE starts_with({key}, '{prefix}')")
    conn.commit()


def setup(conn, p):
    clean(conn, p)
    execute(
        conn,
        f"""INSERT INTO walmart.customers VALUES
        ('{p}c', 'Fixture User', 'fixture@example.invalid', 'VN', '2026-01-02T00:00:00Z')""",
    )
    execute(
        conn,
        f"""INSERT INTO walmart.products VALUES
        ('{p}p', 'fixture-seller', 'before', 'fixture', NULL, '2026-01-02T00:00:00Z')""",
    )
    for day in (1, 2):
        execute(
            conn,
            f"""INSERT INTO walmart.orders VALUES
            ('{p}o{day}', '{p}c', '2026-01-0{day}T12:00:00Z', '2026-01-0{day}T12:01:00Z',
             'paid', 'USD', 10.25, 'VN')""",
        )
        execute(
            conn,
            f"""INSERT INTO walmart.order_items VALUES
            ('{p}o{day}', 1, '{p}p', 'fixture-seller', 1, 10.25, 0)""",
        )
        execute(
            conn,
            f"""INSERT INTO walmart.returns VALUES
            ('{p}r{day}', 1, '{p}o{day}', 1, '2026-01-15T00:00:00Z', 'fixture', 10.25)""",
        )
    conn.commit()


def run():
    run_id, prefix = raw_fixture()
    key, version = key_settings()
    key_id = hashlib.sha256(key).hexdigest()
    spark = get_spark()
    writer = writer_connection(spark)
    try:
        setup(writer, prefix)

        def prepare(day):
            selection = dict(
                run_id=run_id, run_date=f"2026-01-0{day}", channel="walmart", source="jdbc"
            )
            bronze_job(SimpleNamespace(**selection, mode="full", verify=False), spark)
            batch = identity(**selection)
            pin = pin_completion(
                Path("/runtime/bronze") / f"{batch}.json", selection, Path("/runtime/silver/inputs")
            )
            return pin

        def result(pin):
            state = load_pin(pin)
            frames = read_pinned(spark, pin)
            return candidates(spark, frames, state, pin.stem, key, version), state

        def apply(pin, fail=None, correction=False):
            rows, state = result(pin)
            return publish(
                spark,
                rows,
                pin.stem,
                state,
                version,
                ROOT,
                correction=correction,
                fail_after=fail,
                key_id=key_id,
            )[0]

        def table(ledger, entity):
            name = "lh.silver." + entity
            frame = frame_at(spark, name, ledger["tables"][name], spark.table(name).schema)
            from pyspark.sql import functions as F

            return frame.filter(F.col("_dataset_run_id") == run_id)

        old_pin = prepare(1)
        unseen_old_pin = prepare(1)
        execute(
            writer,
            "UPDATE walmart.products SET title='after', updated_ts='2026-01-03T00:00:00Z' "
            f"WHERE product_id='{prefix}p'",
        )
        writer.commit()
        newer = prepare(2)
        ledger = apply(newer)
        release(spark)
        ledger = apply(old_pin)  # Older date must not overwrite the newer shared product.
        assert table(ledger, "products").first()["title"] == "after"
        assert table(ledger, "products").count() == table(ledger, "customers").count() == 1
        assert table(ledger, "orders").count() == table(ledger, "returns").count() == 2
        release(spark)
        execute(writer, f"UPDATE walmart.orders SET total_amount=10.26 WHERE order_id='{prefix}o1'")
        writer.commit()
        bad = prepare(1)
        ledger = apply(bad)
        assert table(ledger, "orders").count() == 1
        assert table(ledger, "products").count() == table(ledger, "customers").count() == 1
        q = "lh.silver.quarantine_orders"
        assert (
            frame_at(spark, q, ledger["tables"][q], spark.table(q).schema)
            .filter("_dataset_run_id='" + run_id + "'")
            .count()
            == 1
        )
        release(spark)
        try:
            apply(old_pin)
        except ValueError:
            pass
        else:
            raise AssertionError("Superseded source was reactivated")
        release(spark)
        try:
            apply(unseen_old_pin)
        except ValueError:
            pass
        else:
            raise AssertionError("Unseen older snapshot was reactivated")
        release(spark)
        execute(
            writer,
            "UPDATE walmart.orders SET total_amount=10.25, updated_ts='2026-01-04T00:00:00Z' "
            f"WHERE order_id='{prefix}o1'",
        )
        writer.commit()
        good = prepare(1)
        rows, state = result(good)
        before = (ROOT / "ledger.json").read_bytes()
        for failure in ("contribution", "quarantine", "completion"):
            try:
                publish(
                    spark, rows, good.stem, state, version, ROOT, fail_after=failure, key_id=key_id
                )
            except RuntimeError:
                pass
            else:
                raise AssertionError("Failure injection did not run")
            assert (ROOT / "ledger.json").read_bytes() == before
            assert json.loads((ROOT / "attempt.json").read_text())["status"] == "failed"
            verify(spark, json.loads(before))
            event("silver_publish_failure_preserved", stage=failure)
        with writer_lock(ROOT):
            try:
                publish(spark, rows, good.stem, state, version, ROOT, key_id=key_id)
            except RuntimeError:
                pass
            else:
                raise AssertionError("Concurrent writer was accepted")
        try:
            publish(spark, rows, good.stem, state, version, ROOT, key_id="0" * 64)
        except ValueError:
            pass
        else:
            raise AssertionError("Mixed PII keys were accepted")
        # Invalid scope must fail without committing a new publication.
        from pyspark.sql import functions as F

        invalid_rows = {
            name: {
                **value,
                "valid": value["valid"].withColumn("_run_date", F.lit(None).cast("date")),
            }
            for name, value in rows.items()
        }
        try:
            publish(spark, invalid_rows, good.stem, state, version, ROOT, key_id=key_id)
        except ValueError:
            pass
        else:
            raise AssertionError("NULL slice identity accepted")
        assert (ROOT / "ledger.json").read_bytes() == before
        ledger = publish(spark, rows, good.stem, state, version, ROOT, key_id=key_id)[0]
        assert table(ledger, "orders").count() == 2
        assert (
            frame_at(spark, q, ledger["tables"][q], spark.table(q).schema)
            .filter("_dataset_run_id='" + run_id + "'")
            .count()
            == 0
        )
        release(spark)
        # Explicitly empty full snapshots retract this fixture's contributions only.
        clean(writer, prefix)
        for day in (1, 2):
            empty = prepare(day)
            ledger = apply(empty)
            release(spark)
        assert table(ledger, "orders").count() == table(ledger, "products").count() == 0
        verify(spark, ledger)
        event("silver_correction_recovery_passed", shared_entities=True, failures=3)
    finally:
        writer.rollback()
        clean(writer, prefix)
        writer.close()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
