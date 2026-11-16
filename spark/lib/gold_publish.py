"""Serialized Gold publication ledger, scoped MERGE replacement and read-back verification."""

import hashlib
import json
import time
from pathlib import Path

from spark.lib.bronze import save_state, snapshot, writer_lock
from spark.lib.gold_entities import (
    GOLD_FIELDS,
    GOLD_KEYS,
    POLICY,
)
from spark.lib.silver_publish import frame_at, summary

ROOT = Path("/runtime/gold/publication")
GOLD_TABLES = list(GOLD_FIELDS.keys())


def replace_table(spark, table, df):
    """Replace Iceberg table contents atomically using delete marker MERGE."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField, StructType

    expected = summary(df)
    if not spark.catalog.tableExists(table):
        writer = (
            df.limit(0)
            .writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
        )
        writer.create()

    partition = None
    if table.endswith(".fact_orders") or table.endswith(".fact_order_items"):
        partition = "days(order_ts)"
    elif table.endswith(".fact_returns"):
        partition = "days(return_ts)"

    if partition:
        show_create = spark.sql(f"SHOW CREATE TABLE {table}").first()[0].upper()
        if "PARTITIONED BY" not in show_create:
            spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD {partition}")

    actual_schema = {f.name: f.dataType for f in spark.table(table).schema}
    if actual_schema != {f.name: f.dataType for f in df.schema}:
        raise ValueError(f"Gold schema mismatch on {table}; migration required")

    df = df.select(*spark.table(table).columns)
    current = spark.table(table)
    if summary(current) == expected and not df.exceptAll(current).limit(1).count():
        files = (
            spark.sql(
                f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes), 0) "
                f"AS bytes FROM {table}.data_files"
            )
            .first()
            .asDict()
        )
        return dict(snapshot=snapshot(spark, table), content=expected, files=files)

    nullable = StructType([StructField(f.name, f.dataType, True) for f in df.schema])
    marker = spark.createDataFrame([tuple(None for _ in df.columns)], nullable)
    source = df.withColumn("_replace", F.lit(False)).unionByName(
        marker.withColumn("_replace", F.lit(True))
    )
    source.createOrReplaceTempView("gold_replacement")
    names = ", ".join("`" + c + "`" for c in df.columns)
    values = ", ".join("s.`" + c + "`" for c in df.columns)
    spark.sql(
        f"MERGE INTO {table} t USING gold_replacement s ON s._replace "
        f"WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT s._replace "
        f"THEN INSERT ({names}) VALUES ({values})"
    )

    current = spark.table(table)
    if summary(current) != expected or df.exceptAll(current).limit(1).count():
        raise ValueError(f"Gold read-back failed for table {table}")

    files = (
        spark.sql(
            f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes), 0) "
            f"AS bytes FROM {table}.data_files"
        )
        .first()
        .asDict()
    )
    return dict(snapshot=snapshot(spark, table), content=expected, files=files)


def publish_gold(
    spark,
    all_tables,
    run_id,
    silver_pub_id,
    root=ROOT,
    fail_after=None,
    force=False,
):
    """Publish Gold tables atomically with per-run scoping and update ledger."""
    from pyspark.sql import functions as F

    root = Path(root)
    with writer_lock(root):
        ledger_path = root / "ledger.json"
        old = (
            json.loads(ledger_path.read_text())
            if ledger_path.exists()
            else dict(status="pending", runs={}, tables={})
        )

        # Check if identical publication for this run already completed
        prior_run = old.get("runs", {}).get(run_id)
        if (
            not force
            and fail_after is None
            and prior_run
            and prior_run.get("silver_publication_id") == silver_pub_id
            and old.get("status") == "completed"
        ):
            verify_gold(spark, old, target_run_id=run_id)
            return old, True

        pending = dict(
            status="running",
            run_id=run_id,
            silver_publication_id=silver_pub_id,
            tables={},
        )
        save_state(root / "attempt.json", pending)
        current = json.loads(json.dumps(old))
        started = time.monotonic()

        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")

            for name, new_df in all_tables.items():
                table_fqdn = f"lh.gold.{name}"

                # Validate row identity and scope
                if (
                    new_df.filter(
                        F.col("_dataset_run_id").isNull() | (F.col("_dataset_run_id") != run_id)
                    )
                    .limit(1)
                    .count()
                ):
                    raise ValueError(f"Out-of-scope or null dataset run ID in {name}")

                for key in GOLD_KEYS[name]:
                    if (
                        new_df.filter(
                            F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
                        )
                        .limit(1)
                        .count()
                    ):
                        raise ValueError(f"Null or empty key {key} in {name}")

                # Combine with other dataset runs from previous committed snapshot
                prior_evidence = old.get("tables", {}).get(table_fqdn)
                base = frame_at(spark, table_fqdn, prior_evidence, new_df.schema)

                other_runs = base.filter(F.col("_dataset_run_id") != run_id)
                combined = other_runs.unionByName(new_df).localCheckpoint(eager=True)

                dup_within_run = (
                    combined.groupBy("_dataset_run_id", *GOLD_KEYS[name])
                    .count()
                    .filter(F.col("count") > 1)
                )
                if dup_within_run.limit(1).count():
                    raise ValueError(f"Duplicate key within run in {name}")

                # Ensure surrogate keys don't collide between runs
                # (dim_date and dim_channel are calendar/channel lookups)
                if name not in ("dim_date", "dim_channel"):
                    collision = combined.groupBy(*GOLD_KEYS[name]).agg(
                        F.countDistinct("_dataset_run_id").alias("_runs")
                    )
                    if collision.filter("_runs > 1").limit(1).count():
                        raise ValueError(f"Cross-run key collision in {name}")

                current["tables"][table_fqdn] = replace_table(spark, table_fqdn, combined)
                pending["tables"][table_fqdn] = current["tables"][table_fqdn]
                save_state(root / "attempt.json", pending)

                if fail_after == "dimensions" and name == "dim_seller":
                    raise RuntimeError("Injected dimensions failure")
                if fail_after == "facts" and name == "fact_order_items":
                    raise RuntimeError("Injected facts failure")

                combined.unpersist()

            current.setdefault("runs", {})[run_id] = {
                "silver_publication_id": silver_pub_id,
                "policy_version": POLICY,
                "updated_at": round(time.time(), 3),
            }
            current["status"] = "completed"
            current["duration_seconds"] = round(time.monotonic() - started, 3)

            verify_gold(spark, current, target_run_id=run_id)

            if fail_after == "completion":
                raise RuntimeError("Injected completion failure")

            gold_pub_id = hashlib.sha256(json.dumps(current, sort_keys=True).encode()).hexdigest()
            save_state(root / f"publication-{gold_pub_id}.json", current)
            save_state(ledger_path, current)

            pending["status"] = "completed"
            save_state(root / "attempt.json", pending)
            return current, False

        except Exception:
            pending["status"] = "failed"
            save_state(root / "attempt.json", pending)
            raise


def verify_gold(spark, ledger, target_run_id=None):
    """Read-back verification of all 8 Gold tables, referential integrity and metrics."""
    if ledger.get("status") != "completed":
        raise ValueError("Gold publication is not marked completed")

    required_tables = {f"lh.gold.{name}" for name in GOLD_TABLES}
    if set(ledger.get("tables", {}).keys()) != required_tables:
        raise ValueError("Incomplete Gold table inventory in ledger")

    from pyspark.sql import functions as F

    frames = {}
    for name in GOLD_TABLES:
        table_fqdn = f"lh.gold.{name}"
        evidence = ledger["tables"][table_fqdn]
        schema = spark.table(table_fqdn).schema
        frame = frame_at(spark, table_fqdn, evidence, schema)

        if name.startswith("fact_"):
            from spark.lib.gold_pipeline import validate_fact_numbers

            validate_fact_numbers(frame, name)

        # Content summary match
        if summary(frame) != evidence["content"]:
            raise ValueError(f"Snapshot summary mismatch on {table_fqdn}")

        # Unique surrogate keys
        for key in GOLD_KEYS[name]:
            if (
                frame.filter(
                    F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
                )
                .limit(1)
                .count()
            ):
                raise ValueError(f"Null or empty surrogate key {key} in {name}")

        keys_to_check = (
            ["_dataset_run_id"] + GOLD_KEYS[name]
            if name in ("dim_date", "dim_channel")
            else GOLD_KEYS[name]
        )
        if frame.groupBy(*keys_to_check).count().filter("count > 1").limit(1).count():
            raise ValueError(f"Duplicate surrogate key in {name}")

        # PII check: no clear PII
        if any(col in frame.columns for col in ("_raw", "email", "full_name")):
            raise ValueError(f"Clear PII detected in {name}")

        if name == "dim_customer":
            bad_hash = frame.filter(
                F.col("name_hash").isNull()
                | F.col("email_hash").isNull()
                | ~F.col("name_hash").rlike("^[0-9a-f]{64}$")
                | ~F.col("email_hash").rlike("^[0-9a-f]{64}$")
            )
            if bad_hash.limit(1).count():
                raise ValueError("Customer PII hashes invalid in dim_customer")

        frames[name] = (
            frame.filter(F.col("_dataset_run_id") == target_run_id) if target_run_id else frame
        )

    # Referential integrity checks
    def check_fk(child_df, parent_df, child_key, parent_key, rel_name):
        parent_projected = parent_df.select(F.col(parent_key).alias(child_key)).distinct()
        orphans = child_df.join(parent_projected, child_key, "left_anti")
        if orphans.limit(1).count():
            raise ValueError(f"Gold relationship failed: {rel_name}")

    check_fk(
        frames["fact_orders"],
        frames["dim_date"],
        "order_date_key",
        "date_key",
        "fact_orders -> dim_date",
    )
    check_fk(
        frames["fact_orders"],
        frames["dim_channel"],
        "channel_key",
        "channel_key",
        "fact_orders -> dim_channel",
    )
    check_fk(
        frames["fact_orders"],
        frames["dim_customer"],
        "customer_key",
        "customer_key",
        "fact_orders -> dim_customer",
    )

    check_fk(
        frames["fact_order_items"],
        frames["fact_orders"],
        "order_key",
        "order_key",
        "fact_order_items -> fact_orders",
    )
    check_fk(
        frames["fact_order_items"],
        frames["dim_date"],
        "order_date_key",
        "date_key",
        "fact_order_items -> dim_date",
    )
    check_fk(
        frames["fact_order_items"],
        frames["dim_product"],
        "product_key",
        "product_key",
        "fact_order_items -> dim_product",
    )
    check_fk(
        frames["fact_order_items"],
        frames["dim_seller"],
        "seller_key",
        "seller_key",
        "fact_order_items -> dim_seller",
    )

    check_fk(
        frames["fact_returns"],
        frames["fact_order_items"],
        "order_item_key",
        "order_item_key",
        "fact_returns -> fact_order_items",
    )
    check_fk(
        frames["fact_returns"],
        frames["dim_date"],
        "return_date_key",
        "date_key",
        "fact_returns -> dim_date (return)",
    )

    # Monetary and line sum reconciliation
    line_totals = (
        frames["fact_order_items"]
        .groupBy("order_key")
        .agg(F.sum("net_line_amount").alias("_sum_net"))
    )
    order_mismatch = (
        frames["fact_orders"]
        .join(line_totals, "order_key", "left")
        .filter(F.col("_sum_net").isNull() | (F.col("net_amount") != F.col("_sum_net")))
    )
    if order_mismatch.limit(1).count():
        raise ValueError("Gold order and items monetary total mismatch")

    # Return refund capped at net line amount
    refund_totals = (
        frames["fact_returns"]
        .groupBy("order_item_key")
        .agg(F.sum("refund_amount").alias("_sum_refund"))
    )
    refund_exceeded = (
        frames["fact_order_items"]
        .join(refund_totals, "order_item_key", "inner")
        .filter(F.col("_sum_refund") > F.col("net_line_amount"))
    )
    if refund_exceeded.limit(1).count():
        raise ValueError("Gold return refund exceeds item line amount")
