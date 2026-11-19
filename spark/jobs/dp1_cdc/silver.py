"""dp1_cdc Silver stage: current state of walmart.orders with soft deletes (ADR-0017 §5).

Takes the latest valid Bronze CDC event per order by (lsn, offset), types it (DECIMAL(18,2), UTC
timestamps) and MERGEs it only when it is newer than the stored state, so replays and reruns never
overwrite a newer row. Deletes set `is_deleted`; a later insert revives the row. Untypable events go
to the quarantine table with `_error_reason`. `--mode verify` is read-only.
"""

import argparse
import hashlib
import json
import logging
import time
from datetime import date

from spark.lib.cdc import (
    BRONZE_TABLE,
    QUARANTINE_SCHEMA,
    QUARANTINE_TABLE,
    SILVER_SCHEMA,
    SILVER_TABLE,
    TYPED_SCHEMA,
    silver_merge_sql,
    typed_row,
)
from spark.lib.session import get_spark

log = logging.getLogger("dp1_cdc_silver")


def event(name: str, **fields) -> None:
    log.info(json.dumps({"event": name, **fields}, sort_keys=True, default=str))


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", default="walmart", choices=("walmart",))
    parser.add_argument("--mode", default="incremental", choices=("incremental", "verify"))
    args = parser.parse_args(argv)
    if date.fromisoformat(args.run_date).isoformat() != args.run_date:
        raise ValueError("Date must be ISO YYYY-MM-DD")
    return args


def ddl(schema: str) -> str:
    return (
        schema.replace(" string", " STRING")
        .replace(" bigint", " BIGINT")
        .replace(" int,", " INT,")
        .replace(" timestamp", " TIMESTAMP")
        .replace(" boolean", " BOOLEAN")
    )


def ensure_tables(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for table, schema in ((SILVER_TABLE, SILVER_SCHEMA), (QUARANTINE_TABLE, QUARANTINE_SCHEMA)):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({ddl(schema)}) USING iceberg TBLPROPERTIES "
            "('format-version'='2','write.parquet.compression-codec'='zstd')"
        )
        expected = spark.createDataFrame([], schema).schema
        actual = spark.table(table).schema
        if [(f.name, f.dataType) for f in actual] != [(f.name, f.dataType) for f in expected]:
            raise ValueError(f"Existing schema of {table} differs from contract")


def latest_events(spark):
    """Typed latest event per order among valid Bronze change events."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    typed = F.udf(typed_row, TYPED_SCHEMA)
    events = (
        spark.table(BRONZE_TABLE)
        .filter((F.col("_parse_status") == "valid") & F.col("op").isin("c", "u", "d", "r"))
        .withColumn("t", typed(F.col("op"), F.col("before"), F.col("after")))
    )
    order = Window.partitionBy("order_id").orderBy(
        F.col("lsn").desc(), F.col("_kafka_offset").desc()
    )
    latest = events.withColumn("rank", F.row_number().over(order)).filter("rank = 1")
    return events, latest


def silver_rows(latest):
    from pyspark.sql import functions as F

    return latest.filter(F.col("t._error_reason").isNull()).select(
        F.col("t.order_id").alias("order_id"),
        F.col("t.customer_id").alias("customer_id"),
        F.to_timestamp("t.order_ts").alias("order_ts"),
        F.to_timestamp("t.updated_ts").alias("updated_ts"),
        F.col("t.status").alias("status"),
        F.col("t.currency").alias("currency"),
        F.col("t.total_amount").cast("decimal(18,2)").alias("total_amount"),
        F.col("t.shipping_country").alias("shipping_country"),
        (F.col("op") == "d").alias("is_deleted"),
        F.col("op").alias("_cdc_op"),
        F.col("lsn").alias("_cdc_lsn"),
        F.col("ts_ms").alias("_cdc_ts_ms"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
        F.current_timestamp().alias("_updated_at"),
    )


def quarantine_rows(spark, events):
    """Bronze records that are invalid, or valid change events that cannot be typed."""
    from pyspark.sql import functions as F

    bad_bronze = spark.table(BRONZE_TABLE).filter(F.col("_parse_status") == "invalid")
    bad_typed = events.filter(F.col("t._error_reason").isNotNull()).withColumn(
        "_error_reason", F.col("t._error_reason")
    )
    columns = [
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "order_id",
        "op",
        "_raw",
        "_error_reason",
    ]
    return (
        bad_bronze.select(*columns)
        .unionByName(bad_typed.select(*columns))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


def content_hash(df) -> str:
    rows = (
        df.select(
            "order_id",
            "status",
            "total_amount",
            "updated_ts",
            "is_deleted",
            "_cdc_lsn",
            "_kafka_offset",
        )
        .orderBy("order_id")
        .collect()
    )
    return hashlib.sha256("\n".join("|".join(map(str, r)) for r in rows).encode()).hexdigest()


def run(spark) -> dict:
    ensure_tables(spark)
    events, latest = latest_events(spark)
    before = content_hash(spark.table(SILVER_TABLE))
    silver_rows(latest).createOrReplaceTempView("cdc_latest")
    spark.sql(silver_merge_sql("cdc_latest"))
    quarantine_rows(spark, events).createOrReplaceTempView("cdc_quarantine")
    spark.sql(
        f"MERGE INTO {QUARANTINE_TABLE} t USING cdc_quarantine s "
        "ON t._kafka_topic = s._kafka_topic AND t._kafka_partition = s._kafka_partition "
        "AND t._kafka_offset = s._kafka_offset WHEN NOT MATCHED THEN INSERT *"
    )
    silver = spark.table(SILVER_TABLE)
    return {
        "orders": silver.count(),
        "live": silver.filter("NOT is_deleted").count(),
        "deleted": silver.filter("is_deleted").count(),
        "quarantined": spark.table(QUARANTINE_TABLE).count(),
        "changed": content_hash(silver) != before,
    }


def verify(spark) -> dict:
    from pyspark.sql import functions as F

    silver = spark.table(SILVER_TABLE)
    _, latest = latest_events(spark)
    expected = silver_rows(latest)
    duplicates = silver.groupBy("order_id").count().filter("count > 1").count()
    result = {
        "orders": silver.count(),
        "duplicate_orders": duplicates,
        "equals_latest_bronze": content_hash(silver) == content_hash(expected),
        "deleted": silver.filter(F.col("is_deleted")).count(),
    }
    result["ok"] = duplicates == 0 and result["equals_latest_bronze"]
    return result


def main(argv=None) -> int:
    args = arguments(argv)
    spark = get_spark(app_name="dp1_cdc_silver")
    try:
        started = time.monotonic()
        if args.mode == "verify":
            result = verify(spark)
            event("cdc_silver_verified", **result)
            return 0 if result["ok"] else 1
        result = run(spark)
        event("cdc_silver_completed", seconds=round(time.monotonic() - started, 1), **result)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
