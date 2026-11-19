"""dp1_cdc: Debezium CDC of walmart.orders from Kafka into Bronze (ADR-0017 §4).

Structured Streaming with `availableNow`: reads every record not yet in the checkpoint, writes each
micro-batch with an idempotent MERGE on Kafka coordinates, then stops. The checkpoint lives on the
`/runtime` volume (the Spark image has no S3A); losing it only replays, never duplicates.
`--mode verify` checks the table read-only: unique coordinates and gap-free offsets per partition.
"""

import argparse
import json
import logging
import os
import time
from datetime import date

from spark.lib.cdc import (
    BRONZE_SCHEMA,
    BRONZE_TABLE,
    PARSED_SCHEMA,
    TOPIC,
    contiguous,
    merge_sql,
    parse_envelope,
)
from spark.lib.session import get_spark

log = logging.getLogger("dp1_cdc")
CHECKPOINT = "/runtime/checkpoints/dp1_cdc"


def event(name: str, **fields) -> None:
    log.info(json.dumps({"event": name, **fields}, sort_keys=True, default=str))


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", default="walmart", choices=("walmart",))
    parser.add_argument("--mode", default="incremental", choices=("incremental", "verify"))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"))
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--max-offsets", type=int, default=20000, help="records per micro-batch")
    args = parser.parse_args(argv)
    if date.fromisoformat(args.run_date).isoformat() != args.run_date:
        raise ValueError("Date must be ISO YYYY-MM-DD")
    return args


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")
    columns = BRONZE_SCHEMA.replace(" string", " STRING").replace(" bigint", " BIGINT")
    columns = (
        columns.replace(" int,", " INT,")
        .replace(" timestamp", " TIMESTAMP")
        .replace(" date", " DATE")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} ({columns}) USING iceberg "
        "PARTITIONED BY (days(_kafka_ts)) TBLPROPERTIES "
        "('format-version'='2','write.parquet.compression-codec'='zstd')"
    )
    expected = spark.createDataFrame([], BRONZE_SCHEMA).schema
    actual = spark.table(BRONZE_TABLE).schema
    if [(f.name, f.dataType) for f in actual] != [(f.name, f.dataType) for f in expected]:
        raise ValueError("Existing Bronze CDC schema differs from contract")


def bronze_rows(batch, batch_id: int, args):
    from pyspark.sql import functions as F

    parse = F.udf(parse_envelope, PARSED_SCHEMA)
    parsed = batch.select(
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_ts"),
        F.col("key").cast("string").alias("_key"),
        F.col("value").cast("string").alias("_raw"),
    ).withColumn("p", parse(F.col("_key"), F.col("_raw")))
    return parsed.select(
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_ts",
        "_key",
        "_raw",
        *[F.col(f"p.{name}").alias(name) for name in ("order_id", "op", "ts_ms", "lsn", "tx_id")],
        F.col("p.before").alias("before"),
        F.col("p.after").alias("after"),
        F.col("p._parse_status").alias("_parse_status"),
        F.col("p._error_reason").alias("_error_reason"),
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(str(batch_id)).alias("_batch_id"),
        F.lit(args.channel).alias("_channel"),
        F.lit("cdc").alias("_source_type"),
        F.lit(args.run_date).cast("date").alias("_run_date"),
    )


def ingest(spark, args) -> dict:
    ensure_table(spark)
    stats = {"batches": 0, "records": 0, "inserted": 0}

    def write(batch, batch_id: int) -> None:
        # foreachBatch hands over a DataFrame of a cloned session: temp views and SQL must use it.
        session = batch.sparkSession
        rows = bronze_rows(batch, batch_id, args).cache()
        count = rows.count()
        if count:
            before = session.table(BRONZE_TABLE).count()
            rows.createOrReplaceTempView("cdc_batch")
            session.sql(merge_sql("cdc_batch"))
            stats["inserted"] += session.table(BRONZE_TABLE).count() - before
        rows.unpersist()
        stats["batches"] += 1
        stats["records"] += count
        event("cdc_bronze_batch", batch_id=batch_id, records=count)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(args.max_offsets))
        .load()
    )
    query = (
        stream.writeStream.foreachBatch(write)
        .option("checkpointLocation", args.checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return stats


def verify(spark, args) -> dict:
    from pyspark.sql import functions as F

    table = spark.table(BRONZE_TABLE)
    duplicates = (
        table.groupBy("_kafka_topic", "_kafka_partition", "_kafka_offset")
        .count()
        .filter("count > 1")
        .count()
    )
    bronze = {
        r["_kafka_partition"]: (r["low"], r["high"], r["n"])
        for r in table.filter(F.col("_kafka_topic") == TOPIC)
        .groupBy("_kafka_partition")
        .agg(
            F.min("_kafka_offset").alias("low"),
            F.max("_kafka_offset").alias("high"),
            F.count("*").alias("n"),
        )
        .collect()
    }
    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .groupBy("partition")
        .agg(F.min("offset").alias("low"))
        .collect()
    )
    earliest = {r["partition"]: r["low"] for r in kafka}
    status = {
        r["_parse_status"]: r["count"] for r in table.groupBy("_parse_status").count().collect()
    }
    result = {
        "rows": sum(n for _, _, n in bronze.values()),
        "duplicates": duplicates,
        "gaps": contiguous(bronze, earliest),
        "parse_status": status,
    }
    result["ok"] = duplicates == 0 and not result["gaps"] and status.get("invalid", 0) == 0
    return result


def main(argv=None) -> int:
    args = arguments(argv)
    spark = get_spark(app_name="dp1_cdc")
    try:
        started = time.monotonic()
        if args.mode == "verify":
            result = verify(spark, args)
            event("cdc_bronze_verified", **result)
            return 0 if result["ok"] else 1
        stats = ingest(spark, args)
        event("cdc_bronze_completed", seconds=round(time.monotonic() - started, 1), **stats)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
