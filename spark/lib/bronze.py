"""Iceberg slice writes and local completion evidence. Import Spark only at runtime."""

import fcntl
import hashlib
import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any

AUDIT_SCHEMA = (
    "_raw string, _source_file string, _source_table string, _parse_status string, "
    "_ingest_ts timestamp, _channel string, _dataset_run_id string, _batch_id string, "
    "_run_date date, _source_type string"
)
CONTENT = ["_raw", "_source_file", "_source_table", "_parse_status"]


def event(name: str, **fields: Any) -> None:
    logging.getLogger("bronze").info(json.dumps({"event": name, **fields}, sort_keys=True))


@contextmanager
def writer_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "bronze.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another Bronze writer/verifier is active") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def save_state(path: Path, state: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True))
    temporary.replace(path)


def content_summary(df: Any) -> dict:
    from pyspark.sql import functions as F

    digest = F.sha2(F.to_json(F.struct(*CONTENT), {"ignoreNullFields": "false"}), 256)
    result = (
        df.select(digest.alias("digest"))
        .agg(
            F.count("*").alias("rows"),
            *[
                F.sum(F.conv(F.substring("digest", start, 15), 16, 10).cast("decimal(38,0)")).alias(
                    f"h{start}"
                )
                for start in (1, 16)
            ],
        )
        .first()
        .asDict()
    )
    return {k: str(v) if v is not None else "0" for k, v in result.items()}


def slice_filter(args: Any):
    from pyspark.sql import functions as F

    return (
        (F.col("_dataset_run_id") == args.run_id)
        & (F.col("_channel") == args.channel)
        & (F.col("_run_date") == F.lit(args.run_date).cast("date"))
        & (F.col("_source_type") == args.source)
    )


def audit(df: Any, args: Any, batch: str):
    from pyspark.sql import functions as F

    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_channel", F.lit(args.channel))
        .withColumn("_dataset_run_id", F.lit(args.run_id))
        .withColumn("_batch_id", F.lit(batch))
        .withColumn("_run_date", F.lit(args.run_date).cast("date"))
        .withColumn("_source_type", F.lit(args.source))
    )


def ensure_table(spark: Any, table: str) -> None:
    columns = (
        AUDIT_SCHEMA.replace(" string", " STRING")
        .replace(" timestamp", " TIMESTAMP")
        .replace(" date", " DATE")
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns}) USING iceberg "
        "PARTITIONED BY (_run_date) TBLPROPERTIES "
        "('format-version'='2','write.parquet.compression-codec'='zstd')"
    )
    expected = spark.createDataFrame([], AUDIT_SCHEMA).schema
    if [(f.name, f.dataType) for f in spark.table(table).schema] != [
        (f.name, f.dataType) for f in expected
    ]:
        raise ValueError("Existing Bronze schema differs from contract")


def snapshot(spark: Any, table: str) -> int:
    return int(
        spark.sql(
            f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
        ).first()[0]
    )


def verify_state(spark: Any, state: dict, args: Any, input_hash: str) -> bool:
    if state.get("status") != "completed" or state.get("input_hash") != input_hash:
        return False
    for table, evidence in state["tables"].items():
        if not spark.catalog.tableExists(table):
            return False
        if (
            spark.sql(
                f"SELECT snapshot_id FROM {table}.snapshots "
                f"WHERE snapshot_id = {int(evidence['snapshot'])}"
            ).count()
            != 1
        ):
            return False
        if content_summary(spark.table(table).filter(slice_filter(args))) != evidence["content"]:
            return False
    return bool(state["tables"])


def replace_slice(spark: Any, table: str, df: Any, args: Any) -> dict:
    ensure_table(spark, table)
    selected = slice_filter(args)
    if df.filter(~selected).limit(1).count():
        raise ValueError("Output escapes selected slice")
    expected = content_summary(df)
    # A delete marker plus insert rows preserves duplicates and supports an empty slice.
    # MERGE avoids relying on truncated file-level statistics to prove a full-file delete.
    from pyspark.sql import functions as F

    marker = spark.createDataFrame([tuple(None for _ in spark.table(table).columns)], AUDIT_SCHEMA)
    source = df.select(*spark.table(table).columns).withColumn("_replace", F.lit(False))
    source = source.unionByName(marker.withColumn("_replace", F.lit(True)))
    source.createOrReplaceTempView("bronze_replacement")
    columns = spark.table(table).columns
    names = ", ".join(columns)
    values = ", ".join("s." + name for name in columns)
    spark.sql(
        f"""MERGE INTO {table} t USING bronze_replacement s
        ON s._replace AND t._dataset_run_id = '{args.run_id}'
        AND t._channel = '{args.channel}' AND t._run_date = DATE '{args.run_date}'
        AND t._source_type = '{args.source}'
        WHEN MATCHED THEN DELETE
        WHEN NOT MATCHED AND NOT s._replace THEN INSERT ({names}) VALUES ({values})"""
    )
    actual = spark.table(table).filter(selected)
    if (
        content_summary(actual) != expected
        or df.select(*CONTENT).exceptAll(actual.select(*CONTENT)).limit(1).count()
    ):
        raise ValueError("Bronze read-back differs from input")
    return {"snapshot": snapshot(spark, table), "content": expected}


def input_digest(manifest_hash: str, args: Any) -> str:
    return hashlib.sha256(
        f"{manifest_hash}:{args.run_id}:{args.run_date}:{args.channel}:{args.source}".encode()
    ).hexdigest()
