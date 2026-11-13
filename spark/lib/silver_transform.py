"""Distributed typed candidates and PII-safe rejection frames for Walmart."""

import hashlib
import json
from typing import Any

from spark.lib.silver_normalize import POLICY, normalize

SCHEMAS = {
    "orders": (
        "channel string, order_id string, customer_id string, order_ts "
        "timestamp, updated_ts timestamp, status string, currency string, "
        "total_amount decimal(18,2), shipping_country string, is_deleted "
        "boolean"
    ),
    "customers": (
        "channel string, customer_id string, name_hash string, email_hash "
        "string, country string, updated_ts timestamp, _pii_key_version string"
    ),
    "order_items": (
        "channel string, order_id string, line_no int, product_id string, "
        "seller_id string, qty int, unit_price decimal(18,2), discount "
        "decimal(18,2)"
    ),
}
LINEAGE = (
    "_dataset_run_id string, _input_id string, _source_table string, "
    "_source_snapshot long, _source_batch_id string, _source_record_hash "
    "string, _record_hash string, _policy_version string"
)


def parsed_candidates(
    spark: Any, source: Any, entity: str, state: dict, input_id: str, key: bytes, version: str
) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    table = f"lh.bronze.walmart_{entity}_raw"
    snapshot = state["tables"][table]["snapshot"]
    selection = state["selection"]

    def convert(row):
        raw = row["_raw"]
        record, reasons = normalize(raw, entity, selection["channel"], key, version)
        canonical = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
        return {
            "record": record or None,
            "_error_reason": reasons,
            "_dataset_run_id": selection["run_id"],
            "_input_id": input_id,
            "_source_table": table,
            "_source_snapshot": snapshot,
            "_source_batch_id": state["batch_id"],
            "_source_record_hash": hashlib.sha256(raw.encode()).hexdigest(),
            "_record_hash": hashlib.sha256(canonical.encode()).hexdigest() if record else None,
            "_policy_version": POLICY,
        }

    # Use Spark's DDL parser to construct the nested schema without changing field names.
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    record_schema = spark.createDataFrame([], SCHEMAS[entity]).schema
    metadata = spark.createDataFrame([], LINEAGE).schema
    envelope = StructType(
        [
            StructField("record", record_schema),
            StructField("_error_reason", ArrayType(StringType())),
        ]
        + list(metadata)
    )
    parsed = spark.createDataFrame(source.select("_raw").rdd.map(convert), envelope).cache()
    audit = [field.name for field in metadata]
    valid = parsed.filter(F.size("_error_reason") == 0).select("record.*", *audit)
    rejected = parsed.filter(F.size("_error_reason") > 0).select(*audit, "_error_reason")
    return valid, rejected


def walmart_candidates(
    spark: Any, frames: dict, state: dict, input_id: str, key: bytes, version: str
) -> dict:
    from pyspark.sql import functions as F

    output = {}
    for entity in SCHEMAS:
        table = f"lh.bronze.walmart_{entity}_raw"
        valid, rejected = parsed_candidates(
            spark, frames[table], entity, state, input_id, key, version
        )
        count = frames[table].count()
        assert valid.count() + rejected.count() == count
        output[entity] = {"valid": valid, "rejected": rejected, "input_rows": count}
    orders = output["orders"]["valid"]
    item_rows = output["order_items"]["valid"]
    totals = item_rows.groupBy("order_id").agg(
        F.sum(F.col("qty") * F.col("unit_price") - F.col("discount")).alias("line_total")
    )
    checked = orders.join(totals, "order_id", "left")
    mismatch = F.col("line_total").isNull() | (F.col("total_amount") != F.col("line_total"))
    audit_cols = [field.strip().split()[0] for field in LINEAGE.split(",")]
    bad_orders = checked.filter(mismatch).select(
        *audit_cols, F.array(F.lit("total_amount:item_total_mismatch")).alias("_error_reason")
    )
    output["orders"]["rejected"] = output["orders"]["rejected"].unionByName(bad_orders)
    orders = checked.filter(~mismatch).drop("line_total")
    output["orders"]["valid"] = orders
    parents = orders.groupBy("order_id").agg(
        F.max(F.struct("updated_ts", "_record_hash")).alias("p")
    )
    items = output["order_items"]["valid"].join(parents, "order_id", "left")
    orphan = items.filter(F.col("p").isNull()).select(
        *[field.strip().split()[0] for field in LINEAGE.split(",")],
        F.array(F.lit("order_id:missing_valid_parent")).alias("_error_reason"),
    )
    output["order_items"]["rejected"] = output["order_items"]["rejected"].unionByName(orphan)
    output["order_items"]["valid"] = (
        items.filter(F.col("p").isNotNull())
        .withColumn("updated_ts", F.col("p.updated_ts"))
        .drop("p")
    )
    first = orders.groupBy("customer_id").agg(F.min("order_ts").alias("first_order_ts"))
    output["customers"]["valid"] = output["customers"]["valid"].join(first, "customer_id", "left")
    return output
