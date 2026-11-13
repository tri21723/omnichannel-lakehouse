"""Five-entity normalization and relational DQ with per-occurrence accounting."""

import hashlib
from datetime import date

from spark.lib.silver_entities import AUDIT, FIELDS, rehash, winners
from spark.lib.silver_expand import expand
from spark.lib.silver_normalize import POLICY, normalize


def candidates(spark, frames, state, input_id, key, version):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    selection = state["selection"]
    channel = selection["channel"]
    root_schema = (
        "entity string, _raw string, _source_record_hash string, "
        "_source_ordinal string, _parent_order_id string"
    )
    expanded = None
    if selection["source"] == "jsonl":
        source = next(iter(frames.values()))
        expanded = spark.createDataFrame(
            source.select("_raw").rdd.flatMap(lambda row: expand(row[0], channel)), root_schema
        ).cache()
    audit_schema = spark.createDataFrame([], AUDIT).schema
    audit_names = audit_schema.fieldNames()
    result = {}
    for entity, ddl in FIELDS.items():
        table = (
            f"lh.bronze.walmart_{entity}_raw"
            if selection["source"] == "jdbc"
            else next(iter(frames))
        )
        snapshot = state["tables"][table]["snapshot"]
        if expanded is None:
            source = (
                frames[table]
                .select("_raw")
                .withColumn("_source_record_hash", F.sha2("_raw", 256))
                .withColumn("_source_ordinal", F.lit("jdbc"))
                .withColumn("_parent_order_id", F.get_json_object("_raw", "$.order_id"))
            )
        else:
            source = expanded.filter(F.col("entity") == entity)
        record_schema = spark.createDataFrame([], ddl).schema
        schema = StructType(
            [
                StructField("record", record_schema),
                StructField("_error_reason", ArrayType(StringType())),
            ]
            + list(audit_schema)
        )

        def convert(row, entity=entity, table=table, snapshot=snapshot):
            record, errors = normalize(row["_raw"], entity, channel, key, version)
            return dict(
                record=record or None,
                _error_reason=errors,
                _source_channel=channel,
                _dataset_run_id=selection["run_id"],
                _input_id=input_id,
                _source_table=table,
                _source_snapshot=snapshot,
                _source_batch_id=state["batch_id"],
                _source_record_hash=row["_source_record_hash"]
                or hashlib.sha256(b"<null>").hexdigest(),
                _source_ordinal=row["_source_ordinal"],
                _parent_order_id=row["_parent_order_id"],
                _run_date=date.fromisoformat(selection["run_date"]),
                _record_hash=None,
                _policy_version=POLICY,
            )

        parsed = spark.createDataFrame(source.rdd.map(convert), schema).cache()
        valid = parsed.filter(F.size("_error_reason") == 0).select("record.*", *audit_names)
        rejected = parsed.filter(F.size("_error_reason") > 0).select(*audit_names, "_error_reason")
        result[entity] = dict(valid=valid, rejected=rejected, input_rows=parsed.count())

    def reject(entity, condition, code):
        current = result[entity]["valid"]
        bad = current.filter(condition).select(
            *audit_names, F.array(F.lit(code)).alias("_error_reason")
        )
        result[entity]["rejected"] = result[entity]["rejected"].unionByName(bad)
        result[entity]["valid"] = current.filter(
            ~F.coalesce(condition, F.lit(True))
        ).localCheckpoint(eager=True)

    def membership(entity, parent, keys, code):
        good = (
            result[parent]["valid"].select(*keys).distinct().withColumn("_has_parent", F.lit(True))
        )
        current = result[entity]["valid"].join(good, keys, "left")
        result[entity]["valid"] = current
        reject(entity, F.col("_has_parent").isNull(), code)
        result[entity]["valid"] = result[entity]["valid"].drop("_has_parent")

    membership(
        "orders", "customers", ["channel", "customer_id"], "customer_id:missing_valid_parent"
    )
    membership(
        "order_items", "products", ["channel", "product_id"], "product_id:missing_valid_parent"
    )
    # Match seller to the product, rather than accepting a valid product with a different seller.
    product_sellers = (
        winners(rehash(result["products"]["valid"], "products"), "products")
        .select("channel", "product_id", "seller_id")
        .distinct()
    )
    product_sellers = product_sellers.withColumn("_seller_ok", F.lit(True))
    result["order_items"]["valid"] = result["order_items"]["valid"].join(
        product_sellers, ["channel", "product_id", "seller_id"], "left"
    )
    reject("order_items", F.col("_seller_ok").isNull(), "seller_id:product_mismatch")
    result["order_items"]["valid"] = result["order_items"]["valid"].drop("_seller_ok")

    order_versions = winners(rehash(result["orders"]["valid"], "orders"), "orders").select(
        "channel", "order_id", F.col("updated_ts").alias("_parent_version")
    )
    for entity in ("order_items", "returns"):
        result[entity]["valid"] = (
            result[entity]["valid"]
            .join(order_versions, ["channel", "order_id"], "left")
            .withColumn("updated_ts", F.col("_parent_version"))
            .drop("_parent_version")
        )
    unique_items = winners(rehash(result["order_items"]["valid"], "order_items"), "order_items")
    totals = unique_items.groupBy("channel", "order_id").agg(
        F.sum(F.col("qty") * F.col("unit_price") - F.col("discount")).alias("_line_total")
    )
    orders = result["orders"]["valid"].join(totals, ["channel", "order_id"], "left")
    result["orders"]["valid"] = orders
    reject(
        "orders",
        F.col("_line_total").isNull() | (F.col("_line_total") != F.col("total_amount")),
        "total_amount:item_total_mismatch",
    )
    result["orders"]["valid"] = result["orders"]["valid"].drop("_line_total")
    # Any rejected child whose parent is identifiable invalidates the complete order.
    bad_items = (
        result["order_items"]["rejected"]
        .select(F.lit(channel).alias("channel"), F.col("_parent_order_id").alias("order_id"))
        .distinct()
    )
    result["orders"]["valid"] = result["orders"]["valid"].join(
        bad_items.withColumn("_bad_item", F.lit(True)), ["channel", "order_id"], "left"
    )
    reject("orders", F.col("_bad_item").isNotNull(), "items:invalid_child")
    result["orders"]["valid"] = result["orders"]["valid"].drop("_bad_item")
    membership("order_items", "orders", ["channel", "order_id"], "order_id:missing_valid_parent")
    membership("returns", "orders", ["channel", "order_id"], "order_id:missing_valid_parent")
    membership(
        "returns", "order_items", ["channel", "order_id", "line_no"], "line_no:missing_valid_parent"
    )

    order_times = winners(rehash(result["orders"]["valid"], "orders"), "orders").select(
        "channel", "order_id", F.col("order_ts").alias("_order_time")
    )
    result["returns"]["valid"] = result["returns"]["valid"].join(
        order_times, ["channel", "order_id"], "left"
    )
    reject("returns", F.col("return_ts") < F.col("_order_time"), "return_ts:before_order")
    result["returns"]["valid"] = result["returns"]["valid"].drop("_order_time")
    item_paid = winners(
        rehash(result["order_items"]["valid"], "order_items"), "order_items"
    ).select(
        "channel",
        "order_id",
        "line_no",
        (F.col("qty") * F.col("unit_price") - F.col("discount")).alias("_paid"),
    )
    unique_returns = winners(rehash(result["returns"]["valid"], "returns"), "returns")
    refunds = unique_returns.groupBy("channel", "order_id", "line_no").agg(
        F.sum("refund_amount").alias("_refund")
    )
    result["returns"]["valid"] = result["returns"]["valid"].join(
        item_paid.join(refunds, ["channel", "order_id", "line_no"]),
        ["channel", "order_id", "line_no"],
        "left",
    )
    reject("returns", F.col("_refund") > F.col("_paid"), "refund_amount:exceeds_paid_line")
    result["returns"]["valid"] = result["returns"]["valid"].drop("_paid", "_refund")
    first = (
        result["orders"]["valid"]
        .groupBy("channel", "customer_id")
        .agg(F.min("order_ts").alias("_first"))
    )
    result["customers"]["valid"] = (
        result["customers"]["valid"]
        .join(first, ["channel", "customer_id"], "left")
        .withColumn("first_order_ts", F.col("_first"))
        .drop("_first")
    )
    for entity, value in result.items():
        value["valid"] = rehash(value["valid"], entity).cache()
        value["rejected"] = value["rejected"].cache()
        valid_count, rejected_count = value["valid"].count(), value["rejected"].count()
        if valid_count + rejected_count != value["input_rows"]:
            raise ValueError("Silver occurrence accounting mismatch")
        value["counts"] = dict(
            input=value["input_rows"], valid=valid_count, rejected=rejected_count
        )
    return result
