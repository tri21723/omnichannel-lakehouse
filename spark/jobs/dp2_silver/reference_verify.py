"""Compare committed Silver keys and native-currency amounts to pinned Bronze inputs."""

import argparse
import json
import logging

from spark.jobs.dp2_silver.main import ROOT
from spark.lib.bronze import event
from spark.lib.session import get_spark
from spark.lib.silver_entities import KEYS
from spark.lib.silver_input import read_pinned
from spark.lib.silver_publish import frame_at, verify


def check_reference(spark, run_id):
    from pyspark.sql import functions as F

    ledger = json.loads((ROOT / "ledger.json").read_text())
    verify(spark, ledger)
    expected = {entity: [] for entity in KEYS}
    amount_parts, refund_parts = [], []
    for info in ledger["slices"].values():
        selection = info["selection"]
        if selection["run_id"] != run_id:
            continue
        pin = ROOT.parent / "inputs" / (info["input_id"] + ".json")
        frames = read_pinned(spark, pin)
        channel = selection["channel"]
        if selection["source"] == "jdbc":
            for entity in KEYS:
                frame = frames[f"lh.bronze.walmart_{entity}_raw"]
                columns = [
                    F.get_json_object("_raw", "$." + name).alias(name)
                    for name in KEYS[entity]
                    if name != "channel"
                ]
                expected[entity].append(frame.select(F.lit(channel).alias("channel"), *columns))
            orders = frames["lh.bronze.walmart_orders_raw"].select(
                F.get_json_object("_raw", "$.order_id").alias("order_id"),
                F.get_json_object("_raw", "$.currency").alias("currency"),
                F.get_json_object("_raw", "$.total_amount").cast("decimal(18,2)").alias("amount"),
            )
            returns = frames["lh.bronze.walmart_returns_raw"].select(
                F.get_json_object("_raw", "$.order_id").alias("order_id"),
                F.get_json_object("_raw", "$.return_id").alias("return_id"),
                F.get_json_object("_raw", "$.return_line_no").alias("return_line_no"),
                F.get_json_object("_raw", "$.refund_amount").cast("decimal(18,2)").alias("refund"),
            )
        else:
            frame = next(iter(frames.values()))
            schema = (
                "order_id string, currency string, total_amount string, "
                "customer struct<customer_id:string>, "
                "items array<struct<line_no:int,product_id:string,"
                "product:struct<product_id:string>>>, "
                "returns array<struct<order_id:string,return_id:string,"
                "return_line_no:int,refund_amount:string>>"
            )
            parsed = frame.select(F.from_json("_raw", schema).alias("r")).select("r.*")
            expected["orders"].append(parsed.select(F.lit(channel).alias("channel"), "order_id"))
            expected["customers"].append(
                parsed.select(F.lit(channel).alias("channel"), "customer.customer_id")
            )
            items = parsed.select("order_id", F.explode("items").alias("i"))
            expected["order_items"].append(
                items.select(
                    F.lit(channel).alias("channel"),
                    "order_id",
                    F.col("i.line_no").cast("string").alias("line_no"),
                )
            )
            expected["products"].append(
                items.select(
                    F.lit(channel).alias("channel"),
                    F.col("i.product.product_id").alias("product_id"),
                )
            )
            returns = parsed.select("order_id", F.explode("returns").alias("r")).select(
                "order_id",
                "r.return_id",
                F.col("r.return_line_no").cast("string").alias("return_line_no"),
                F.col("r.refund_amount").cast("decimal(18,2)").alias("refund"),
            )
            expected["returns"].append(
                returns.select(F.lit(channel).alias("channel"), "return_id", "return_line_no")
            )
            orders = parsed.select(
                "order_id", "currency", F.col("total_amount").cast("decimal(18,2)").alias("amount")
            )
        orders = orders.dropDuplicates(["order_id"])
        amount_parts.append(orders.withColumn("channel", F.lit(channel)))
        refund_parts.append(
            returns.dropDuplicates(["return_id", "return_line_no"])
            .join(orders.select("order_id", "currency"), "order_id")
            .withColumn("channel", F.lit(channel))
        )
    if not all(expected.values()):
        raise ValueError("No complete reference input set for requested dataset")

    def union(parts):
        result = parts[0]
        for part in parts[1:]:
            result = result.unionByName(part)
        return result

    outputs, counts = {}, {}
    for entity, parts in expected.items():
        table = "lh.silver." + entity
        actual = frame_at(spark, table, ledger["tables"][table], spark.table(table).schema).filter(
            F.col("_dataset_run_id") == run_id
        )
        outputs[entity] = actual
        # Normalize key representation for INT vs raw JSON string extraction.
        columns = KEYS[entity]
        source_keys = (
            union(parts).select(*[F.col(k).cast("string").alias(k) for k in columns]).distinct()
        )
        actual_keys = actual.select(*[F.col(k).cast("string").alias(k) for k in columns])
        if (
            source_keys.exceptAll(actual_keys).limit(1).count()
            or actual_keys.exceptAll(source_keys).limit(1).count()
        ):
            raise ValueError("Silver keys differ from authoritative source")
        counts[entity] = actual.count()
    source_amounts = (
        union(amount_parts).groupBy("channel", "currency").agg(F.sum("amount").alias("amount"))
    )
    actual_amounts = (
        outputs["orders"].groupBy("channel", "currency").agg(F.sum("total_amount").alias("amount"))
    )
    # Break the aggregate/ExceptAll lineage: Spark 3.5 can otherwise prune a grouping
    # attribute while rewriting ExceptAll's duplicate expansion.
    source_amounts = source_amounts.localCheckpoint(eager=True)
    actual_amounts = actual_amounts.localCheckpoint(eager=True)
    if (
        source_amounts.exceptAll(actual_amounts).count()
        or actual_amounts.exceptAll(source_amounts).count()
    ):
        raise ValueError("Native-currency order amount mismatch")
    source_refunds = (
        union(refund_parts).groupBy("channel", "currency").agg(F.sum("refund").alias("refund"))
    )
    actual_refunds = (
        outputs["returns"]
        .join(outputs["orders"].select("channel", "order_id", "currency"), ["channel", "order_id"])
        .groupBy("channel", "currency")
        .agg(F.sum("refund_amount").alias("refund"))
    )
    source_refunds = source_refunds.localCheckpoint(eager=True)
    actual_refunds = actual_refunds.localCheckpoint(eager=True)
    if (
        source_refunds.exceptAll(actual_refunds).count()
        or actual_refunds.exceptAll(source_refunds).count()
    ):
        raise ValueError("Native-currency refund mismatch")
    event("silver_reference_verified", run_id=run_id, counts=counts, currencies_separate=True)
    return counts


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    spark = get_spark()
    try:
        check_reference(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
