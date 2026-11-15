"""Data quality, surrogate key, monetary precision and PII boundary assertions for Gold."""

import argparse
import logging
from typing import Any

from pyspark.sql import functions as F

from spark.lib.bronze import event
from spark.lib.session import get_spark


def test_gold_dq(spark: Any, run_id: str) -> None:
    # 1. Inspect fact_orders DQ
    orders = spark.table("lh.gold.fact_orders").filter(F.col("_dataset_run_id") == run_id)
    if orders.count() == 0:
        raise AssertionError(f"fact_orders is empty for run {run_id}")

    # Check key format: 64-char hex
    invalid_keys = orders.filter(~F.col("order_key").rlike("^[0-9a-f]{64}$")).count()
    if invalid_keys > 0:
        raise AssertionError(f"fact_orders has {invalid_keys} malformed order_key hashes")

    # Check non-null monetary fields and currency
    null_monetary = orders.filter(
        F.col("net_amount").isNull()
        | F.col("gross_amount").isNull()
        | F.col("discount_amount").isNull()
        | F.col("currency").isNull()
    ).count()
    if null_monetary > 0:
        raise AssertionError(f"fact_orders has {null_monetary} null monetary or currency values")

    # 2. Inspect fact_order_items DQ
    items = spark.table("lh.gold.fact_order_items").filter(F.col("_dataset_run_id") == run_id)
    if items.count() == 0:
        raise AssertionError(f"fact_order_items is empty for run {run_id}")

    # Line item arithmetic: net_line_amount == gross_line_amount - discount
    bad_line_math = items.filter(
        F.col("net_line_amount") != (F.col("gross_line_amount") - F.col("discount"))
    ).count()
    if bad_line_math > 0:
        raise AssertionError(
            f"fact_order_items has {bad_line_math} rows with invalid net arithmetic"
        )

    # Order item roll-up vs order totals
    item_agg = items.groupBy("order_key").agg(
        F.count("*").alias("calc_item_count"),
        F.sum("qty").alias("calc_total_qty"),
        F.sum("gross_line_amount").alias("calc_gross"),
        F.sum("discount").alias("calc_disc"),
        F.sum("net_line_amount").alias("calc_net"),
    )
    joined_orders = orders.join(item_agg, "order_key")
    mismatched_orders = joined_orders.filter(
        (F.col("item_count") != F.col("calc_item_count"))
        | (F.col("total_qty") != F.col("calc_total_qty"))
        | (F.col("gross_amount") != F.col("calc_gross"))
        | (F.col("discount_amount") != F.col("calc_disc"))
        | (F.col("net_amount") != F.col("calc_net"))
    ).count()
    if mismatched_orders > 0:
        raise AssertionError(
            f"fact_orders has {mismatched_orders} orders not matching item roll-ups"
        )

    # 3. Inspect fact_returns DQ
    returns = spark.table("lh.gold.fact_returns").filter(F.col("_dataset_run_id") == run_id)
    if returns.count() > 0:
        # Check refund <= net_line_amount
        joined_returns = returns.join(
            items.select("order_item_key", "net_line_amount"), "order_item_key"
        )
        excessive_refunds = joined_returns.filter(
            F.col("refund_amount") > F.col("net_line_amount")
        ).count()
        if excessive_refunds > 0:
            raise AssertionError(
                f"fact_returns has {excessive_refunds} returns exceeding line net amount"
            )

    # 4. Inspect dim_customer PII Boundary
    cust = spark.table("lh.gold.dim_customer").filter(F.col("_dataset_run_id") == run_id)
    for forbidden in ("full_name", "email", "name", "phone", "address"):
        if forbidden in cust.columns:
            raise AssertionError(f"dim_customer leaks PII column: {forbidden}")

    invalid_hashes = cust.filter(
        (~F.col("name_hash").rlike("^[0-9a-f]{64}$"))
        | (~F.col("email_hash").rlike("^[0-9a-f]{64}$"))
    ).count()
    if invalid_hashes > 0:
        raise AssertionError(f"dim_customer has {invalid_hashes} invalid SHA-256 hashes")

    # 5. Multi-currency check
    currencies = [row.currency for row in orders.select("currency").distinct().collect()]
    if "VND" not in currencies:
        raise AssertionError("Expected VND currency for Lazada not found")
    if "USD" not in currencies:
        raise AssertionError("Expected USD currency not found")

    event(
        "gold_dq_passed", run_id=run_id, checked_orders=orders.count(), checked_items=items.count()
    )


def run() -> None:
    parser = argparse.ArgumentParser(description="Run Gold Data Quality assertions")
    parser.add_argument("--run-id", default="186ae107d0793d2ee237")
    args = parser.parse_args()

    spark = get_spark()
    try:
        test_gold_dq(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
