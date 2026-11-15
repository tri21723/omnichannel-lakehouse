"""Data quality, point-in-time leakage, and maturity censoring assertions for DP3."""

import argparse
import logging
from datetime import datetime
from typing import Any

from pyspark.sql import functions as F

from spark.lib.bronze import event
from spark.lib.dp3_features import (
    compute_customer_daily_features,
    compute_product_daily_features,
)
from spark.lib.dp3_input import DEFAULT_GOLD_ROOT, load_gold_publication, read_gold_entities
from spark.lib.dp3_labels import compute_order_return_labels
from spark.lib.session import get_spark


def test_dp3_dq(spark: Any, run_id: str) -> None:
    # 1. Inspect feat_customer_daily
    cust_feat = spark.table("lh.gold.feat_customer_daily").filter(
        F.col("_dataset_run_id") == run_id
    )
    if cust_feat.count() == 0:
        raise AssertionError(f"feat_customer_daily is empty for run {run_id}")

    # Check key format: 64-char hex
    invalid_cust_keys = cust_feat.filter(
        ~F.col("customer_feature_key").rlike("^[0-9a-f]{64}$")
    ).count()
    if invalid_cust_keys > 0:
        raise AssertionError(
            f"feat_customer_daily has {invalid_cust_keys} malformed customer_feature_key hashes"
        )

    # Check surrogate key uniqueness within grain (run_id, channel, customer_id, as_of_date)
    dup_cust = cust_feat.groupBy("customer_feature_key").count().filter("count > 1").count()
    if dup_cust > 0:
        raise AssertionError(
            f"feat_customer_daily has {dup_cust} duplicate customer_feature_key values"
        )

    # Check window order containment: orders_7d <= orders_30d <= lifetime_order_count
    bad_order_windows = cust_feat.filter(
        (F.col("orders_7d") > F.col("orders_30d"))
        | (F.col("orders_30d") > F.col("lifetime_order_count"))
    ).count()
    if bad_order_windows > 0:
        raise AssertionError(
            f"feat_customer_daily has {bad_order_windows} rows violating "
            "7d <= 30d <= lifetime order count"
        )

    # Check monetary non-negativity and bounds
    bad_spend = cust_feat.filter(
        (F.col("spend_7d") < 0)
        | (F.col("spend_30d") < 0)
        | (F.col("lifetime_net_amount") < 0)
        | (F.col("spend_7d") > F.col("spend_30d"))
        | (F.col("spend_30d") > F.col("lifetime_net_amount"))
    ).count()
    if bad_spend > 0:
        raise AssertionError(
            f"feat_customer_daily has {bad_spend} rows violating "
            "spend_7d <= spend_30d <= lifetime_net_amount"
        )

    # Check return_rate_30d bounds [0.0, 1.0]
    bad_cust_rate = cust_feat.filter(
        (F.col("return_rate_30d") < 0) | (F.col("return_rate_30d") > 1)
    ).count()
    if bad_cust_rate > 0:
        raise AssertionError(
            f"feat_customer_daily has {bad_cust_rate} rows with return_rate_30d out of range [0, 1]"
        )

    # 2. Inspect feat_product_daily
    prod_feat = spark.table("lh.gold.feat_product_daily").filter(F.col("_dataset_run_id") == run_id)
    if prod_feat.count() == 0:
        raise AssertionError(f"feat_product_daily is empty for run {run_id}")

    invalid_prod_keys = prod_feat.filter(
        ~F.col("product_feature_key").rlike("^[0-9a-f]{64}$")
    ).count()
    if invalid_prod_keys > 0:
        raise AssertionError(
            f"feat_product_daily has {invalid_prod_keys} malformed product_feature_key hashes"
        )

    dup_prod = prod_feat.groupBy("product_feature_key").count().filter("count > 1").count()
    if dup_prod > 0:
        raise AssertionError(
            f"feat_product_daily has {dup_prod} duplicate product_feature_key values"
        )

    # Check 7d <= 30d for orders, units and returns
    bad_prod_windows = prod_feat.filter(
        (F.col("orders_7d") > F.col("orders_30d"))
        | (F.col("units_sold_7d") > F.col("units_sold_30d"))
        | (F.col("return_count_7d") > F.col("return_count_30d"))
        | (F.col("refund_amount_7d") > F.col("refund_amount_30d"))
    ).count()
    if bad_prod_windows > 0:
        raise AssertionError(
            f"feat_product_daily has {bad_prod_windows} rows violating 7d <= 30d metric windowing"
        )

    # 3. Inspect lbl_order_return
    labels = spark.table("lh.gold.lbl_order_return").filter(F.col("_dataset_run_id") == run_id)
    if labels.count() == 0:
        raise AssertionError(f"lbl_order_return is empty for run {run_id}")

    invalid_label_keys = labels.filter(~F.col("order_label_key").rlike("^[0-9a-f]{64}$")).count()
    if invalid_label_keys > 0:
        raise AssertionError(
            f"lbl_order_return has {invalid_label_keys} malformed order_label_key hashes"
        )

    dup_labels = labels.groupBy("order_label_key").count().filter("count > 1").count()
    if dup_labels > 0:
        raise AssertionError(f"lbl_order_return has {dup_labels} duplicate order_label_key values")

    # Maturity Censoring verification:
    # 1. Immature orders must have 100% NULL labels
    immature_leaks = labels.filter(
        (~F.col("is_mature"))
        & (
            F.col("is_returned").isNotNull()
            | F.col("first_return_ts").isNotNull()
            | F.col("days_to_return").isNotNull()
            | F.col("return_line_count").isNotNull()
            | F.col("refund_amount").isNotNull()
            | F.col("refund_ratio").isNotNull()
        )
    ).count()
    if immature_leaks > 0:
        raise AssertionError(
            f"lbl_order_return has {immature_leaks} immature orders with non-null labels "
            "(censoring violation)"
        )

    # 2. Mature orders must have non-null is_returned in {0, 1}
    mature = labels.filter(F.col("is_mature"))
    bad_mature_returned = mature.filter(
        F.col("is_returned").isNull() | (~F.col("is_returned").isin(0, 1))
    ).count()
    if bad_mature_returned > 0:
        raise AssertionError(
            f"lbl_order_return has {bad_mature_returned} mature orders with invalid is_returned"
        )

    # 3. Return count and ratio checks for mature returned orders
    mature_returned = mature.filter(F.col("is_returned") == 1)
    bad_returned_lines = mature_returned.filter(
        (F.col("return_line_count") <= 0)
        | (F.col("refund_amount") <= 0)
        | (F.col("days_to_return") < 0)
        | (F.col("refund_ratio") <= 0)
        | (F.col("refund_ratio") > 1.0)
    ).count()
    if bad_returned_lines > 0:
        raise AssertionError(
            f"lbl_order_return has {bad_returned_lines} returned orders with "
            "invalid line, amount, or ratio"
        )

    # 4. Maturity censoring boundary test (2026-01-03 vs 2026-01-18)
    gold_ledger, gold_pub_id = load_gold_publication(DEFAULT_GOLD_ROOT / "ledger.json")
    frames, _, pub_id = read_gold_entities(spark, gold_ledger, run_id, publication_id=gold_pub_id)
    expected_returns = (
        frames["fact_returns"]
        .join(
            frames["fact_orders"]
            .filter(~F.coalesce(F.col("is_deleted"), F.lit(False)))
            .select("order_key", "order_ts"),
            on="order_key",
            how="inner",
        )
        .filter(
            (F.col("return_ts") >= F.col("order_ts"))
            & (F.col("return_ts") <= F.col("order_ts") + F.expr("INTERVAL 14 DAYS"))
        )
        .select("order_key")
        .distinct()
        .count()
    )

    labels_early = labels.filter(F.col("as_of_date").cast("string") == "2026-01-03")
    if labels_early.count() == 0 or labels_early.filter(F.col("is_mature")).count() > 0:
        raise AssertionError("Expected 100% immature orders at as_of_date = 2026-01-03")

    labels_mature = labels.filter(F.col("as_of_date").cast("string") == "2026-01-18")
    if labels_mature.count() == 0 or labels_mature.filter(~F.col("is_mature")).count() > 0:
        raise AssertionError("Expected 100% mature orders at as_of_date = 2026-01-18")

    actual_mature_returns = labels_mature.filter(F.col("is_returned") == 1).count()
    if actual_mature_returns != expected_returns:
        raise AssertionError(
            f"Mature return count mismatch at 2026-01-18: "
            f"got {actual_mature_returns}, expected {expected_returns}"
        )

    # 5. Point-in-time future event injection leakage test
    future_ts = datetime(2026, 1, 4, 0, 0, 0)
    future_order = (
        frames["fact_orders"]
        .limit(1)
        .withColumn("order_key", F.lit("f" * 64))
        .withColumn("order_id", F.lit("future_order_leak_test"))
        .withColumn("order_ts", F.lit(future_ts))
    )
    future_return = (
        frames["fact_returns"]
        .limit(1)
        .withColumn("return_key", F.lit("e" * 64))
        .withColumn("return_id", F.lit("future_return_leak_test"))
        .withColumn("return_ts", F.lit(future_ts))
    )
    injected_frames = dict(frames)
    injected_frames["fact_orders"] = frames["fact_orders"].unionByName(future_order)
    injected_frames["fact_returns"] = frames["fact_returns"].unionByName(future_return)

    inj_cust = compute_customer_daily_features(spark, injected_frames, run_id, pub_id, "2026-01-03")
    inj_prod = compute_product_daily_features(spark, injected_frames, run_id, pub_id, "2026-01-03")
    inj_lbl = compute_order_return_labels(
        spark, injected_frames, run_id, pub_id, "2026-01-03", horizon_days=14
    )
    pub_cust_03 = cust_feat.filter(F.col("as_of_date").cast("string") == "2026-01-03")
    pub_prod_03 = prod_feat.filter(F.col("as_of_date").cast("string") == "2026-01-03")
    if (
        inj_cust.exceptAll(pub_cust_03).limit(1).count() > 0
        or pub_cust_03.exceptAll(inj_cust).limit(1).count() > 0
        or inj_prod.exceptAll(pub_prod_03).limit(1).count() > 0
        or pub_prod_03.exceptAll(inj_prod).limit(1).count() > 0
        or inj_lbl.exceptAll(labels_early).limit(1).count() > 0
        or labels_early.exceptAll(inj_lbl).limit(1).count() > 0
    ):
        raise AssertionError("Point-in-time leakage detected after injecting future events")

    # 6. Point-in-time training join test (customer & product features as of order date)
    order_items = frames["fact_order_items"].select("order_key", "product_key").distinct()
    joined_training = (
        labels_mature.alias("lbl")
        .join(order_items.alias("oi"), on="order_key", how="inner")
        .join(
            cust_feat.alias("cf"),
            on=(
                (F.col("lbl._dataset_run_id") == F.col("cf._dataset_run_id"))
                & (F.col("lbl.customer_key") == F.col("cf.customer_key"))
                & (F.to_date(F.col("lbl.order_ts")) == F.col("cf.as_of_date"))
            ),
            how="inner",
        )
        .join(
            prod_feat.alias("pf"),
            on=(
                (F.col("lbl._dataset_run_id") == F.col("pf._dataset_run_id"))
                & (F.col("oi.product_key") == F.col("pf.product_key"))
                & (F.to_date(F.col("lbl.order_ts")) == F.col("pf.as_of_date"))
            ),
            how="inner",
        )
    )
    if joined_training.count() == 0:
        raise AssertionError(
            "Point-in-time training join between labels and features yielded 0 rows"
        )

    leakage_joins = joined_training.filter(
        (F.col("lbl.order_ts") >= F.col("cf.as_of_ts"))
        | (F.col("lbl.order_ts") >= F.col("pf.as_of_ts"))
    ).count()
    if leakage_joins > 0:
        raise AssertionError(
            f"Training join revealed {leakage_joins} rows where order_ts >= as_of_ts "
            "(future leakage)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify DP3 data quality and censoring.")
    parser.add_argument("--run-id", required=True, help="Dataset run ID to test")
    args = parser.parse_args()

    spark = get_spark()
    try:
        test_dp3_dq(spark, args.run_id)
        event("dp3_dq_passed", run_id=args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
