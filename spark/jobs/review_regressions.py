"""Boundary and overflow regressions on real Spark, without publishing tables."""

import logging

from spark.lib.bronze import event
from spark.lib.dp3_features import validate_feature_currency
from spark.lib.dp3_labels import compute_order_return_labels
from spark.lib.gold_pipeline import validate_fact_numbers
from spark.lib.session import get_spark


def run():
    spark = get_spark()
    try:
        orders = spark.sql(
            """SELECT 'o' order_key, 'c' customer_key, 'ch' channel_key,
          20260101 order_date_key, 'walmart' channel, 'o' order_id,
          timestamp '2026-01-01 00:00:00' order_ts, false is_deleted,
          'USD' currency, cast(10 as decimal(18,2)) net_amount"""
        )
        returns = spark.sql(
            """SELECT 'o' order_key, 'r' return_key,
          timestamp '2026-01-02 00:00:00' return_ts,
          cast(10 as decimal(18,2)) refund_amount"""
        )
        frames = dict(fact_orders=orders, fact_returns=returns)
        boundary = compute_order_return_labels(
            spark, frames, "fixture", "pin", "2026-01-01", 1
        ).first()
        assert boundary.is_mature is False and boundary.is_returned is None
        observed = compute_order_return_labels(
            spark, frames, "fixture", "pin", "2026-01-02", 1
        ).first()
        assert observed.is_mature and observed.is_returned == 1
        spark.conf.set("spark.sql.ansi.enabled", "false")
        invalid = spark.sql(
            """SELECT 2 qty, cast(6000000000000000 as decimal(18,2)) unit_price,
          cast(3000000000000000 as decimal(18,2)) discount,
          cast(12000000000000000 as decimal(18,2)) gross_line_amount,
          cast(9000000000000000 as decimal(18,2)) net_line_amount"""
        )
        assert invalid.first().gross_line_amount is None
        try:
            validate_fact_numbers(invalid, "fact_order_items")
        except ValueError:
            pass
        else:
            raise AssertionError("Overflow was accepted")
        valid = spark.createDataFrame(
            [("amazon", "USD"), ("shopee", "USD"), ("lazada", "VND")],
            "channel string, currency string",
        )
        validate_feature_currency(valid, "fixture")
        for bad in [("amazon", "EUR"), ("amazon", None), ("unknown", "USD")]:
            mixed = valid.union(spark.createDataFrame([bad], valid.schema))
            try:
                validate_feature_currency(mixed, "fixture")
            except ValueError:
                pass
            else:
                raise AssertionError(f"Unsupported currency accepted: {bad}")
        event("review_regressions_passed", cases=7)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
