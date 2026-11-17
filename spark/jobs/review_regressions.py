"""Boundary and overflow regressions on real Spark, without publishing tables."""

import logging

from spark.lib.bronze import event
from spark.lib.dp3_features import (
    compute_customer_daily_features,
    compute_product_daily_features,
    validate_feature_currency,
)
from spark.lib.dp3_labels import compute_order_return_labels
from spark.lib.gold_pipeline import validate_fact_numbers
from spark.lib.numeric import checked_int
from spark.lib.session import get_spark


def check_feature_numbers(spark):
    orders = spark.sql(
        """SELECT cast(id as string) order_key, 'amazon' channel,
        'c' customer_id, timestamp '2026-01-01 12:00:00' order_ts,
        false is_deleted, 'USD' currency,
        cast(6000000000000000 as decimal(18,2)) net_amount FROM range(2)"""
    )
    frames = {
        "fact_orders": orders,
        "fact_returns": spark.createDataFrame(
            [],
            "order_key string, channel string, "
            "return_key string, product_key string, return_ts timestamp, "
            "refund_amount decimal(18,2), currency string",
        ),
        "fact_order_items": orders.selectExpr(
            "*",
            "'p' product_id",
            "1 qty",
            "net_amount gross_line_amount",
            "net_amount net_line_amount",
            "cast(0 as decimal(18,2)) discount",
        ),
        "dim_customer": spark.createDataFrame(
            [("ck", "amazon", "c")], "customer_key string, channel string, customer_id string"
        ),
        "dim_product": spark.createDataFrame(
            [("pk", "sk", "amazon", "p", "s")],
            "product_key string, seller_key string, channel string, "
            "product_id string, seller_id string",
        ),
        "dim_channel": spark.createDataFrame(
            [("ch", "amazon")], "channel_key string, channel string"
        ),
    }
    for compute in (compute_customer_daily_features, compute_product_daily_features):
        try:
            compute(spark, frames, "fixture", "pin", "2026-01-03").collect()
        except ValueError as exc:
            assert "numeric overflow" in str(exc)
        else:
            raise AssertionError("Feature monetary overflow accepted")
    from pyspark.sql import functions as F

    normal = dict(
        frames, fact_orders=orders.withColumn("net_amount", F.lit(6).cast("decimal(18,2)"))
    )
    row = compute_customer_daily_features(spark, normal, "fixture", "pin", "2026-01-03").first()
    assert row.lifetime_net_amount == 12 and row.spend_30d == 12 and row.aov_30d == 6
    three_orders = normal["fact_orders"].unionByName(
        normal["fact_orders"].limit(1).withColumn("order_key", F.lit("third"))
    )
    quantity_frames = dict(
        normal,
        fact_orders=three_orders,
        fact_order_items=three_orders.selectExpr(
            "*",
            "'p' product_id",
            "2000000000 qty",
            "cast(0 as decimal(18,2)) gross_line_amount",
            "cast(0 as decimal(18,2)) net_line_amount",
            "cast(0 as decimal(18,2)) discount",
        ),
    )
    try:
        compute_product_daily_features(
            spark, quantity_frames, "fixture", "pin", "2026-01-03"
        ).collect()
    except Exception as exc:
        assert "nonnegative INT range" in str(exc)
    else:
        raise AssertionError("Positive-wrapping quantity overflow accepted")
    empty = dict(normal, fact_orders=normal["fact_orders"].limit(0))
    row = compute_customer_daily_features(spark, empty, "fixture", "pin", "2026-01-03").first()
    assert row.lifetime_order_count == 0 and row.spend_30d == 0 and row.aov_30d is None


def check_int_bounds(spark):
    for ansi in ("false", "true"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        valid = spark.sql("SELECT 0L n UNION ALL SELECT 2147483647L n")
        assert sorted(r.n for r in valid.select(checked_int(valid.n).alias("n")).collect()) == [
            0,
            2147483647,
        ]
        for sql in (
            "SELECT -1L n",
            "SELECT 2147483648L n",
            "SELECT cast(NULL as bigint) n",
            "SELECT sum(2000000000L) n FROM range(3)",
        ):
            frame = spark.sql(sql)
            try:
                frame.select(checked_int(frame.n)).collect()
            except Exception as exc:
                assert "nonnegative INT range" in str(exc)
            else:
                raise AssertionError("Invalid INT narrowing accepted")
    spark.conf.set("spark.sql.ansi.enabled", "false")


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
        check_feature_numbers(spark)
        check_int_bounds(spark)
        event("review_regressions_passed", cases=22)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
