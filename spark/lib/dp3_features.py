"""Feature engineering logic for feat_customer_daily and feat_product_daily."""

from datetime import date, datetime, time, timedelta
from typing import Any

from spark.lib.dp3_entities import (
    POLICY,
    get_channel_currency,
    rehash_dp3,
)


def _to_date(val: str | date) -> date:
    if isinstance(val, str):
        return date.fromisoformat(val)
    return val


def validate_feature_currency(frame: Any, name: str) -> None:
    """Current feature grain supports exactly the configured native channel currency."""
    from pyspark.sql import functions as F

    supported = F.col("channel").isin("walmart", "amazon", "tiktok_shop", "shopee", "lazada", "pos")
    invalid = (~F.coalesce(supported, F.lit(False))) | (
        ~F.col("currency").eqNullSafe(get_channel_currency(F.col("channel")))
    )
    if frame.filter(invalid).limit(1).count():
        raise ValueError(f"Unsupported or mixed feature currency in {name}")


def validate_feature_aggregates(frame: Any, name: str) -> None:
    """Reject invalid aggregate casts before joins can replace NULL with zero."""
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    numeric = [f.name for f in frame.schema if isinstance(f.dataType, NumericType)]
    invalid = reduce(or_, (F.col(c).isNull() | (F.col(c) < 0) for c in numeric))
    if frame.filter(invalid).limit(1).count():
        raise ValueError(f"Feature numeric overflow or invalid aggregate in {name}")


def compute_customer_daily_features(
    spark: Any,
    frames: dict[str, Any],
    run_id: str,
    pub_id: str,
    as_of_date: str | date,
) -> Any:
    """Compute point-in-time feat_customer_daily for a single as_of_date."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, TimestampType

    eval_date = _to_date(as_of_date)
    next_date = eval_date + timedelta(days=1)
    as_of_ts = datetime.combine(next_date, time(0, 0, 0))
    ts_7d = datetime.combine(next_date - timedelta(days=7), time(0, 0, 0))
    ts_30d = datetime.combine(next_date - timedelta(days=30), time(0, 0, 0))
    date_key = int(eval_date.strftime("%Y%m%d"))

    # Active orders placed strictly before as_of_ts
    orders = frames["fact_orders"].filter(
        (F.col("order_ts") < F.lit(as_of_ts)) & (~F.coalesce(F.col("is_deleted"), F.lit(False)))
    )

    validate_feature_currency(orders, "fact_orders")

    # Recency, frequency, monetary aggregations per (channel, customer_id)
    order_aggs = orders.groupBy("channel", "customer_id").agg(
        F.max("order_ts").alias("last_order_ts"),
        F.datediff(F.lit(eval_date), F.to_date(F.max("order_ts"))).alias("days_since_last_order"),
        F.count("order_key").cast(IntegerType()).alias("lifetime_order_count"),
        F.coalesce(F.sum("net_amount"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("lifetime_net_amount"),
        F.count(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("order_key")))
        .cast(IntegerType())
        .alias("orders_7d"),
        F.count(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("order_key")))
        .cast(IntegerType())
        .alias("orders_30d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("net_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("spend_7d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("net_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("spend_30d"),
    )

    validate_feature_aggregates(order_aggs, "customer orders")

    # Returns within dual-timestamp boundary (order_ts < as_of_ts AND return_ts < as_of_ts)
    # for orders placed in the 30-day window
    returns = frames["fact_returns"].join(
        orders.select("order_key", "order_ts", "customer_id"),
        on="order_key",
        how="inner",
    )
    valid_returns_30d = returns.filter(
        (F.col("order_ts") >= F.lit(ts_30d))
        & (F.col("order_ts") < F.lit(as_of_ts))
        & (F.col("return_ts") < F.lit(as_of_ts))
    )

    validate_feature_currency(valid_returns_30d, "fact_returns")

    return_aggs = valid_returns_30d.groupBy("channel", "customer_id").agg(
        F.countDistinct("order_key").cast(IntegerType()).alias("returned_orders_30d"),
        F.coalesce(F.sum("refund_amount"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("refund_amount_30d"),
    )

    validate_feature_aggregates(return_aggs, "customer returns")

    # Base customers from dim_customer
    dim_cust = frames["dim_customer"].select(
        "customer_key",
        "channel",
        "customer_id",
    )
    dim_chan = frames["dim_channel"].select("channel_key", "channel").distinct()

    joined = (
        dim_cust.join(dim_chan, on="channel", how="left")
        .join(order_aggs, on=["channel", "customer_id"], how="left")
        .join(return_aggs, on=["channel", "customer_id"], how="left")
    )

    result = joined.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.lit(run_id),
                F.col("channel"),
                F.col("customer_id"),
                F.lit(eval_date.isoformat()),
            ),
            256,
        ).alias("customer_feature_key"),
        "customer_key",
        "channel_key",
        F.lit(date_key).cast(IntegerType()).alias("as_of_date_key"),
        "channel",
        "customer_id",
        F.lit(eval_date).cast(DateType()).alias("as_of_date"),
        F.lit(as_of_ts).cast(TimestampType()).alias("as_of_ts"),
        get_channel_currency(F.col("channel")).alias("currency"),
        "last_order_ts",
        F.col("days_since_last_order").cast(IntegerType()).alias("days_since_last_order"),
        F.coalesce(F.col("lifetime_order_count"), F.lit(0)).alias("lifetime_order_count"),
        F.coalesce(F.col("lifetime_net_amount"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("lifetime_net_amount"),
        F.coalesce(F.col("orders_7d"), F.lit(0)).alias("orders_7d"),
        F.coalesce(F.col("orders_30d"), F.lit(0)).alias("orders_30d"),
        F.coalesce(F.col("spend_7d"), F.lit(0)).cast("decimal(18,2)").alias("spend_7d"),
        F.coalesce(F.col("spend_30d"), F.lit(0)).cast("decimal(18,2)").alias("spend_30d"),
        F.when(
            F.coalesce(F.col("orders_30d"), F.lit(0)) > 0,
            (F.col("spend_30d") / F.col("orders_30d")).cast("decimal(18,2)"),
        )
        .otherwise(F.lit(None))
        .alias("aov_30d"),
        F.coalesce(F.col("returned_orders_30d"), F.lit(0)).alias("returned_orders_30d"),
        F.coalesce(F.col("refund_amount_30d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("refund_amount_30d"),
        F.when(
            F.coalesce(F.col("orders_30d"), F.lit(0)) > 0,
            (F.coalesce(F.col("returned_orders_30d"), F.lit(0)) / F.col("orders_30d")).cast(
                "decimal(18,4)"
            ),
        )
        .otherwise(F.lit(0).cast("decimal(18,4)"))
        .alias("return_rate_30d"),
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_gold_publication_id"),
        F.lit(POLICY).alias("_policy_version"),
    )

    return rehash_dp3(result, "feat_customer_daily")


def compute_product_daily_features(
    spark: Any,
    frames: dict[str, Any],
    run_id: str,
    pub_id: str,
    as_of_date: str | date,
) -> Any:
    """Compute point-in-time feat_product_daily for a single as_of_date."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, TimestampType

    eval_date = _to_date(as_of_date)
    next_date = eval_date + timedelta(days=1)
    as_of_ts = datetime.combine(next_date, time(0, 0, 0))
    ts_7d = datetime.combine(next_date - timedelta(days=7), time(0, 0, 0))
    ts_30d = datetime.combine(next_date - timedelta(days=30), time(0, 0, 0))
    date_key = int(eval_date.strftime("%Y%m%d"))

    # Active orders placed strictly before as_of_ts
    orders = frames["fact_orders"].filter(
        (F.col("order_ts") < F.lit(as_of_ts)) & (~F.coalesce(F.col("is_deleted"), F.lit(False)))
    )

    # Order items belonging to active orders placed before as_of_ts
    items = (
        frames["fact_order_items"]
        .join(
            orders.select("order_key"),
            on="order_key",
            how="inner",
        )
        .filter(F.col("order_ts") < F.lit(as_of_ts))
    )

    validate_feature_currency(orders, "fact_orders")
    validate_feature_currency(items, "fact_order_items")

    item_aggs = items.groupBy("channel", "product_id").agg(
        F.countDistinct(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("order_key")))
        .cast(IntegerType())
        .alias("orders_7d"),
        F.countDistinct(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("order_key")))
        .cast(IntegerType())
        .alias("orders_30d"),
        F.coalesce(F.sum(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("qty"))), F.lit(0))
        .cast(IntegerType())
        .alias("units_sold_7d"),
        F.coalesce(F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("qty"))), F.lit(0))
        .cast(IntegerType())
        .alias("units_sold_30d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("gross_line_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("gross_revenue_7d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("gross_line_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("gross_revenue_30d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("net_line_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("net_revenue_7d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("net_line_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("net_revenue_30d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("discount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("discount_amount_30d"),
    )

    validate_feature_aggregates(item_aggs, "product items")

    # Returns within dual-timestamp boundary (order_ts < as_of_ts AND return_ts < as_of_ts)
    returns = (
        frames["fact_returns"]
        .join(
            orders.select("order_key", "order_ts"),
            on="order_key",
            how="inner",
        )
        .filter((F.col("order_ts") < F.lit(as_of_ts)) & (F.col("return_ts") < F.lit(as_of_ts)))
    )

    validate_feature_currency(returns, "fact_returns")

    return_aggs = returns.groupBy("channel", "product_key").agg(
        F.count(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("return_key")))
        .cast(IntegerType())
        .alias("return_count_7d"),
        F.count(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("return_key")))
        .cast(IntegerType())
        .alias("return_count_30d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_7d), F.col("refund_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("refund_amount_7d"),
        F.coalesce(
            F.sum(F.when(F.col("order_ts") >= F.lit(ts_30d), F.col("refund_amount"))),
            F.lit(0),
        )
        .cast("decimal(18,2)")
        .alias("refund_amount_30d"),
    )

    validate_feature_aggregates(return_aggs, "product returns")

    # Base products from dim_product
    dim_prod = frames["dim_product"].select(
        "product_key",
        "seller_key",
        "channel",
        "product_id",
        "seller_id",
    )
    dim_chan = frames["dim_channel"].select("channel_key", "channel").distinct()

    joined = (
        dim_prod.join(dim_chan, on="channel", how="left")
        .join(item_aggs, on=["channel", "product_id"], how="left")
        .join(return_aggs, on=["channel", "product_key"], how="left")
    )

    result = joined.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.lit(run_id),
                F.col("channel"),
                F.col("product_id"),
                F.lit(eval_date.isoformat()),
            ),
            256,
        ).alias("product_feature_key"),
        "product_key",
        "seller_key",
        "channel_key",
        F.lit(date_key).cast(IntegerType()).alias("as_of_date_key"),
        "channel",
        "product_id",
        "seller_id",
        F.lit(eval_date).cast(DateType()).alias("as_of_date"),
        F.lit(as_of_ts).cast(TimestampType()).alias("as_of_ts"),
        get_channel_currency(F.col("channel")).alias("currency"),
        F.coalesce(F.col("orders_7d"), F.lit(0)).alias("orders_7d"),
        F.coalesce(F.col("orders_30d"), F.lit(0)).alias("orders_30d"),
        F.coalesce(F.col("units_sold_7d"), F.lit(0)).alias("units_sold_7d"),
        F.coalesce(F.col("units_sold_30d"), F.lit(0)).alias("units_sold_30d"),
        F.coalesce(F.col("gross_revenue_7d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("gross_revenue_7d"),
        F.coalesce(F.col("gross_revenue_30d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("gross_revenue_30d"),
        F.coalesce(F.col("net_revenue_7d"), F.lit(0)).cast("decimal(18,2)").alias("net_revenue_7d"),
        F.coalesce(F.col("net_revenue_30d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("net_revenue_30d"),
        F.coalesce(F.col("discount_amount_30d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("discount_amount_30d"),
        F.coalesce(F.col("return_count_7d"), F.lit(0)).alias("return_count_7d"),
        F.coalesce(F.col("return_count_30d"), F.lit(0)).alias("return_count_30d"),
        F.coalesce(F.col("refund_amount_7d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("refund_amount_7d"),
        F.coalesce(F.col("refund_amount_30d"), F.lit(0))
        .cast("decimal(18,2)")
        .alias("refund_amount_30d"),
        F.when(
            F.coalesce(F.col("orders_30d"), F.lit(0)) > 0,
            (F.coalesce(F.col("return_count_30d"), F.lit(0)) / F.col("orders_30d")).cast(
                "decimal(18,4)"
            ),
        )
        .otherwise(F.lit(0).cast("decimal(18,4)"))
        .alias("return_rate_30d"),
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_gold_publication_id"),
        F.lit(POLICY).alias("_policy_version"),
    )

    return rehash_dp3(result, "feat_product_daily")


def build_features(
    spark: Any,
    frames: dict[str, Any],
    run_id: str,
    pub_id: str,
    as_of_dates: list[str | date],
) -> dict[str, Any]:
    """Build feat_customer_daily and feat_product_daily across multiple evaluation dates."""
    cust_frames = []
    prod_frames = []

    for dt in as_of_dates:
        cust_df = compute_customer_daily_features(spark, frames, run_id, pub_id, dt)
        prod_df = compute_product_daily_features(spark, frames, run_id, pub_id, dt)
        cust_frames.append(cust_df)
        prod_frames.append(prod_df)

    combined_cust = cust_frames[0]
    for df in cust_frames[1:]:
        combined_cust = combined_cust.unionByName(df)

    combined_prod = prod_frames[0]
    for df in prod_frames[1:]:
        combined_prod = combined_prod.unionByName(df)

    return {
        "feat_customer_daily": combined_cust,
        "feat_product_daily": combined_prod,
    }
