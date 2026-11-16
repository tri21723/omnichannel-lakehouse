"""Build Gold dimensions and facts from scoped Silver entity DataFrames."""

from spark.lib.gold_entities import (
    CHANNELS,
    POLICY,
    channel_key,
    rehash_gold,
)


def validate_fact_numbers(df, name):
    """Reject decimal overflow and invalid aggregates even when ANSI is disabled."""
    from pyspark.sql import functions as F

    fields = {
        "fact_order_items": [
            "qty",
            "unit_price",
            "discount",
            "gross_line_amount",
            "net_line_amount",
        ],
        "fact_orders": ["item_count", "total_qty", "gross_amount", "discount_amount", "net_amount"],
        "fact_returns": ["refund_amount"],
    }[name]
    invalid = F.lit(False)
    for field in fields:
        invalid = invalid | F.col(field).isNull() | (F.col(field) < 0)
    if df.filter(invalid).limit(1).count():
        raise ValueError(f"Gold numeric overflow or invalid amount in {name}")


def build_dimensions(spark, silver_frames, run_id, pub_id, snapshots):
    """Build the five Gold dimensions."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    # 1. dim_channel
    channel_rows = [
        (
            channel_key(ch),
            ch,
            display_name,
            mode,
            run_id,
        )
        for ch, (display_name, mode) in CHANNELS.items()
    ]
    channel_schema = StructType(
        [
            StructField("channel_key", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("channel_name", StringType(), False),
            StructField("ingestion_mode", StringType(), False),
            StructField("_dataset_run_id", StringType(), False),
        ]
    )
    dim_channel = spark.createDataFrame(channel_rows, channel_schema)

    # 2. dim_date: collect all distinct dates across order_ts, first_order_ts, return_ts
    order_dates = (
        silver_frames["orders"]
        .select(F.to_date("order_ts").alias("dt"))
        .filter(F.col("dt").isNotNull())
    )
    cust_dates = (
        silver_frames["customers"]
        .select(F.to_date("first_order_ts").alias("dt"))
        .filter(F.col("dt").isNotNull())
    )
    return_dates = (
        silver_frames["returns"]
        .select(F.to_date("return_ts").alias("dt"))
        .filter(F.col("dt").isNotNull())
    )

    all_dates = order_dates.union(cust_dates).union(return_dates).distinct()
    dim_date = all_dates.select(
        F.date_format("dt", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        F.col("dt").cast(DateType()).alias("calendar_date"),
        F.year("dt").cast(IntegerType()).alias("year"),
        F.quarter("dt").cast(IntegerType()).alias("quarter"),
        F.month("dt").cast(IntegerType()).alias("month"),
        F.dayofmonth("dt").cast(IntegerType()).alias("day_of_month"),
        # ISO day of week: 1=Mon .. 7=Sun. In Spark dayofweek: 1=Sun, 2=Mon .. 7=Sat
        ((F.dayofweek("dt") + 5) % 7 + 1).cast(IntegerType()).alias("day_of_week"),
        F.dayofweek("dt").isin(1, 7).cast(BooleanType()).alias("is_weekend"),
        F.lit(run_id).alias("_dataset_run_id"),
    ).sort("date_key")

    # 3. dim_customer
    cust_snap = snapshots.get("customers", 0)
    dim_customer = silver_frames["customers"].select(
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("customer_id")),
            256,
        ).alias("customer_key"),
        "channel",
        "customer_id",
        "name_hash",
        "email_hash",
        "country",
        "first_order_ts",
        "updated_ts",
        "_pii_key_version",
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(cust_snap).alias("_silver_snapshot"),
        F.lit(POLICY).alias("_policy_version"),
    )
    dim_customer = rehash_gold(dim_customer, "dim_customer")

    # 4. dim_seller (ID-only derived from products and order_items)
    prod_sellers = (
        silver_frames["products"]
        .select("channel", "seller_id")
        .filter(F.col("seller_id").isNotNull())
    )
    item_sellers = (
        silver_frames["order_items"]
        .select("channel", "seller_id")
        .filter(F.col("seller_id").isNotNull())
    )
    all_sellers = prod_sellers.union(item_sellers).distinct()

    dim_seller = all_sellers.select(
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("seller_id")),
            256,
        ).alias("seller_key"),
        "channel",
        "seller_id",
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(POLICY).alias("_policy_version"),
    )
    dim_seller = rehash_gold(dim_seller, "dim_seller")

    # 5. dim_product
    prod_snap = snapshots.get("products", 0)
    dim_product = silver_frames["products"].select(
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("product_id")),
            256,
        ).alias("product_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("seller_id")),
            256,
        ).alias("seller_key"),
        "channel",
        "product_id",
        "seller_id",
        "title",
        "category",
        "brand",
        "updated_ts",
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(prod_snap).alias("_silver_snapshot"),
        F.lit(POLICY).alias("_policy_version"),
    )
    dim_product = rehash_gold(dim_product, "dim_product")

    return {
        "dim_date": dim_date,
        "dim_channel": dim_channel,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
    }


def build_facts(spark, silver_frames, dims, run_id, pub_id, snapshots):
    """Build the three Gold fact tables with exact DECIMAL reconciliation."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    order_snap = snapshots.get("orders", 0)
    item_snap = snapshots.get("order_items", 0)
    ret_snap = snapshots.get("returns", 0)

    # 1. Prepare fact_order_items
    items = silver_frames["order_items"]
    orders = silver_frames["orders"]

    # Join items with orders to get order-level timestamps and status
    order_context = orders.select(
        "channel",
        "order_id",
        "customer_id",
        "order_ts",
        F.col("updated_ts").alias("_order_updated_ts"),
        F.col("status").alias("_order_status"),
        F.col("currency").alias("_order_currency"),
    )

    items_with_order = items.join(order_context, ["channel", "order_id"], "inner")

    fact_order_items = items_with_order.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.lit(run_id),
                F.col("channel"),
                F.col("order_id"),
                F.col("line_no").cast("string"),
            ),
            256,
        ).alias("order_item_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("order_id")),
            256,
        ).alias("order_key"),
        F.date_format("order_ts", "yyyyMMdd").cast(IntegerType()).alias("order_date_key"),
        F.sha2(
            F.concat_ws(":", F.lit("channel"), F.col("channel")),
            256,
        ).alias("channel_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("customer_id")),
            256,
        ).alias("customer_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("product_id")),
            256,
        ).alias("product_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("seller_id")),
            256,
        ).alias("seller_key"),
        "channel",
        "order_id",
        "line_no",
        "product_id",
        "seller_id",
        "order_ts",
        F.coalesce(items["updated_ts"], F.col("_order_updated_ts")).alias("updated_ts"),
        F.col("_order_status").alias("status"),
        F.col("_order_currency").alias("currency"),
        "qty",
        "unit_price",
        "discount",
        (F.col("qty") * F.col("unit_price")).cast("decimal(18,2)").alias("gross_line_amount"),
        (F.col("qty") * F.col("unit_price") - F.col("discount"))
        .cast("decimal(18,2)")
        .alias("net_line_amount"),
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(item_snap).alias("_silver_snapshot"),
        F.lit(POLICY).alias("_policy_version"),
    )
    validate_fact_numbers(fact_order_items, "fact_order_items")
    fact_order_items = rehash_gold(fact_order_items, "fact_order_items")

    # 2. Prepare fact_orders
    # Aggregate items to order level
    line_aggs = fact_order_items.groupBy("channel", "order_id").agg(
        F.count("*").cast(IntegerType()).alias("item_count"),
        F.sum("qty").cast(IntegerType()).alias("total_qty"),
        F.sum("gross_line_amount").cast("decimal(18,2)").alias("gross_amount"),
        F.sum("discount").cast("decimal(18,2)").alias("discount_amount"),
        F.sum("net_line_amount").cast("decimal(18,2)").alias("net_amount"),
    )

    orders_joined = orders.join(line_aggs, ["channel", "order_id"], "inner")

    # Validate net_amount equals Silver orders total_amount
    mismatch = orders_joined.filter(F.col("net_amount") != F.col("total_amount"))
    if mismatch.limit(1).count() > 0:
        bad = mismatch.select("channel", "order_id", "total_amount", "net_amount").first()
        raise ValueError(f"Gold order total reconciliation failed: {bad.asDict()}")

    fact_orders = orders_joined.select(
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("order_id")),
            256,
        ).alias("order_key"),
        F.date_format("order_ts", "yyyyMMdd").cast(IntegerType()).alias("order_date_key"),
        F.sha2(
            F.concat_ws(":", F.lit("channel"), F.col("channel")),
            256,
        ).alias("channel_key"),
        F.sha2(
            F.concat_ws(":", F.lit(run_id), F.col("channel"), F.col("customer_id")),
            256,
        ).alias("customer_key"),
        "channel",
        "order_id",
        "customer_id",
        "order_ts",
        "updated_ts",
        "status",
        "currency",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "item_count",
        "total_qty",
        "shipping_country",
        "is_deleted",
        "fulfillment_type",
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(order_snap).alias("_silver_snapshot"),
        F.lit(POLICY).alias("_policy_version"),
    )
    validate_fact_numbers(fact_orders, "fact_orders")
    fact_orders = rehash_gold(fact_orders, "fact_orders")

    # 3. Prepare fact_returns
    # Join returns with fact_order_items to inherit all dimensional keys
    item_context = fact_order_items.select(
        "channel",
        "order_id",
        "line_no",
        "order_key",
        "order_item_key",
        "order_date_key",
        "channel_key",
        "customer_key",
        "product_key",
        "seller_key",
        "order_ts",
        "net_line_amount",
        "currency",
    )

    returns = silver_frames["returns"]
    ret_joined = returns.join(
        item_context,
        ["channel", "order_id", "line_no"],
        "inner",
    )

    # Validate return_ts >= order_ts
    early_returns = ret_joined.filter(F.col("return_ts") < F.col("order_ts"))
    if early_returns.limit(1).count() > 0:
        bad_ret = early_returns.select("channel", "return_id", "return_ts", "order_ts").first()
        raise ValueError(f"Return predates order: {bad_ret.asDict()}")

    # Validate refund_amount cumulative <= net_line_amount
    line_refunds = ret_joined.groupBy("channel", "order_id", "line_no").agg(
        F.sum("refund_amount").alias("_cum_refund")
    )
    excessive_refunds = line_refunds.join(
        item_context.select("channel", "order_id", "line_no", "net_line_amount").distinct(),
        ["channel", "order_id", "line_no"],
    ).filter(F.col("_cum_refund") > F.col("net_line_amount"))
    if excessive_refunds.limit(1).count() > 0:
        bad_ref = excessive_refunds.first()
        raise ValueError(f"Refund exceeds line net amount: {bad_ref.asDict()}")

    fact_returns = ret_joined.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.lit(run_id),
                F.col("channel"),
                F.col("return_id"),
                F.col("return_line_no").cast("string"),
            ),
            256,
        ).alias("return_key"),
        "order_key",
        "order_item_key",
        "order_date_key",
        F.date_format("return_ts", "yyyyMMdd").cast(IntegerType()).alias("return_date_key"),
        "channel_key",
        "customer_key",
        "product_key",
        "seller_key",
        "channel",
        "return_id",
        "return_line_no",
        "order_id",
        "line_no",
        "reason",
        "return_ts",
        "updated_ts",
        "currency",
        F.col("refund_amount").cast("decimal(18,2)").alias("refund_amount"),
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_silver_publication_id"),
        F.lit(ret_snap).alias("_silver_snapshot"),
        F.lit(POLICY).alias("_policy_version"),
    )
    validate_fact_numbers(fact_returns, "fact_returns")
    fact_returns = rehash_gold(fact_returns, "fact_returns")

    return {
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "fact_returns": fact_returns,
    }
