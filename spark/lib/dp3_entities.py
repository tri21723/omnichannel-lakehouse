"""Schema definitions, surrogate keys and deterministic hashing for Phase 4D."""

from typing import Any

POLICY = "gold-policy-v1"
LABEL_POLICY = "return-label-policy-v2"

DP3_KEYS = {
    "feat_customer_daily": ["customer_feature_key"],
    "feat_product_daily": ["product_feature_key"],
    "lbl_order_return": ["order_label_key"],
}

DP3_FIELDS = {
    "feat_customer_daily": (
        "customer_feature_key string, customer_key string, channel_key string, "
        "as_of_date_key int, channel string, customer_id string, as_of_date date, "
        "as_of_ts timestamp, currency string, last_order_ts timestamp, "
        "days_since_last_order int, lifetime_order_count int, "
        "lifetime_net_amount decimal(18,2), orders_7d int, orders_30d int, "
        "spend_7d decimal(18,2), spend_30d decimal(18,2), aov_30d decimal(18,2), "
        "returned_orders_30d int, refund_amount_30d decimal(18,2), "
        "return_rate_30d decimal(18,4)"
    ),
    "feat_product_daily": (
        "product_feature_key string, product_key string, seller_key string, "
        "channel_key string, as_of_date_key int, channel string, product_id string, "
        "seller_id string, as_of_date date, as_of_ts timestamp, currency string, "
        "orders_7d int, orders_30d int, units_sold_7d int, units_sold_30d int, "
        "gross_revenue_7d decimal(18,2), gross_revenue_30d decimal(18,2), "
        "net_revenue_7d decimal(18,2), net_revenue_30d decimal(18,2), "
        "discount_amount_30d decimal(18,2), return_count_7d int, "
        "return_count_30d int, refund_amount_7d decimal(18,2), "
        "refund_amount_30d decimal(18,2), return_rate_30d decimal(18,4)"
    ),
    "lbl_order_return": (
        "order_label_key string, order_key string, customer_key string, "
        "channel_key string, order_date_key int, as_of_date_key int, "
        "channel string, order_id string, order_ts timestamp, as_of_date date, "
        "as_of_ts timestamp, horizon_days int, currency string, "
        "net_amount decimal(18,2), is_mature boolean, is_returned int, "
        "first_return_ts timestamp, days_to_return int, return_line_count int, "
        "refund_amount decimal(18,2), refund_ratio decimal(18,4)"
    ),
}

DP3_AUDIT = (
    "_dataset_run_id string, _gold_publication_id string, "
    "_record_hash string, _policy_version string"
)


def get_channel_currency(channel_col: Any) -> Any:
    """Return currency expression for a given channel column."""
    from pyspark.sql import functions as F

    return (
        F.when(channel_col.isin("shopee", "lazada", "pos"), F.lit("VND"))
        .when(channel_col.isin("walmart", "amazon", "tiktok_shop"), F.lit("USD"))
        .otherwise(F.lit("USD"))
    )


def rehash_dp3(df: Any, table_name: str) -> Any:
    """Compute deterministic SHA-256 _record_hash over non-audit fields."""
    from pyspark.sql import functions as F

    fields_str = DP3_FIELDS[table_name]
    clean_str = fields_str.replace("decimal(18,2)", "decimal").replace("decimal(18,4)", "decimal")
    names = sorted(name.split()[0] for name in clean_str.split(", "))
    return df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(*names),
                {
                    "ignoreNullFields": "false",
                    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
                },
            ),
            256,
        ),
    )
