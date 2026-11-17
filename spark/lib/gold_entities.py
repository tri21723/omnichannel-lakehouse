"""Gold schema definitions, surrogate keys and deterministic hashing."""

POLICY = "gold-policy-v2"

CHANNELS = {
    "walmart": ("Walmart", "batch_jdbc"),
    "amazon": ("Amazon", "batch_jsonl"),
    "tiktok_shop": ("TikTok Shop", "batch_jsonl"),
    "shopee": ("Shopee", "batch_jsonl"),
    "lazada": ("Lazada", "batch_jsonl"),
}

GOLD_KEYS = {
    "dim_date": ["date_key"],
    "dim_channel": ["channel_key"],
    "dim_customer": ["customer_key"],
    "dim_product": ["product_key"],
    "dim_seller": ["seller_key"],
    "fact_orders": ["order_key"],
    "fact_order_items": ["order_item_key"],
    "fact_returns": ["return_key"],
}

GOLD_FIELDS = {
    "dim_date": (
        "date_key int, calendar_date date, year int, quarter int, month int, "
        "day_of_month int, day_of_week int, is_weekend boolean, _dataset_run_id string"
    ),
    "dim_channel": (
        "channel_key string, channel string, channel_name string, "
        "ingestion_mode string, _dataset_run_id string"
    ),
    "dim_customer": (
        "customer_key string, channel string, customer_id string, name_hash string, "
        "email_hash string, country string, first_order_ts timestamp, "
        "updated_ts timestamp, _pii_key_version string"
    ),
    "dim_product": (
        "product_key string, seller_key string, channel string, product_id string, "
        "seller_id string, title string, category string, brand string, updated_ts timestamp"
    ),
    "dim_seller": ("seller_key string, channel string, seller_id string"),
    "fact_orders": (
        "order_key string, order_date_key int, channel_key string, customer_key string, "
        "channel string, order_id string, customer_id string, order_ts timestamp, "
        "updated_ts timestamp, status string, currency string, gross_amount decimal(18,2), "
        "discount_amount decimal(18,2), net_amount decimal(18,2), item_count int, "
        "total_qty int, shipping_country string, is_deleted boolean, fulfillment_type string"
    ),
    "fact_order_items": (
        "order_item_key string, order_key string, order_date_key int, channel_key string, "
        "customer_key string, product_key string, seller_key string, channel string, "
        "order_id string, line_no int, product_id string, seller_id string, "
        "order_ts timestamp, updated_ts timestamp, status string, currency string, "
        "qty int, unit_price decimal(18,2), discount decimal(18,2), "
        "gross_line_amount decimal(18,2), net_line_amount decimal(18,2)"
    ),
    "fact_returns": (
        "return_key string, order_key string, order_item_key string, order_date_key int, "
        "return_date_key int, channel_key string, customer_key string, product_key string, "
        "seller_key string, channel string, return_id string, return_line_no int, "
        "order_id string, line_no int, reason string, return_ts timestamp, "
        "updated_ts timestamp, currency string, refund_amount decimal(18,2)"
    ),
}

GOLD_AUDIT = (
    "_dataset_run_id string, _silver_publication_id string, _silver_snapshot long, "
    "_record_hash string, _policy_version string"
)

# dim_seller does not carry _silver_snapshot since it combines multiple entities
SELLER_AUDIT = (
    "_dataset_run_id string, _silver_publication_id string, "
    "_record_hash string, _policy_version string"
)


def channel_key(channel_str):
    import hashlib

    return hashlib.sha256(f"channel:{channel_str}".encode()).hexdigest()


def customer_key(run_id, channel, customer_id):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{customer_id}".encode()).hexdigest()


def product_key(run_id, channel, product_id):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{product_id}".encode()).hexdigest()


def seller_key(run_id, channel, seller_id):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{seller_id}".encode()).hexdigest()


def order_key(run_id, channel, order_id):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{order_id}".encode()).hexdigest()


def order_item_key(run_id, channel, order_id, line_no):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{order_id}:{line_no}".encode()).hexdigest()


def return_key(run_id, channel, return_id, return_line_no):
    import hashlib

    return hashlib.sha256(f"{run_id}:{channel}:{return_id}:{return_line_no}".encode()).hexdigest()


def date_key_int(dt):
    """Format a date or datetime object as YYYYMMDD integer."""
    return int(dt.strftime("%Y%m%d"))


def rehash_gold(df, table_name):
    from pyspark.sql import functions as F

    fields_str = GOLD_FIELDS[table_name]
    names = sorted(
        name.split()[0] for name in fields_str.replace("decimal(18,2)", "decimal").split(", ")
    )
    return df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(*names),
                {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
            ),
            256,
        ),
    )
