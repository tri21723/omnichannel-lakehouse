"""Physical entity candidates, deterministic winner selection and schema definitions."""

KEYS = {
    "orders": ["channel", "order_id"],
    "order_items": ["channel", "order_id", "line_no"],
    "customers": ["channel", "customer_id"],
    "products": ["channel", "product_id"],
    "returns": ["channel", "return_id", "return_line_no"],
}
FIELDS = {
    "orders": (
        "channel string, order_id string, customer_id string, order_ts "
        "timestamp, updated_ts timestamp, status string, currency string, "
        "total_amount decimal(18,2), shipping_country string, is_deleted "
        "boolean, fulfillment_type string"
    ),
    "order_items": (
        "channel string, order_id string, line_no int, product_id string, "
        "seller_id string, qty int, unit_price decimal(18,2), discount "
        "decimal(18,2), updated_ts timestamp"
    ),
    "customers": (
        "channel string, customer_id string, name_hash string, email_hash "
        "string, country string, updated_ts timestamp, _pii_key_version string, "
        "first_order_ts timestamp"
    ),
    "products": (
        "channel string, product_id string, seller_id string, title string, "
        "category string, updated_ts timestamp, brand string"
    ),
    "returns": (
        "channel string, return_id string, return_line_no int, order_id string, "
        "line_no int, reason string, return_ts timestamp, refund_amount "
        "decimal(18,2), updated_ts timestamp"
    ),
}
AUDIT = (
    "_source_channel string, _dataset_run_id string, _input_id string, "
    "_source_table string, _source_snapshot long, _source_batch_id string, "
    "_source_record_hash string, _source_ordinal string, _parent_order_id "
    "string, _run_date date, _record_hash string, _policy_version string"
)


def rehash(df, entity):
    from pyspark.sql import functions as F

    names = sorted(
        name.split()[0] for name in FIELDS[entity].replace("decimal(18,2)", "decimal").split(", ")
    )
    if entity == "customers":
        names.remove("first_order_ts")
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


def winners(df, entity):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = [
        F.col("updated_ts").desc_nulls_last(),
        F.col("_record_hash").desc(),
        F.col("_source_record_hash").asc(),
        F.col("_source_ordinal").asc(),
        F.col("_input_id").asc(),
    ]
    return (
        df.withColumn(
            "_rank", F.row_number().over(Window.partitionBy(*KEYS[entity]).orderBy(*order))
        )
        .filter("_rank=1")
        .drop("_rank")
    )
