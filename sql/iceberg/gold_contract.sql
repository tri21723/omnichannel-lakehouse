-- Gold Phase 4C logical schemas. Runtime validates named field types before writing.
CREATE TABLE IF NOT EXISTS lh.gold.dim_date (
    date_key INT,
    calendar_date DATE,
    year INT,
    quarter INT,
    month INT,
    day_of_month INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    _dataset_run_id STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.dim_channel (
    channel_key STRING,
    channel STRING,
    channel_name STRING,
    ingestion_mode STRING,
    _dataset_run_id STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.dim_customer (
    customer_key STRING,
    channel STRING,
    customer_id STRING,
    name_hash STRING,
    email_hash STRING,
    country STRING,
    first_order_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    _pii_key_version STRING,
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _silver_snapshot LONG,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.dim_product (
    product_key STRING,
    seller_key STRING,
    channel STRING,
    product_id STRING,
    seller_id STRING,
    title STRING,
    category STRING,
    brand STRING,
    updated_ts TIMESTAMP,
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _silver_snapshot LONG,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.dim_seller (
    seller_key STRING,
    channel STRING,
    seller_id STRING,
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.fact_orders (
    order_key STRING,
    order_date_key INT,
    channel_key STRING,
    customer_key STRING,
    channel STRING,
    order_id STRING,
    customer_id STRING,
    order_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    status STRING,
    currency STRING,
    gross_amount DECIMAL(18, 2),
    discount_amount DECIMAL(18, 2),
    net_amount DECIMAL(18, 2),
    item_count INT,
    total_qty INT,
    shipping_country STRING,
    is_deleted BOOLEAN,
    fulfillment_type STRING,
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _silver_snapshot LONG,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (order_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.fact_order_items (
    order_item_key STRING,
    order_key STRING,
    order_date_key INT,
    channel_key STRING,
    customer_key STRING,
    product_key STRING,
    seller_key STRING,
    channel STRING,
    order_id STRING,
    line_no INT,
    product_id STRING,
    seller_id STRING,
    order_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    status STRING,
    currency STRING,
    qty INT,
    unit_price DECIMAL(18, 2),
    discount DECIMAL(18, 2),
    gross_line_amount DECIMAL(18, 2),
    net_line_amount DECIMAL(18, 2),
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _silver_snapshot LONG,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (order_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.fact_returns (
    return_key STRING,
    order_key STRING,
    order_item_key STRING,
    order_date_key INT,
    return_date_key INT,
    channel_key STRING,
    customer_key STRING,
    product_key STRING,
    seller_key STRING,
    channel STRING,
    return_id STRING,
    return_line_no INT,
    order_id STRING,
    line_no INT,
    reason STRING,
    return_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    currency STRING,
    refund_amount DECIMAL(18, 2),
    _dataset_run_id STRING,
    _silver_publication_id STRING,
    _silver_snapshot LONG,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (return_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.feat_customer_daily (
    customer_feature_key STRING,
    customer_key STRING,
    channel_key STRING,
    as_of_date_key INT,
    channel STRING,
    customer_id STRING,
    as_of_date DATE,
    as_of_ts TIMESTAMP,
    currency STRING,
    last_order_ts TIMESTAMP,
    days_since_last_order INT,
    lifetime_order_count INT,
    lifetime_net_amount DECIMAL(18, 2),
    orders_7d INT,
    orders_30d INT,
    spend_7d DECIMAL(18, 2),
    spend_30d DECIMAL(18, 2),
    aov_30d DECIMAL(18, 2),
    returned_orders_30d INT,
    refund_amount_30d DECIMAL(18, 2),
    return_rate_30d DECIMAL(18, 4),
    _dataset_run_id STRING,
    _gold_publication_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (as_of_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.feat_product_daily (
    product_feature_key STRING,
    product_key STRING,
    seller_key STRING,
    channel_key STRING,
    as_of_date_key INT,
    channel STRING,
    product_id STRING,
    seller_id STRING,
    as_of_date DATE,
    as_of_ts TIMESTAMP,
    currency STRING,
    orders_7d INT,
    orders_30d INT,
    units_sold_7d INT,
    units_sold_30d INT,
    gross_revenue_7d DECIMAL(18, 2),
    gross_revenue_30d DECIMAL(18, 2),
    net_revenue_7d DECIMAL(18, 2),
    net_revenue_30d DECIMAL(18, 2),
    discount_amount_30d DECIMAL(18, 2),
    return_count_7d INT,
    return_count_30d INT,
    refund_amount_7d DECIMAL(18, 2),
    refund_amount_30d DECIMAL(18, 2),
    return_rate_30d DECIMAL(18, 4),
    _dataset_run_id STRING,
    _gold_publication_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (as_of_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.gold.lbl_order_return (
    order_label_key STRING,
    order_key STRING,
    customer_key STRING,
    channel_key STRING,
    order_date_key INT,
    as_of_date_key INT,
    channel STRING,
    order_id STRING,
    order_ts TIMESTAMP,
    as_of_date DATE,
    as_of_ts TIMESTAMP,
    horizon_days INT,
    currency STRING,
    net_amount DECIMAL(18, 2),
    is_mature BOOLEAN,
    is_returned INT,
    first_return_ts TIMESTAMP,
    days_to_return INT,
    return_line_count INT,
    refund_amount DECIMAL(18, 2),
    refund_ratio DECIMAL(18, 4),
    _dataset_run_id STRING,
    _gold_publication_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (as_of_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05
