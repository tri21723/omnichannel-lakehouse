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
