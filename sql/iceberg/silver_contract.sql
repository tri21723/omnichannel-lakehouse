-- Phase 4B draft schema, not applied automatically. See ADR-0008.
CREATE TABLE IF NOT EXISTS lh.silver.orders (
    channel STRING,
    order_id STRING,
    customer_id STRING,
    order_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    status STRING,
    currency STRING,
    total_amount DECIMAL(18, 2),
    shipping_country STRING,
    is_deleted BOOLEAN,
    _dataset_run_id STRING,
    _input_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (order_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.order_items (
    channel STRING,
    order_id STRING,
    line_no INT,
    product_id STRING,
    seller_id STRING,
    qty INT,
    unit_price DECIMAL(18, 2),
    discount DECIMAL(18, 2),
    updated_ts TIMESTAMP,
    _dataset_run_id STRING,
    _input_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.customers (
    channel STRING,
    customer_id STRING,
    name_hash STRING,
    email_hash STRING,
    country STRING,
    first_order_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    _pii_key_version STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.products (
    channel STRING,
    product_id STRING,
    title STRING,
    category STRING,
    brand STRING,
    seller_id STRING,
    updated_ts TIMESTAMP,
    _dataset_run_id STRING,
    _input_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.returns (
    channel STRING,
    return_id STRING,
    return_line_no INT,
    order_id STRING,
    line_no INT,
    return_ts TIMESTAMP,
    reason STRING,
    refund_amount DECIMAL(18, 2),
    updated_ts TIMESTAMP,
    _dataset_run_id STRING,
    _input_id STRING,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (return_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05
