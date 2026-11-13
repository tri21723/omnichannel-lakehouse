-- Silver Phase 4B logical schemas. Runtime validates named field types before writing.
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
    fulfillment_type STRING,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (order_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver._contrib_orders (
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
    fulfillment_type STRING,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.quarantine_orders (
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING,
    _error_reason ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (_run_date)
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
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver._contrib_order_items (
    channel STRING,
    order_id STRING,
    line_no INT,
    product_id STRING,
    seller_id STRING,
    qty INT,
    unit_price DECIMAL(18, 2),
    discount DECIMAL(18, 2),
    updated_ts TIMESTAMP,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.quarantine_order_items (
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING,
    _error_reason ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.customers (
    channel STRING,
    customer_id STRING,
    name_hash STRING,
    email_hash STRING,
    country STRING,
    updated_ts TIMESTAMP,
    _pii_key_version STRING,
    first_order_ts TIMESTAMP,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver._contrib_customers (
    channel STRING,
    customer_id STRING,
    name_hash STRING,
    email_hash STRING,
    country STRING,
    updated_ts TIMESTAMP,
    _pii_key_version STRING,
    first_order_ts TIMESTAMP,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.quarantine_customers (
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING,
    _error_reason ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.products (
    channel STRING,
    product_id STRING,
    seller_id STRING,
    title STRING,
    category STRING,
    updated_ts TIMESTAMP,
    brand STRING,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver._contrib_products (
    channel STRING,
    product_id STRING,
    seller_id STRING,
    title STRING,
    category STRING,
    updated_ts TIMESTAMP,
    brand STRING,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.quarantine_products (
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING,
    _error_reason ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.returns (
    channel STRING,
    return_id STRING,
    return_line_no INT,
    order_id STRING,
    line_no INT,
    reason STRING,
    return_ts TIMESTAMP,
    refund_amount DECIMAL(18, 2),
    updated_ts TIMESTAMP,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (DAYS (return_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver._contrib_returns (
    channel STRING,
    return_id STRING,
    return_line_no INT,
    order_id STRING,
    line_no INT,
    reason STRING,
    return_ts TIMESTAMP,
    refund_amount DECIMAL(18, 2),
    updated_ts TIMESTAMP,
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05

CREATE TABLE IF NOT EXISTS lh.silver.quarantine_returns (
    _source_channel STRING,
    _dataset_run_id STRING,
    _input_id STRING,
    _source_table STRING,
    _source_snapshot LONG,
    _source_batch_id STRING,
    _source_record_hash STRING,
    _source_ordinal STRING,
    _parent_order_id STRING,
    _run_date DATE,
    _record_hash STRING,
    _policy_version STRING,
    _error_reason ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (_run_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05
