-- Phase 4E downstream data warehouse physical DDL (owner: spark_writer, read-only: trino_ro).
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    _dataset_run_id TEXT NOT NULL,
    date_key INTEGER NOT NULL,
    calendar_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    PRIMARY KEY (_dataset_run_id, date_key)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_channel (
    _dataset_run_id TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    channel_name TEXT NOT NULL,
    ingestion_mode TEXT NOT NULL,
    PRIMARY KEY (_dataset_run_id, channel_key)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    name_hash TEXT NOT NULL,
    email_hash TEXT NOT NULL,
    country TEXT NOT NULL,
    first_order_ts TIMESTAMPTZ NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL,
    _pii_key_version TEXT NOT NULL,
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _silver_snapshot BIGINT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
    seller_key TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    seller_id TEXT NOT NULL,
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key TEXT PRIMARY KEY,
    seller_key TEXT NOT NULL REFERENCES warehouse.dim_seller (seller_key),
    channel TEXT NOT NULL,
    product_id TEXT NOT NULL,
    seller_id TEXT NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    updated_ts TIMESTAMPTZ NOT NULL,
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _silver_snapshot BIGINT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key TEXT PRIMARY KEY,
    order_date_key INTEGER NOT NULL,
    channel_key TEXT NOT NULL,
    customer_key TEXT NOT NULL REFERENCES warehouse.dim_customer (customer_key),
    channel TEXT NOT NULL,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    order_ts TIMESTAMPTZ NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    currency TEXT NOT NULL,
    gross_amount NUMERIC(18, 2) NOT NULL CHECK (gross_amount >= 0),
    discount_amount NUMERIC(18, 2) NOT NULL CHECK (discount_amount >= 0),
    net_amount NUMERIC(18, 2) NOT NULL CHECK (net_amount >= 0),
    item_count INTEGER NOT NULL CHECK (item_count >= 0),
    total_qty INTEGER NOT NULL CHECK (total_qty >= 0),
    shipping_country TEXT NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    fulfillment_type TEXT,
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _silver_snapshot BIGINT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL,
    FOREIGN KEY (_dataset_run_id, order_date_key)
    REFERENCES warehouse.dim_date (_dataset_run_id, date_key),
    FOREIGN KEY (_dataset_run_id, channel_key)
    REFERENCES warehouse.dim_channel (_dataset_run_id, channel_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_key TEXT PRIMARY KEY,
    order_key TEXT NOT NULL REFERENCES warehouse.fact_orders (order_key),
    order_date_key INTEGER NOT NULL,
    channel_key TEXT NOT NULL,
    customer_key TEXT NOT NULL REFERENCES warehouse.dim_customer (customer_key),
    product_key TEXT NOT NULL REFERENCES warehouse.dim_product (product_key),
    seller_key TEXT NOT NULL REFERENCES warehouse.dim_seller (seller_key),
    channel TEXT NOT NULL,
    order_id TEXT NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    product_id TEXT NOT NULL,
    seller_id TEXT NOT NULL,
    order_ts TIMESTAMPTZ NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    currency TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    unit_price NUMERIC(18, 2) NOT NULL CHECK (unit_price >= 0),
    discount NUMERIC(18, 2) NOT NULL CHECK (discount >= 0),
    gross_line_amount NUMERIC(18, 2) NOT NULL CHECK (gross_line_amount >= 0),
    net_line_amount NUMERIC(18, 2) NOT NULL CHECK (net_line_amount >= 0),
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _silver_snapshot BIGINT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL,
    FOREIGN KEY (_dataset_run_id, order_date_key)
    REFERENCES warehouse.dim_date (_dataset_run_id, date_key),
    FOREIGN KEY (_dataset_run_id, channel_key)
    REFERENCES warehouse.dim_channel (_dataset_run_id, channel_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_returns (
    return_key TEXT PRIMARY KEY,
    order_key TEXT NOT NULL REFERENCES warehouse.fact_orders (order_key),
    order_item_key TEXT NOT NULL REFERENCES warehouse.fact_order_items (order_item_key),
    order_date_key INTEGER NOT NULL,
    return_date_key INTEGER NOT NULL,
    channel_key TEXT NOT NULL,
    customer_key TEXT NOT NULL REFERENCES warehouse.dim_customer (customer_key),
    product_key TEXT NOT NULL REFERENCES warehouse.dim_product (product_key),
    seller_key TEXT NOT NULL REFERENCES warehouse.dim_seller (seller_key),
    channel TEXT NOT NULL,
    return_id TEXT NOT NULL,
    return_line_no INTEGER NOT NULL CHECK (return_line_no > 0),
    order_id TEXT NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    reason TEXT NOT NULL,
    return_ts TIMESTAMPTZ NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL,
    currency TEXT NOT NULL,
    refund_amount NUMERIC(18, 2) NOT NULL CHECK (refund_amount >= 0),
    _dataset_run_id TEXT NOT NULL,
    _silver_publication_id TEXT NOT NULL,
    _silver_snapshot BIGINT NOT NULL,
    _record_hash TEXT NOT NULL,
    _policy_version TEXT NOT NULL,
    FOREIGN KEY (_dataset_run_id, order_date_key)
    REFERENCES warehouse.dim_date (_dataset_run_id, date_key),
    FOREIGN KEY (_dataset_run_id, return_date_key)
    REFERENCES warehouse.dim_date (_dataset_run_id, date_key),
    FOREIGN KEY (_dataset_run_id, channel_key)
    REFERENCES warehouse.dim_channel (_dataset_run_id, channel_key)
);

CREATE INDEX IF NOT EXISTS warehouse_dim_customer_run_idx
ON warehouse.dim_customer (_dataset_run_id);

CREATE INDEX IF NOT EXISTS warehouse_dim_seller_run_idx
ON warehouse.dim_seller (_dataset_run_id);

CREATE INDEX IF NOT EXISTS warehouse_dim_product_run_idx
ON warehouse.dim_product (_dataset_run_id);

CREATE INDEX IF NOT EXISTS warehouse_fact_orders_run_ts_idx
ON warehouse.fact_orders (_dataset_run_id, order_ts);

CREATE INDEX IF NOT EXISTS warehouse_fact_orders_channel_currency_idx
ON warehouse.fact_orders (channel, currency);

CREATE INDEX IF NOT EXISTS warehouse_fact_order_items_run_ts_idx
ON warehouse.fact_order_items (_dataset_run_id, order_ts);

CREATE INDEX IF NOT EXISTS warehouse_fact_order_items_channel_currency_idx
ON warehouse.fact_order_items (channel, currency);

CREATE INDEX IF NOT EXISTS warehouse_fact_returns_run_ts_idx
ON warehouse.fact_returns (_dataset_run_id, return_ts);

CREATE INDEX IF NOT EXISTS warehouse_fact_returns_channel_currency_idx
ON warehouse.fact_returns (channel, currency);
