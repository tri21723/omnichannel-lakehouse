-- Initial schema only: future changes require explicit migrations.
CREATE SCHEMA IF NOT EXISTS walmart;
CREATE SCHEMA IF NOT EXISTS warehouse AUTHORIZATION spark_writer;

CREATE TABLE IF NOT EXISTS walmart.customers (
    customer_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS walmart.products (
    product_id TEXT PRIMARY KEY,
    seller_id TEXT NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    updated_ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS walmart.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES walmart.customers (customer_id),
    order_ts TIMESTAMPTZ NOT NULL,
    updated_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    currency CHAR(3) NOT NULL,
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0),
    shipping_country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS walmart.order_items (
    order_id TEXT NOT NULL REFERENCES walmart.orders (order_id),
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    product_id TEXT NOT NULL REFERENCES walmart.products (product_id),
    seller_id TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    unit_price NUMERIC(18, 2) NOT NULL CHECK (unit_price >= 0),
    discount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (discount >= 0),
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS walmart.returns (
    return_id TEXT NOT NULL,
    return_line_no INTEGER NOT NULL CHECK (return_line_no > 0),
    order_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    return_ts TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL,
    refund_amount NUMERIC(18, 2) NOT NULL CHECK (refund_amount >= 0),
    PRIMARY KEY (return_id, return_line_no),
    FOREIGN KEY (order_id, line_no) REFERENCES walmart.order_items (order_id, line_no)
);

CREATE INDEX IF NOT EXISTS orders_updated_ts_idx ON walmart.orders (updated_ts);
ALTER TABLE walmart.orders REPLICA IDENTITY FULL;
