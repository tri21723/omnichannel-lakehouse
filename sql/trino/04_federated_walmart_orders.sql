-- Federated (Q1 + Q2): Gold Walmart orders (Iceberg) joined with the OLTP source orders
-- (PostgreSQL) by order_id. Only IDs, status and amounts; no PII columns are read.
-- Params: dataset_run_id. Parity-checked against Spark (JDBC read of walmart.orders).
SELECT
    COUNT(*) AS gold_orders,
    COUNT(o.order_id) AS matched_in_oltp,
    COUNT_IF(o.order_id IS NOT NULL AND f.status <> o.status) AS status_mismatches,
    SUM(f.gross_amount) AS gold_gross_amount,
    SUM(o.total_amount) AS oltp_total_amount
FROM iceberg.gold.fact_orders AS f
LEFT JOIN postgresql.walmart.orders AS o ON f.order_id = o.order_id
WHERE
    f._dataset_run_id = :dataset_run_id
    AND f.channel = 'walmart'
    AND COALESCE(f.is_deleted, FALSE) = FALSE
