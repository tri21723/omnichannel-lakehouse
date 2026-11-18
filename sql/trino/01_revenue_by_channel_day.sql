-- Net revenue per channel, currency and UTC order date for one dataset run (Gold).
-- Params: dataset_run_id. Parity-checked against Spark (make verify-query).
SELECT
    f.channel,
    f.currency,
    CAST(f.order_ts AS DATE) AS order_date,
    COUNT(*) AS orders,
    SUM(f.net_amount) AS net_amount
FROM iceberg.gold.fact_orders AS f
WHERE
    f._dataset_run_id = :dataset_run_id
    AND COALESCE(f.is_deleted, FALSE) = FALSE
GROUP BY f.channel, f.currency, CAST(f.order_ts AS DATE)
ORDER BY f.channel, f.currency, order_date
