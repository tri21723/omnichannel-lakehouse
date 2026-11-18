-- Return-label maturity at one evaluation date: immature labels must have is_returned NULL.
-- Money stays per currency. Params: dataset_run_id, as_of_date. Parity-checked against Spark.
SELECT
    l.currency,
    l.is_mature,
    l.is_returned,
    COUNT(*) AS labels,
    SUM(l.net_amount) AS net_amount
FROM iceberg.gold.lbl_order_return AS l
WHERE
    l._dataset_run_id = :dataset_run_id
    AND l.as_of_date = DATE :as_of_date
GROUP BY l.currency, l.is_mature, l.is_returned
ORDER BY l.currency, l.is_mature, l.is_returned
