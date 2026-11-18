-- Returned order lines per product category and currency (return rate = returned_lines / lines).
-- Money is never summed across currencies. Params: dataset_run_id. Parity-checked against Spark.
WITH items AS (
    SELECT
        p.category,
        i.currency,
        COUNT(*) AS lines
    FROM iceberg.gold.fact_order_items AS i
    INNER JOIN iceberg.gold.dim_product AS p
        ON
            i.product_key = p.product_key
            AND i._dataset_run_id = p._dataset_run_id
    WHERE i._dataset_run_id = :dataset_run_id
    GROUP BY p.category, i.currency
),

returned AS (
    SELECT
        p.category,
        r.currency,
        COUNT(*) AS returned_lines,
        SUM(r.refund_amount) AS refund_amount
    FROM iceberg.gold.fact_returns AS r
    INNER JOIN iceberg.gold.dim_product AS p
        ON
            r.product_key = p.product_key
            AND r._dataset_run_id = p._dataset_run_id
    WHERE r._dataset_run_id = :dataset_run_id
    GROUP BY p.category, r.currency
)

SELECT
    items.category,
    items.currency,
    items.lines,
    returned.refund_amount,
    COALESCE(returned.returned_lines, 0) AS returned_lines
FROM items
LEFT JOIN returned
    ON items.category = returned.category AND items.currency = returned.currency
ORDER BY items.category, items.currency
