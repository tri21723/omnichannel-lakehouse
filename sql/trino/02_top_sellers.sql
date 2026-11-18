-- Top 10 sellers by order lines with their channel totals (share = lines / channel_lines).
-- The default dataset puts ~50% of each channel on one hot seller.
-- Params: dataset_run_id. Parity-checked against Spark.
SELECT
    i.channel,
    i.seller_id,
    COUNT(*) AS lines,
    SUM(COUNT(*)) OVER (PARTITION BY i.channel) AS channel_lines,
    SUM(i.net_line_amount) AS net_amount
FROM iceberg.gold.fact_order_items AS i
WHERE i._dataset_run_id = :dataset_run_id
GROUP BY i.channel, i.seller_id
ORDER BY lines DESC, i.channel ASC, i.seller_id ASC
LIMIT 10
