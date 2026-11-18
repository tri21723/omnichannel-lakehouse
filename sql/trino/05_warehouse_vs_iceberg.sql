-- Federated reconciliation: order-line totals of the 30-day window in Iceberg Gold vs the
-- PostgreSQL warehouse mirror, per channel and currency (diffs must be 0.00).
-- Params: dataset_run_id, window_start, window_end (from warehouse.export_state).
WITH lake AS (
    SELECT
        i.channel,
        i.currency,
        COUNT(*) AS lines,
        SUM(i.net_line_amount) AS net_amount
    FROM iceberg.gold.fact_order_items AS i
    INNER JOIN iceberg.gold.fact_orders AS f
        ON i.order_key = f.order_key AND i._dataset_run_id = f._dataset_run_id
    WHERE
        i._dataset_run_id = :dataset_run_id
        AND COALESCE(f.is_deleted, FALSE) = FALSE
        AND i.order_ts >= TIMESTAMP :window_start
        AND i.order_ts < TIMESTAMP :window_end
    GROUP BY i.channel, i.currency
),

mirror AS (
    SELECT
        w.channel,
        w.currency,
        COUNT(*) AS lines,
        SUM(w.net_line_amount) AS net_amount
    FROM postgresql.warehouse.fact_order_items AS w
    WHERE w._dataset_run_id = :dataset_run_id
    GROUP BY w.channel, w.currency
)

SELECT
    lake.lines AS lake_lines,
    mirror.lines AS warehouse_lines,
    lake.net_amount AS lake_net_amount,
    mirror.net_amount AS warehouse_net_amount,
    COALESCE(lake.channel, mirror.channel) AS channel,
    COALESCE(lake.currency, mirror.currency) AS currency,
    COALESCE(lake.net_amount, 0) - COALESCE(mirror.net_amount, 0) AS net_diff
FROM lake
FULL OUTER JOIN mirror
    ON lake.channel = mirror.channel AND lake.currency = mirror.currency
ORDER BY channel, currency
