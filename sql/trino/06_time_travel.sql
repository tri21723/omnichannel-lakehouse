-- Time travel: the same dataset at a pinned snapshot (from the Gold ledger) and as of a
-- timestamp. Params: dataset_run_id, fact_orders_snapshot, as_of_ts.
SELECT
    'version' AS mode,
    COUNT(*) AS orders
FROM iceberg.gold.fact_orders FOR VERSION AS OF :fact_orders_snapshot -- noqa: PRS
WHERE _dataset_run_id = :dataset_run_id
UNION ALL -- noqa: PRS
SELECT
    'timestamp' AS mode,
    COUNT(*) AS orders
FROM iceberg.gold.fact_orders FOR TIMESTAMP AS OF TIMESTAMP :as_of_ts
WHERE _dataset_run_id = :dataset_run_id
