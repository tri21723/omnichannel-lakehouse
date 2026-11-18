-- Iceberg metadata: latest snapshots of Gold fact_orders and their operations.
-- No params.
SELECT
    s.committed_at,
    s.snapshot_id,
    s.operation,
    s.summary['added-records'] AS added_records,
    s.summary['total-records'] AS total_records
FROM iceberg.gold."fact_orders$snapshots" AS s -- noqa: RF05
ORDER BY s.committed_at DESC
LIMIT 10
