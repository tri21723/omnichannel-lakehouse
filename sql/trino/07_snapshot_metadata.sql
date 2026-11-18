-- Iceberg metadata: latest snapshots of Gold fact_orders and their operations.
-- No params. Some snapshots (e.g. the empty append at table creation) lack summary keys,
-- so ELEMENT_AT returns NULL where map subscript would fail.
SELECT
    s.committed_at,
    s.snapshot_id,
    s.operation,
    ELEMENT_AT(s.summary, 'added-records') AS added_records,
    ELEMENT_AT(s.summary, 'total-records') AS total_records
FROM iceberg.gold."fact_orders$snapshots" AS s -- noqa: RF05
ORDER BY s.committed_at DESC
LIMIT 10
