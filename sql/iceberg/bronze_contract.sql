-- Draft Phase 4 contract only; not applied by Phase 3 smoke.
CREATE TABLE IF NOT EXISTS lh.bronze.walmart_orders_raw (
    _raw STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING,
    _source_table STRING,
    _channel STRING,
    _dataset_run_id STRING,
    _batch_id STRING,
    _run_date DATE
) USING ICEBERG
PARTITIONED BY (_run_date)
-- Property keys are quoted Iceberg options, not identifiers.
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd'); -- noqa: RF05
