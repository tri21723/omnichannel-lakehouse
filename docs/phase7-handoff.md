# Phase 7 handoff: streaming with Kafka + Flink

Phase 6 is complete locally (`v0.6-query`). This page lists what Phase 7 can rely on and the
constraints it inherits. It does not implement Phase 7 ([plan §Phase 7](plan.md)).

## 1. What exists

- Batch chain Bronze → Silver → Gold → DP3 → warehouse, orchestrated by Airflow (`batch_daily`,
  ADR-0013) and queryable read-only through Trino (ADR-0015).
- Generator config already carries stream controls (`late_rate`, `late_max_minutes`,
  `burst_multiplier`; reserved, not used by batch). `generators/stream/`, `flink/jobs/amazon_late/`
  and `flink/jobs/tiktok_dedup/` are empty placeholders. `make gen-stream` and `make up-stream`
  still `exit 2`.
- Reserved names/ports (AGENTS.md): Kafka 9092, kafka-ui 8085, Flink JobManager 8082; topics
  `amazon.order-notifications`, `tiktok_shop.order-webhooks`; bucket `checkpoints` exists
  (Spark already has read/write on it, ADR-0004).

## 2. Contracts Phase 7 must keep

- **Bronze is the entry point.** Flink output (CSV/Parquet) lands in Bronze through the existing
  ingestion rules (ADR-0007): channel-scoped identity, audit columns `_ingest_ts`, `_source_file`,
  `_channel`, `_batch_id`, replay without duplicates, completion records. A new streaming source
  needs its own manifest/ground truth (`stream_manifest.json`) and an ADR for its slice identity.
- **Late and duplicate events are never dropped silently:** late events go to a side output that
  is persisted and reconciled; dedup state has a TTL documented next to the watermark settings.
- **Spark drivers are serialized** by `/runtime/locks/spark-driver.lock`; a DAG that ingests Flink
  output into Bronze must use the `spark_driver` pool and the existing task factory.
- **Trino stays read-only** and must not be used to write streaming results. If streaming tables
  should be queryable, they must live in an exposed Iceberg namespace and respect PII rules
  (raw payloads stay out of Trino like `iceberg.bronze`).
- **Datasets coexist:** a streaming dataset gets its own `_dataset_run_id`; never overwrite the
  three existing batch datasets.

## 3. Resources

The host has ~11.7 GiB for Docker. Measured: core ≈ 0.7 GiB, Spark workers ≈ 1 GiB each while
working, Airflow ≈ 1.2 GiB idle / 2.1 GiB with a driver, Trino ≈ 1.75 GiB. The plan budgets
Kafka + kafka-ui ≈ 1.5 GiB and Flink ≈ 3 GiB, so `stream` should run with core only (no Spark,
Airflow or Trino) for Flink tests, and Bronze ingestion of Flink output should run afterwards.
Every new service needs `mem_limit`, a healthcheck and the `stream` profile.

## 4. Suggested verification

- Ground truth from the generator (unique count, late count, duplicate count) per run.
- Kill the TaskManager during a run: no loss or duplication after checkpoint recovery.
- After ingestion into Bronze/Silver: Trino counts (read-only) equal the ground truth.
