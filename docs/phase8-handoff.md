# Phase 8 handoff: CDC with Debezium

Phase 7 is complete locally (`v0.7-stream`). This page lists what Phase 8 can rely on and the
constraints it inherits. It does not implement Phase 8 ([plan §Phase 8](plan.md)).

## 1. What exists

- **Kafka 4.3.2 (KRaft)** in profile `stream` (ADR-0016): container listener `kafka:29092`, host
  `127.0.0.1:9092`, automatic topic creation **off** (topics come from the idempotent
  `docker/stream/kafka-init.sh`), transactions enabled for a single broker (replication 1,
  min ISR 1). kafka-ui on `127.0.0.1:8085` is read-only.
- **PostgreSQL** already runs with `wal_level=logical`, `max_replication_slots=10`,
  `max_wal_senders=10`, `max_slot_wal_keep_size=5GB` (core compose), and role `debezium_repl`
  (`REPLICATION`, password `DEBEZIUM_REPL_DB_PASSWORD` in `.env`) can read `walmart.orders`
  (`make smoke-core`). No publication, slot or `REPLICA IDENTITY FULL` exists yet.
- **Bronze `--source stream`** shows the pattern for non-file sources: an immutable manifest per
  closed unit, `select_*` validation, the shared `stage()`/`replace_slice()` and completion records
  (ADR-0007 addendum). The audit schema was kept unchanged (branch in `_source_table`).
- **Exactly-once lessons** (streaming validation): sinks must commit with the checkpoint;
  watermarks are not checkpointed; a session cluster without HA loses jobs on JobManager restart;
  `docker kill` does not trigger the restart policy; Maven Central rate-limits CI (retries are
  configured in `Dockerfile.flink`).

## 2. Contracts Phase 8 must keep

- Topic name `platform_cdc.walmart.orders` (AGENTS.md); create it (or let Debezium create it) in
  an explicit, idempotent step — do not turn on broker-wide auto creation.
- `REPLICA IDENTITY FULL` for CDC tables, `heartbeat.interval.ms`, and the existing
  `max_slot_wal_keep_size` so an idle or stopped connector cannot fill the disk; monitor
  `pg_replication_slots` / `pg_wal_lsn_diff`.
- Ordering by `lsn`/`ts_ms`; deletes as soft delete or SCD2, documented in `docs/data-model.md`.
- Walmart batch ingestion (JSONL + JDBC snapshot evidence, ADR-0003/0007) and the three batch
  datasets must not change; CDC data needs its own dataset/slice identity and Bronze table
  (`bronze.walmart_orders_cdc` in the plan). PII (`walmart.customers`) stays out of Trino.
- Spark drivers stay serialized by `/runtime/locks/spark-driver.lock` and the `spark_driver` pool.

## 3. Resources

Measured with `core` + `stream`: Kafka ≈ 0.55 GiB, kafka-ui ≈ 0.3 GiB, Flink JM ≈ 0.5 GiB,
TM ≈ 0.9 GiB under load, host ≈ 7 GiB available. Kafka Connect + Debezium need roughly 1–1.5 GiB;
Flink can be stopped during CDC work (`docker compose stop flink-jobmanager flink-taskmanager`), and
Spark `dp1_cdc` should run with Flink stopped. New services need `mem_limit`, a healthcheck and the
`cdc` profile; `make up-cdc` is still a placeholder (`exit 2`).

## 4. Suggested verification

- Scripted INSERT / repeated UPDATE / DELETE / concurrent multi-row UPDATE on `walmart.orders`,
  with expected final state computed independently; replay of the CDC job must not duplicate.
- Postgres vs Silver comparison (count + checksum) like the warehouse three-way check.
- Slot lag before/after stopping the connector; runbook for dropping a slot and re-snapshotting.
