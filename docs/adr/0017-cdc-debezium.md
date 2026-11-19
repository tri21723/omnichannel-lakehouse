# ADR-0017: CDC of `walmart.orders` with Debezium (Phase 8)

- Status: Accepted for implementation (Phase 8).
- Context: Walmart orders live in the OLTP database `sources` (ADR-0003) and reach Bronze today only
  as daily JDBC snapshots (ADR-0007). Phase 8 adds the plan's CDC flow C1–C3: row changes on
  `walmart.orders` go through Debezium and Kafka (profile `stream`, ADR-0016) into Bronze and a
  current-state Silver table. Preflight ([CDC validation](../cdc-validation.md) §P8-01): PostgreSQL
  16.15 with `wal_level=logical`, publication `dbz_pub FOR TABLE walmart.orders` already created by
  the core bootstrap, role `debezium_repl` with `REPLICATION` but no `CREATE` on the database,
  `walmart.orders` `REPLICA IDENTITY FULL`, 3,135 orders from the three batch datasets.

## 1. Runtime and versions

| Component | Pin | Notes |
|---|---|---|
| Kafka Connect + Debezium | `quay.io/debezium/connect:3.6.3.Final` (`sha256:99aa63f43fa828bc991687dd9d3895cd83cb4213e3848ed0238c5fd83ba95e1c`, 777 MiB) | Debezium images are published on quay.io (Docker Hub stopped at 3.0.0). 3.6.3 (2026-11-02) is the newest release older than two weeks; 3.6.4/3.7.x were newer. The image bundles Kafka Connect 4.3.0 (same line as the 4.3.2 broker), Java 25, `debezium-connector-postgres-3.6.3.Final` and PostgreSQL JDBC 42.7.13, and runs as user `kafka` |
| Spark ↔ Kafka | `spark-sql-kafka-0-10_2.12:3.5.5`, `spark-token-provider-kafka-0-10_2.12:3.5.5`, `kafka-clients:3.4.1`, `commons-pool2:2.11.1` | added to the Spark image with checksums and Maven retries; Spark 3.5.5 itself is unchanged. Clients 3.4.1 work with the 4.3.2 broker (Kafka 4 only dropped clients older than 2.1) |
| Kafka | 4.3.2 from profile `stream` (ADR-0016) | only `kafka` is started; Flink and kafka-ui stay stopped |

Details and sources in ADR-0001.

## 2. Kafka Connect (profile `cdc`, `docker/compose.cdc.yml`)

- One worker in distributed mode (`group.id=cdc-connect`), internal topics `_connect.configs`,
  `_connect.offsets`, `_connect.status` (replication 1, compacted, created by the worker through the
  admin API; broker auto-creation stays off). Offsets therefore survive container re-creation.
- Converters: JSON without embedded schemas for keys and values.
- No host port: Connect's REST default 8083 is Trino's host port and AGENTS §5 has no slot for it.
  Management uses `docker compose exec kafka-connect curl localhost:8083`.
- `mem_limit` 1 GiB, heap 512 MiB, healthcheck on the internal REST endpoint.
- Secrets: the connector JSON in the repo references `${env:DEBEZIUM_REPL_DB_PASSWORD}` through
  Kafka's `EnvVarConfigProvider`; the password is neither in the file nor in `_connect.configs`.

## 3. Connector `walmart-orders` (`docker/cdc/connectors/walmart-orders.json`)

| Setting | Value | Why |
|---|---|---|
| `plugin.name` | `pgoutput` | built into PostgreSQL |
| `slot.name` / `publication.name` | `dbz_walmart` / `dbz_pub` | publication already exists |
| `publication.autocreate.mode` | `disabled` | `debezium_repl` cannot create publications, and must not widen it |
| `table.include.list` | `walmart.orders` | `customers` (raw names/emails) never reaches Kafka |
| `topic.prefix` | `platform_cdc` | topic `platform_cdc.walmart.orders` (AGENTS naming) |
| `snapshot.mode` | `initial` | 3,135 existing rows as `op = r`, then streaming |
| `decimal.handling.mode` | `string` | money stays exact (`total_amount` NUMERIC(18,2)) |
| `time.precision.mode` | `adaptive_time_microseconds` | `TIMESTAMPTZ` arrive as ISO-8601 UTC strings |
| `heartbeat.interval.ms` | `10000` | slot keeps advancing |
| `heartbeat.action.query` | `SELECT pg_logical_emit_message(false, 'debezium-heartbeat', now()::text)` | the same database receives heavy warehouse writes outside the publication; a logical message moves the slot without a heartbeat table (privilege verified in P8-03) |
| `message.prefix.exclude.list` | `debezium-heartbeat` | the heartbeat message still advances the slot but is not published; otherwise Debezium routes it to `platform_cdc.message`, which does not exist (broker auto-creation off), and the task blocks |
| `tombstones.on.delete` | `true` (default) | delete = event with `op = d` and a following null-value tombstone; both land in Bronze |
| `skipped.operations` | `t` (default) | `TRUNCATE` is not propagated; a truncate of the source requires a resnapshot (runbook) |

No connector-level `topic.creation.*` key is set: any such key switches on Connect's topic-creation
groups and then requires `topic.creation.default.*`; topics are pre-created instead.

Delivery is at-least-once: if Connect could not commit the offset that marks the snapshot as done
(observed in P8-03 while the task was blocked), a restart repeats the snapshot, and changes that
happened before the new snapshot are visible only as its `r` rows. Bronze keeps every record by its
Kafka coordinates and Silver applies the latest `(lsn, offset)` per key, so duplicates are harmless.

Topics are created explicitly and idempotently in the `cdc` bootstrap: `platform_cdc.walmart.orders`
(3 partitions, key = primary key, `retention.ms` 7 days) and the heartbeat topic
`__debezium-heartbeat.platform_cdc` (1 partition). A key always maps to one partition, so all
changes of one order are totally ordered by offset.

## 4. Bronze: `dp1_cdc` → `lh.bronze.walmart_orders_cdc`

- Spark Structured Streaming with `trigger(availableNow=True)` reads the topic. The checkpoint is on
  the `/runtime` volume (`/runtime/checkpoints/dp1_cdc`, shared by Make and Airflow drivers), not
  `s3://checkpoints/`: the Spark image only has Iceberg's `S3FileIO`, and adding S3A (`hadoop-aws` +
  AWS SDK) is not worth it because losing the checkpoint only replays — the MERGE below inserts
  nothing twice (measured in P8-05). Each micro-batch is written with `foreachBatch` as
  `MERGE INTO … ON (_kafka_topic, _kafka_partition, _kafka_offset) WHEN NOT MATCHED THEN INSERT`,
  executed through the micro-batch's own session (`batch.sparkSession`: foreachBatch uses a cloned
  session, so temp views are not visible to the outer one).
  Iceberg's streaming sink does not document exactly-once for a retried micro-batch, while the MERGE
  makes every retry and replay idempotent by construction (AGENTS §7: MERGE or overwrite).
- Columns: Kafka coordinates and timestamp, the raw key and value (`_raw`, null for tombstones), the
  parsed envelope (`op` c/u/d/r, `ts_ms`, `source.lsn`, `source.txId`, `before`, `after` as JSON
  text), `_parse_status`/`_error_reason`, and audit columns (`_ingest_ts`, `_batch_id`,
  `_source_type = cdc`, `_channel = walmart`). Nothing is dropped: tombstones and unparsable values
  are kept with their status.
- This table is separate from the JDBC Bronze tables and from every published Silver/Gold table.

## 5. Silver: `lh.silver.walmart_orders_cdc_current` (current state, soft delete)

Owner decision (2026-11-19): current state with soft deletes; history stays in Bronze CDC.

- One row per `order_id` with typed business columns (DECIMAL(18,2), UTC timestamps), `is_deleted`,
  `_cdc_op`, `_cdc_lsn`, `_cdc_ts_ms`, `_kafka_partition`, `_kafka_offset`.
- Ordering key per order: `(lsn, kafka offset)`. Each run takes the latest Bronze event per key and
  merges it only if its ordering key is greater than the stored one, so replays and out-of-order
  batches never overwrite a newer state. `d` → `is_deleted = true` (row kept, business columns from
  `before`); a later `c` for the same key revives it.
- Rows that cannot be typed go to `lh.silver.quarantine_walmart_orders_cdc` with `_error_reason`.
- Published batch Silver tables (`silver.orders`, …) are not touched.

## 6. Scenarios and isolation from batch

- The generator writes CDC scenarios (inserts, repeated updates, deletes, multi-row transactions,
  concurrent transactions, delete then re-insert) only on rows whose ids start with
  `<cdc_run_id>-walmart-`, using the `generator` role. Batch JDBC selects by
  `<run_id>-walmart-`, so the three batch datasets cannot change; `make cdc-batch-guard` proves it.
- Ground truth: `s3://raw/manifests/<cdc_run_id>/cdc_ground_truth.json` (final state per order,
  expected operation counts, checksum), immutable.

## 7. Verification and operations

- `make verify-cdc` (read-only): PostgreSQL ↔ Silver — live row count and checksum over
  `(order_id, status, total_amount, updated_ts)` for the whole table and for the scenario prefix,
  deleted ids present in Silver with `is_deleted`, Bronze offsets unique.
- Slot monitoring (`pg_replication_slots`, `pg_wal_lsn_diff`), connector stop/restart, Kafka/Connect
  restart, resnapshot and source schema change are exercised and documented in the runbook.
- Scheduling: DAG `dp1_cdc` (Bronze then Silver, pool `spark_driver`, paused at creation) every
  10 minutes plus Make targets; never a long-running Spark streaming query holding the driver lock.

## 8. Consequences

- The Spark image grows by four jars; batch jobs are unaffected (no version changes).
- A new profile `cdc` and service `kafka-connect`; `make up-cdc` replaces the placeholder.
- The replication slot `dbz_walmart` retains WAL while the connector is down, bounded by
  `max_slot_wal_keep_size=5GB`; dropping the slot is a manual, documented step.
