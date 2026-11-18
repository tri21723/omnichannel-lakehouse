# Implementation status

## Available

- Project scaffold, development tools, Makefile, lint/unit-test CI and Compose validation.
- Original plan/architecture, proposed data contracts, ADRs and generator defaults.
- Channel-scoped identity helper and unit tests.
- PostgreSQL + MinIO core Compose with healthchecks, memory limits and named volumes.
- Re-runnable database/role/schema/publication and bucket/user/policy bootstrap.
- Generated local credentials; service permissions and denied-access smoke tests.
- Core build, startup, restart and persistence checks validated locally.
- Deterministic five-channel JSONL, exact duplicate/skew/return counts and schema drift.
- Chunked files and SHA-256 manifests; create-only S3 publish with manifest last.
- Walmart COPY staging, transactional idempotent upserts and full-row reconciliation.
- Generator Docker image, CLI, gen-batch/gen-load/gen-sources/verify-sources targets.
- 23 unit tests; default dataset generated and verified twice against MinIO/PostgreSQL.

See [validation evidence](core-validation.md). DBeaver UI remains unverified.
Boto3 access is now exercised by [generator integration](generator-validation.md). Service containers are left running
for local use. `make down` stops them while retaining data.

## Phase 3 available

- Lakekeeper v0.9.3 migration/server using the existing PostgreSQL role and database.
- Idempotent project/warehouse reconciliation and persistent runtime configuration.
- Spark 3.5.5 master + two workers, Iceberg 1.9.2, Java 17 and Python 3.11.11.
- Distributed CREATE/INSERT/MERGE, schema evolution, time travel, replay and restart
  validation. Full down/up preserves catalog and snapshot data.
- Non-root S3 permissions and object inspection; core/source regression checks pass.
- Manual integration CI, SQL dialect config, runbook and Phase 4 handoff contract.

Evidence: [lakehouse validation](lakehouse-validation.md). Hosted status: see the user confirmation below.

## Phase 4A — Bronze available

- Manifest-selected five-channel JSONL ingestion with SHA/size/count validation.
- Bounded shared staging accessible to both Spark workers; raw/unknown fields,
  duplicates and malformed/blank lines preserved with parse status/provenance.
- Eager JVM JDBC readers import one exported PostgreSQL snapshot across five Walmart
  tables. Concurrent inserts/updates and broken reader/coordinator recovery are tested.
- Atomic MERGE replaces one run/channel/date/source slice; full replay, two runs on
  the same date, initially empty slices and NULL identity rejection are verified.
- Completion records selected source, snapshots, attempt status/timestamps and metrics.
  Accepted staging/write failures become failed; partial runs cannot be consumed.
- CLI safe reason codes, read-only verifier and manual integration workflow with
  allowlisted CI diagnostics. Hosted CI success is user-confirmed; workflow/run scope is not recorded.

See [Bronze validation](bronze-validation.md), [contract](bronze-contract.md) and
[completed checklist](phase4a-checklist.md). Historical test counts above refer to earlier
milestones; the final Phase 4A report records the current count.

## Phase 4B — Silver complete locally

- Local milestone v0.4b-silver; 98 unit tests and lint, matrix/recovery/restart/regression pass.
- Five entities across Walmart JDBC and four authoritative JSONL channels.
- Strict typed parsing, schema drift mappings, channel-scoped keys and relational DQ.
- Required keyed HMAC PII, reference-only quarantine and per-entity occurrence accounting.
- Immutable pinned Bronze inputs, ancestry-checked revisions, deterministic reduction,
  shared profile retention and scoped corrections/empty-slice retraction.
- Serialized MERGE publication with a completed snapshot ledger, read-only verifier,
  failure recovery and source key/monetary reconciliation.
- Small matrix: 375 orders; default matrix: 15,000 orders. Both datasets coexist.
- Manual Silver CI workflow added; new hosted execution is not claimed.

See [Silver validation](silver-validation.md), [completed checklist](phase4b-checklist.md) and
[Gold handoff](gold-handoff.md).

## Phase 4C — Gold complete locally

- Local milestone v0.4c-gold; 110 unit tests, full lint and all regression/recovery checks pass.
- Kimball star schema published in `lh.gold` on Iceberg v2: 5 dimensions (`dim_date`, `dim_channel`,
  `dim_customer`, `dim_product`, `dim_seller`) and 3 facts (`fact_orders`, `fact_order_items`, `fact_returns`).
- Pinned inputs from completed Silver publication ledger `/runtime/silver/publication/ledger.json`
  by exact snapshot ID with zero fallback to latest.
- Multi-dataset coexistence: small fixture (`186ae107d0793d2ee237`, 375 orders) and default reference
  (`b1e64afaff0176de180a`, 15,000 orders) coexist cleanly in `lh.gold.*` (15,375 orders total) with
  per-run scoping (`_dataset_run_id`) and zero cross-run surrogate key collisions.
- Exact monetary arithmetic using `DECIMAL(18,2)`: full three-way reconciliation against Silver and
  Bronze totals across natural keys, line-item sums, and multi-currency segregation (`USD`, `VND`).
- Strict PII isolation: zero raw names or emails; `dim_customer` contains only SHA-256 HMAC hashes.
- Atomic publication ledger with failure injection recovery (`fail_after="dimensions"` and
  `fail_after="facts"`) verified via `make test-gold-recovery`.

See [Gold validation](gold-validation.md) and [Features & Warehouse handoff](features-warehouse-handoff.md).

## Phase 4D — Feature Store and Return Labels complete locally

- Local milestone v0.4d-features; 138 unit tests, full lint and all regression/recovery checks pass.
- Feature Store and supervised Return Labels published in `lh.gold` on Iceberg v2:
  `lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`, and `lh.gold.lbl_order_return`.
- Pinned inputs from completed Gold publication ledger `/runtime/gold/publication/ledger.json`
  by exact snapshot ID (`46d63f72f5aba0aec78bc2601491a2bb5db092662feb551ea61fb16640acd37c`);
  independent DP3 publication ledger recorded at `/runtime/dp3/publication/ledger.json`.
- Dual-timestamp point-in-time leakage prevention (`order_ts < as_of_ts` and `return_ts < as_of_ts`)
  and 7d/30d rolling windows verified with future event injection tests (`make test-dp3-dq`).
- Observation horizon (`horizon_days = 14`) and maturity right-censoring (`is_mature = false`
  forces `is_returned = NULL`, never `0`) verified at `2026-01-03` (100% immature) and `2026-01-18`
  (100% mature, exact 30 / 1,500 returned orders).
- Multi-dataset coexistence (`186ae107d0793d2ee237` and `b1e64afaff0176de180a`), atomic recovery
  across three failure injection points (`make test-dp3-recovery`), and restart persistence verified.

See [DP3 validation](dp3-validation.md), [completed checklist](phase4d-checklist.md), and
[Features & Warehouse handoff](features-warehouse-handoff.md).

## Phase 4E — Data Warehouse Export (`dp4_warehouse`) complete locally

- Local milestone `v0.4e-warehouse`; all lint checks, unit tests, bi-directional reconciliation, sliding-window pruning, transactional rollback, and post-restart regression checks pass.
- Synchronizes a 30-day UTC sliding window `[run_date - 29 days, run_date + 1 day)` from pinned `lh.gold.*` Iceberg snapshots (`/runtime/gold/publication/ledger.json`) into PostgreSQL schema `warehouse.*` (`dim_date`, `dim_channel`, `dim_customer`, `dim_seller`, `dim_product`, `fact_orders`, `fact_order_items`, `fact_returns`).
- Physical DDL (`sql/postgres/003_warehouse.sql`) enforces PKs (including multi-run composite keys on `dim_date` and `dim_channel`), foreign keys, `NUMERIC(18,2) >= 0` constraints, indexes, zero clear PII, and `trino_ro` read-only permissions.
- Single-connection JVM JDBC driver writes as `spark_writer` (`setAutoCommit(False)`) ensure atomic ACID rollback across all 8 tables (`fail_after="dimensions" | "facts" | "completion"`), isolated ledger `/runtime/warehouse/export/ledger.json`, and safe idempotent replay.
- Multi-dataset coexistence verified across `186ae107d0793d2ee237` (375 orders) and `b1e64afaff0176de180a` (15,000 orders) with `0.00` cent-level monetary discrepancy across all channels/currencies (`USD`, `VND`).
- Manual CI workflow `.github/workflows/warehouse.yml` and allowlisted events added; hosted CI run not claimed until triggered.

See [Warehouse validation](warehouse-validation.md), [ADR-0011](adr/0011-warehouse-export.md), and [Features & Warehouse handoff](features-warehouse-handoff.md).

## Phase 4F — Performance & Skew Benchmark complete locally

- Local milestone `v0.4f-benchmark`; Phase 4 closed. Baseline: Gold publication `332aa290…`
  (policy v2), DP3 features v4 / labels v3 and warehouse export all pinned to it; verify,
  reconcile, DQ, 22 regressions and same-parameter replay (all skipped, ledgers unchanged) pass
  ([baseline manifest](benchmarks/baseline-4f.json)).
- Harness `spark/jobs/benchmark_skew/` + `spark/lib/benchmark_*.py`: pinned datasets by deterministic
  synthetic amplification (15k / 105k / 1.005M lines, control vs 80% hot key), one Spark
  application per trial, `noop` materialization, event-log metrics (task skew, shuffle, spill,
  retries), physical-plan evidence, correctness gate vs B0, redacted reports.
- 240 trials across smoke, ci, matrix-100k, matrix-1m and focus-1m: all ok and correct, no spill
  or task failure. AQE skew join (tuned), broadcast alone, salting and more partitions gave no
  reproducible gain; the production session (AQE + broadcast) was consistently fastest
  (paired 1.25× vs B0 on the 1M join), so production config is unchanged.
- Manual CI workflow `.github/workflows/benchmark.yml` with allowlisted events; hosted run not claimed.

See [skew benchmark](benchmarks/skew.md), [ADR-0012](adr/0012-skew-benchmark.md),
[checklist](phase4f-checklist.md) and [Phase 5 handoff](phase5-handoff.md).

## Phase 5 — Airflow orchestration complete locally

- Local milestone `v0.5-orchestration`. Apache Airflow 3.3.2 (`standalone`, LocalExecutor,
  `parallelism=1`, metadata in PostgreSQL `airflow`) in profile `orch`, UI on 127.0.0.1:8081,
  simple auth with a generated admin password ([ADR-0013](adr/0013-airflow-orchestration.md)).
- The Airflow container is the Spark client-mode driver (same jars/code as the Spark image,
  `SPARK_DRIVER_HOST`); every driver from `make` or Airflow takes `/runtime/locks/spark-driver.lock`
  and the `spark_driver` pool has one slot.
- DAGs: `dp1_bronze`, `dp2_silver`, `dp2_gold`, `dp3_features`, `dp3_return_labels`,
  `dp4_warehouse` (one per job) and controller `batch_daily` (deferrable triggers,
  `depends_on_past`), plus `make test-dags`, `make e2e`, CI DagBag job and a manual e2e workflow.
- New orchestration fixture `5f552019ee41c24af95f` was ingested end to end through Airflow for
  2026-01-01..03 and replayed; the existing datasets still verify and reconcile.
- Fix found by orchestration: pinned older Gold publications are read from their archive, so
  warehouse reconciliation keeps working after another dataset publishes Gold.
- Layer assets for UI visibility; `maintenance_iceberg` weekly dry-run retention report
  ([ADR-0014](adr/0014-iceberg-snapshot-retention.md)) that excludes pinned snapshots and never
  deletes; Graph/Gantt screenshots in `docs/images/phase5/`.
- Deferred: real snapshot expiry/compaction, DataHub lineage (Phase 9), `dp1_cdc` (Phase 8).
  Hosted runs of the new workflows are not claimed.

See [orchestration validation](orchestration-validation.md), [checklist](phase5-checklist.md)
and [Phase 6 handoff](phase6-handoff.md).

## Phase 6 — Trino query layer complete locally

- Local milestone `v0.6-query`. Trino 484 single node in profile `query` (127.0.0.1:8083,
  2.5 GiB), catalogs `iceberg` (Lakekeeper REST + MinIO, read-only key) and `postgresql`
  (`trino_ro`); `make up-query` needs no Spark/Airflow ([ADR-0015](adr/0015-trino-query-layer.md)).
- Read-only in three layers (file-based rules, `iceberg.security=read_only`, storage credentials);
  a table-procedure gap in file-based rules was found and closed. Raw PII is never returned
  (`iceberg.bronze` hidden, `walmart.customers` name/email masked); `iceberg.benchmark` hidden.
- `sql/trino` samples (incl. federated and time travel) with a typed-parameter runner;
  `make verify-query` proves Trino = Spark on the same SQL at ledger snapshots, ledger row counts,
  Iceberg = warehouse = ledger money, and time travel. DBeaver template + JDBC check; read-only
  asset-triggered `query_checks` DAG; manual workflow `query.yml` green on GitHub
  ([run 44374160331](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44374160331)).

See [query validation](query-validation.md), [checklist](phase6-checklist.md),
[DBeaver](dbeaver.md) and [Phase 7 handoff](phase7-handoff.md).

## Next: Phase 7 — Kafka + Flink

Not started. See [Phase 7 handoff](phase7-handoff.md).

## Later phases

Airflow, Trino, streaming, CDC capture, governance and AWS are not implemented.
Their placeholder Make targets fail explicitly. Later components need compatibility
verification. No cloud resources were provisioned. Parallel
JDBC partitions, snapshot expiry, compaction and orphan cleanup remain deferred.

## Hosted CI update — 2026-11-13

The user confirmed that the billing issue is resolved and hosted CI succeeded.
No workflow name, run URL/ID or commit was supplied, so this records user-reported
success rather than independent verification of every integration workflow.
Future Silver changes still require their own relevant checks.
