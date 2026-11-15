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

## Next: Phase 4D — Feature Store and Return Labels (`dp3_features`, `dp3_return_labels`)

Per ADR-0003, `dp3_features` builds customer and product features with point-in-time correctness
(`order_ts < as_of_ts`), and `dp3_return_labels` computes mature return classification labels with
observation horizons and data leakage prevention. Warehouse export to PostgreSQL is Phase 4E.

## Later phases

Airflow, Trino, streaming, CDC capture, governance and AWS are not implemented.
Their placeholder Make targets fail explicitly. Later components need compatibility
verification. No cloud resources were provisioned. Million-row benchmarks, parallel
JDBC partitions, snapshot expiry, compaction and orphan cleanup remain deferred.

## Hosted CI update — 2026-11-13

The user confirmed that the billing issue is resolved and hosted CI succeeded.
No workflow name, run URL/ID or commit was supplied, so this records user-reported
success rather than independent verification of every integration workflow.
Future Silver changes still require their own relevant checks.
