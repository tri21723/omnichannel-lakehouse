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

Evidence: [lakehouse validation](lakehouse-validation.md). Hosted CI has not been run.

## Next: Phase 4 — business Bronze pipeline

Detailed upcoming work: [Phase 4A checklist](../TODO.md). Phase 3 checklist is
[archived](phase3-checklist.md); Initial Walmart JSONL/JDBC slice now runs locally; Phase 4A validation is in progress.

Continue dp1_bronze validation against [the ingestion/replay contract](bronze-contract.md), then
Walmart Silver/Gold vertical slice. Multi-table JDBC snapshot coordination needs
explicit implementation/testing before claiming consistent parallel source ingestion.
Streaming generator and million-row benchmarks remain deferred.

## Later phases

Business Spark jobs, Airflow, Trino, streaming, CDC capture, governance and AWS
are not implemented. Their placeholder Make targets fail explicitly. Later components
still need compatibility verification. No cloud resources exist
as a result of this implementation.

## Phase 4A initial vertical slice

- Manifest-selected bounded JSONL staging shared by driver and workers.
- Raw payload/provenance/parse status preserved, including intentional duplicates.
- Atomic MERGE replacement and persistent completion state with replay/verify.
- Eager JVM JDBC export under one PostgreSQL snapshot across all 5 Walmart tables.
- Walmart one-day JSONL/JDBC full replay, incremental and read-only verification pass.
- Extended multi-channel, mutation, failure and persistence coverage is tracked in TODO.

No Silver/Gold business transforms or full Phase 4A completion is claimed.
