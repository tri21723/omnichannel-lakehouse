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

## Next: Phase 3 — Iceberg + Lakekeeper

- Verify/pin Spark, Iceberg, Java/Scala and S3/JDBC compatibility.
- Deploy Lakekeeper backed by the existing PostgreSQL database and MinIO bucket.
- Bootstrap REST catalog and namespaces; test create/read/MERGE/time travel.
- Begin Walmart batch vertical slice, selecting one dataset manifest explicitly.

Batch/load parts of Phase 2 are implemented. Streaming generator and its late/burst
controls remain reserved for Phase 7. Million-row benchmarks have not been run.

## Later phases

Lakekeeper/Spark/Iceberg, Airflow, Trino, streaming, CDC capture, governance and AWS
are not implemented. Their Make targets fail explicitly. Runtime version compatibility
for these components remains pending ADR-0001 verification. No cloud resources exist
as a result of this implementation.
