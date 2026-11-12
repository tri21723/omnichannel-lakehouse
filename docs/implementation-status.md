# Implementation status

## Available

- Project scaffold, development tools, Makefile, lint/unit-test CI and Compose validation.
- Original plan/architecture, proposed data contracts, ADRs and generator defaults.
- Channel-scoped identity helper and unit tests.
- PostgreSQL + MinIO core Compose with healthchecks, memory limits and named volumes.
- Re-runnable database/role/schema/publication and bucket/user/policy bootstrap.
- Generated local credentials; service permissions and denied-access smoke tests.
- Core build, startup, restart and persistence checks validated locally.

See [validation evidence](core-validation.md). DBeaver UI and boto3 checks remain
unverified; automated S3 tests currently use mc. Service containers are left running
for local use. `make down` stops them while retaining data.

## Next: Phase 2 — generators

- Deterministic batch/load CLI using generator config and seed.
- Walmart source load and JSONL exports with manifest/reconciliation.
- Controlled duplicates, skew and Amazon schema drift for five channels.
- Runtime image, tests and working make gen-batch/gen-load targets.

## Later phases

Lakekeeper/Spark/Iceberg, Airflow, Trino, streaming, CDC capture, governance and AWS
are not implemented. Their Make targets fail explicitly. Runtime version compatibility
for these components remains pending ADR-0001 verification. No cloud resources exist
as a result of this implementation.
