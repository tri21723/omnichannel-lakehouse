# ADR-0001: Runtime versions and compatibility verification

- Status: Core versions integration-tested locally; batch/stream pending.
- Development baseline: Python 3.11; tooling pins live in requirements-dev.txt.
- Python 3.12 can run scaffold checks; service images will target Python 3.11.

Before adding runtime images, verify official documentation and record exact
versions, source URLs and smoke-test evidence for:

| Group | Compatibility to verify |
| --- | --- |
| Spark | Spark, Java, Scala, Iceberg runtime, Hadoop, S3 libraries, PostgreSQL JDBC |
| Catalog/query | Lakekeeper, PostgreSQL, MinIO, Iceberg REST, Trino |
| Orchestration | Airflow, Python, Java, Spark provider/client |
| Stream/CDC | Kafka, Flink, connector libraries, Debezium, PostgreSQL |
| Governance | DataHub and its bundled dependencies |

Do not use `latest` tags or treat example versions in the plan as validated pins.
Runtime Compose files will be added with their implementation phases rather than
empty services that imply a working stack.

## Core versions (2026-11-12)

| Component | Pin | Distribution |
| --- | --- | --- |
| PostgreSQL | 16.15-bookworm | Official postgres image |
| MinIO | RELEASE.2025-09-07T16-13-09Z | Built from upstream source tag |
| MinIO client | RELEASE.2025-08-13T08-35-41Z | Built from upstream source tag |
| Go build toolchain | 1.24.7-alpine3.22 | Official golang image |
| MinIO runtime base | Alpine 3.22.2 | Official alpine image |

The MinIO server requires Go >= 1.24; mc requires Go >= 1.23. Both are built
with Go 1.24.7 and GOTOOLCHAIN=local. No service binaries are taken from third-party
images. Runtime images preserve upstream licenses. Git release tags and Go module
checksums pin source dependencies; OS package updates are not a fully locked build.

The MinIO repositories are archived. Tested Quay pulls returned 401 and Docker Hub
server lookup was denied. Build from the same upstream releases instead of relying
on those registries. These are local learning/demo versions, not a production
maintenance commitment. A future production migration needs its own ADR.

Sources checked:
- [Postgres official image](https://hub.docker.com/_/postgres)
- [MinIO server release](https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z)
- [MinIO client release](https://github.com/minio/mc/releases/tag/RELEASE.2025-08-13T08-35-41Z)
- [Server Go requirements](https://github.com/minio/minio/blob/RELEASE.2025-09-07T16-13-09Z/go.mod)
- [Client Go requirements](https://github.com/minio/mc/blob/RELEASE.2025-08-13T08-35-41Z/go.mod)

Validation evidence is recorded in docs/core-validation.md. No Spark/Iceberg/Trino
compatibility is claimed by these core smoke tests.

## Generator runtime

Python image: `python:3.11.11-slim-bookworm`. Direct Python dependencies are pinned in
`generators/requirements.txt`; dependency upgrades affecting bytes require reviewing
batch-v1 identity/replay compatibility. This is a direct-dependency pin set, not a
fully hashed transitive lockfile. Python 3.12 host unit tests and Python 3.11 container
integration are both exercised. No existing service image version was changed.

## Phase 3 selected pins

See ADR-0006: Spark 3.5.5, Scala 2.12, Java 17, Python 3.11.11,
Iceberg runtime and AWS bundle 1.9.2, JDBC 42.7.5, Lakekeeper v0.9.3.
These are new pins; core versions remain unchanged. Actual runtime evidence is
recorded separately in lakehouse-validation.md. Maven Iceberg JARs verify SHA-512
against Maven Central at build time; OS packages remain distribution-managed.
