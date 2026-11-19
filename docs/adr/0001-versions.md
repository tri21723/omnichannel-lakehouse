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

## Phase 5 selected pins (2026-11-18)

See ADR-0013: Apache Airflow **3.3.2** (`apache/airflow:3.3.2-python3.11`), installed
packages constrained by `constraints-3.3.2/constraints-3.11.txt`. Airflow 2.x reached EOL on
2026-04-22; 3.4.0 (released 2026-11-06) was considered too new, so the last patch of the
previous minor line is used. The Airflow image adds Java 17 and PySpark 3.5.5 (outside the
Airflow constraints, `py4j` only dependency) and copies the Iceberg 1.9.2 / PostgreSQL JDBC
42.7.5 jars from the pinned Spark image so driver and executors load identical jars.
Python 3.11 matches the Spark workers (3.11.11). No existing pin or port was changed.

Sources checked (2026-11-18):
- [Airflow supported versions](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html)
- [apache-airflow on PyPI](https://pypi.org/project/apache-airflow/3.3.2/) (3.3.2 uploaded 2026-09-17)
- [Constraints 3.3.2 / Python 3.11](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt)
- [Simple auth manager](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html)

## Phase 6 selected pins (2026-11-18)

See ADR-0015: Trino **484** (`trinodb/trino:484`, image
`sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`, published
2026-10-16, bundled OpenJDK 25.0.4). It was the newest tag and older than two weeks at
selection time. Iceberg access uses the built-in Iceberg connector with the REST catalog
(Lakekeeper v0.9.3) and the native S3 file system (`fs.s3.enabled`); PostgreSQL access uses the
built-in PostgreSQL connector. No existing pin or port was changed.

Sources checked (2026-11-18):
- [trinodb/trino tags](https://hub.docker.com/r/trinodb/trino/tags)
- [Iceberg REST catalog properties](https://trino.io/docs/484/object-storage/metastores.html)
- [Native S3 file system](https://trino.io/docs/484/object-storage/file-system-s3.html)
- [File-based access control](https://trino.io/docs/484/security/file-system-access-control.html)

## Phase 7 selected pins (2026-11-19)

See ADR-0016. Apache Kafka **4.3.2** (`apache/kafka:4.3.2`, published 2026-10-20, KRaft) rather
than 4.4.0 (2026-10-22): 4.3 is the newest line kafka-python lists as tested. kafka-ui
`kafbat/kafka-ui:v1.5.0` (2026-04-20). Apache Flink **2.2.1** (`flink:2.2.1-java17`) because the
newest Kafka connector, `flink-connector-kafka:5.0.0-2.2`, supports Flink 2.1/2.2 but not 2.3.0.
Jobs are Java, built with `maven:3.9.16-eclipse-temurin-17`. Generator client `kafka-python`
3.0.x (pure Python). Image digests are recorded when the images are pulled (P7-03). No existing
pin or port was changed; Kafka 9092, kafka-ui 8085 and Flink 8082 are the reserved ports.

Sources checked (2026-11-19):
- [Kafka downloads](https://kafka.apache.org/community/downloads/) (supported: 4.4.0, 4.3.2, 4.2.2)
- [apache/kafka tags](https://hub.docker.com/r/apache/kafka/tags)
- [Flink downloads](https://flink.apache.org/downloads/) (2.3.0 current; connector 5.0.0 for 2.1.x/2.2.x)
- [flink-connector-kafka versions](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/maven-metadata.xml)
- [flink tags](https://hub.docker.com/_/flink/tags), [kafbat/kafka-ui tags](https://hub.docker.com/r/kafbat/kafka-ui/tags)
- [kafka-python compatibility](https://kafka-python.readthedocs.io/en/master/compatibility.html)
  and [KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html)
- [apache-flink on PyPI](https://pypi.org/project/apache-flink/2.2.1/) (PyFlink option rejected, ADR-0016)
