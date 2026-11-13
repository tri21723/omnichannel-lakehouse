# ADR-0006: Local REST catalog and Iceberg runtime

Status: locally validated; runtime evidence in lakehouse-validation.md.

- Spark 3.5.5 / Scala 2.12 / Java 17 / Python 3.11.11.
- Iceberg runtime spark-3.5_2.12 and aws-bundle both 1.9.2; PostgreSQL JDBC 42.7.5.
- Lakekeeper v0.9.3 with PostgreSQL 16 backend. Local unauthenticated catalog binds
  only to loopback on the host; no external identity provider in this phase.
- Iceberg tables use S3FileIO with explicit Spark service credentials. Lakekeeper
  validates warehouse storage with its own credential; STS vending is disabled.
- Raw objects will be read with boto3 by exact manifest keys into bounded local
  staging before Spark parsing. S3A/Hadoop-AWS is deferred; do not interpret S3FileIO
  as a general Spark filesystem. No additional Hadoop SDK is added.
- Spark driver runs in a dedicated submit container with stable service DNS; workers
  can reach it inside the Compose network. Two workers advertise 2 cores/2GiB each,
  with extra container memory for JVM overhead. Jobs cap total cores at two.
- Bootstrap preserves the default project from Lakekeeper and stores warehouse UUID/name
  in a shared runtime volume. REST config handshake v0.9.3 resolves warehouse name;
  UUID is retained for management API lookup and bootstrap identity assertions.
- Encryption key persists in .env. Missing variables can be added without rotating
  existing credentials. Migration runs explicitly before serve.

Sources checked 2026-11-13:
- https://spark.apache.org/docs/3.5.5/
- https://iceberg.apache.org/docs/1.9.2/spark-getting-started/
- https://github.com/lakekeeper/lakekeeper/blob/v0.9.3/examples/minimal/docker-compose.yaml
- https://github.com/lakekeeper/lakekeeper/blob/v0.9.3/examples/minimal/create-default-warehouse.json
- https://docs.lakekeeper.io/docs/0.9.x/storage/
