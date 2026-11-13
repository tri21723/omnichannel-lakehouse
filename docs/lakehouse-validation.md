# Phase 3 validation — 2026-11-13

## Baseline P3-01

Core resumed successfully. smoke-core, verify-sources and make check passed (23 tests).
Dataset b1e64afaff0176de180a: 3,000 Walmart orders reconciled, 200 customers,
20 products, 3,000 items and 300 returns. All five S3 channel inventories/checksums pass.
Host reports 11GiB RAM, approximately 9.5GiB available, 84GiB disk available.
Port inspection in sandbox was restricted; actual service binding is checked at startup.
No generated data or credentials were changed.

## Iceberg runtime and catalog

Validated locally with Spark 3.5.5, Iceberg 1.9.2 (runtime/AWS bundle), Java 17.0.21,
Python 3.11.11 and Lakekeeper v0.9.3. Image builds twice use cache; both Iceberg
JARs pass Maven Central SHA-512 checks. No dependency downloads at job submission.

Warehouse UUID: eca02e18-df2c-11f1-aec2-47941e962d23. Bootstrap repeated and after
resume retains the same ID. The REST handshake resolves local_lakehouse by name,
whereas management uses UUID. A real API mismatch was corrected before validation:
list uses id/name; S3 endpoint is normalized with a trailing slash.

Two alive Spark workers were confirmed through master JSON status; Spark distributed
SQL actions successfully wrote/read MinIO with non-root service credentials.

Fixture: lh.bronze._smoke_orders, format v2, day partition, Parquet/ZSTD.
INSERT snapshot 9213428472453640956: [(1,10.00), (2,20.00)]. After MERGE:
[(1,15.00), (2,20.00), (3,30.00)]. Nullable note column added; old rows read NULL.
Time travel returns the original two rows and amounts. Replay verifies existing
fixture rather than resetting it.

Passed commands: init-lakehouse twice, smoke-lakehouse twice, restart-lakehouse then
verify-lakehouse; make down then up-batch then verify-lakehouse. The read-only verifier
retains the same snapshot without regenerating fixture data. Core smoke and source
reconciliation still pass after the full down/up cycle.

MinIO inspection: 4 metadata JSON and 4 Parquet files under warehouse prefix at
validation time. Spark raw write is correctly denied; generator lakehouse read is
correctly denied by core regression tests. No raw dataset objects were replaced.

Bootstrap unit tests cover unchanged warehouse identity, storage mismatch refusal,
bounded unavailable-catalog retries and conflict/forbidden handling. API error bodies
are redacted. A manual GitHub integration workflow is provided; it has not been run
on GitHub from this session. Container restarts are tested, not host disaster recovery.

Maintenance remains a documented operational policy; no snapshot expiry, orphan
removal or production-data compaction was performed. Authentication is disabled for
local demo, and the current Spark image targets amd64. Business Bronze ingestion,
Trino and Airflow are subsequent phases. No cloud resources were provisioned.

Final runtime probe loads PostgreSQL JDBC, Iceberg SparkCatalog and S3FileIO classes;
Scala 2.12.18 and Hadoop 3.3.4 confirmed. test-catalog-failure injects an invalid catalog
path only in its own driver, asserts HTTP 404 failure, then successfully reads the
existing snapshot again. Bootstrap availability tests cover bounded connection retries.
