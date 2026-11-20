# ADR-0018: Data governance with DataHub (Phase 9)

- Status: Accepted for implementation (Phase 9).
- Context: batch, streaming and CDC flows are complete and their lineage is already recorded in
  ledgers that pin exact upstream snapshots (ADR-0007…0017). Phase 9 adds the plan's governance flow
  (O4, G1): a catalogue of sources and lakehouse tables, enriched metadata, and lineage from sources to
  the warehouse that a script can assert. Preflight ([governance validation](../governance-validation.md)
  §P9-01): ~7.7 GiB free with `core` + Lakekeeper + Kafka; `trino_ro` and `spark_reader` can read
  `walmart.customers` rows.

## 1. Versions

| Component | Pin | Notes |
|---|---|---|
| DataHub server | `acryldata/datahub-gms`, `datahub-frontend-react`, `datahub-upgrade` **v1.7.0.1** (2026-09-03) | newest server line whose matching CLI is older than two weeks; v1.8.0 (2026-10-22) has only a 2-day-old CLI 1.8.0, v1.9.x is days old |
| CLI / SDK | `acryl-datahub` **1.7.0.19** (2026-10-30) with extras `datahub-rest`, `postgres`, `kafka`, `iceberg` | same 1.7 line as the server |
| Search engine | `opensearchproject/opensearch:2.19.3` | the version the v1.7.0.1 quickstart uses |

Digests are recorded when the images are pulled (P9-03) and in ADR-0001.

## 2. Deployment (profile `governance`, `docker/compose.governance.yml`)

Only what DataHub needs, reusing the platform's PostgreSQL and Kafka (the official quickstart would
add MySQL, its own Kafka and more, which does not fit the memory budget):

| Service | Port | `mem_limit` | Notes |
|---|---|---|---|
| `datahub-opensearch` | none | 1.5 GiB | single node, heap 768 MiB–1 GiB, security plugin off (loopback only) |
| `datahub-system-update` | none | job | `datahub-upgrade -u SystemUpdate`: builds indices, SQL schema and **pre-creates DataHub's Kafka topics** (`DATAHUB_PRECREATE_TOPICS`), so broker auto-creation stays off |
| `datahub-gms` | none (internal 8080) | 2 GiB | heap 1 GiB; internal schema registry (`SCHEMA_REGISTRY_TYPE=INTERNAL`); never published, so Spark keeps host 8080 |
| `datahub-frontend` | `127.0.0.1:9002` | 1 GiB | heap 512 MiB; the only UI/GraphQL entry from the host |
| `gov-postgres-init` | none | job | idempotent: database/role `datahub` (metadata store) and role `datahub_ro` (ingestion) — core bootstrap unchanged |

- Metadata store: PostgreSQL 16, `jdbc:postgresql://postgres:5432/datahub`, role `datahub` (owner of
  database `datahub`), a configuration DataHub supports officially (`quickstart-postgres` profiles).
- Kafka: the platform broker (`kafka:29092`); DataHub topics are created by the system-update job.
- No `datahub-actions`, MySQL, Neo4j or extra Kafka/Schema Registry.
- Secrets: generated once in `.env` by `make env-gov` (database password, system client secret,
  token salt and signing key, frontend secret, UI admin password); none of the quickstart defaults
  (`JohnSnowKnowsNothing`, fixed `DATAHUB_SECRET`, `datahub/datahub`) is used. GMS runs without
  metadata-service authentication but has no host port; the UI requires the generated password.
- Budget ≈ 4.5 GiB (measured: GMS 1.6 GiB, OpenSearch 1.2 GiB, frontend 0.6 GiB); DataHub runs with `core` + Kafka (+ Lakekeeper/MinIO for Iceberg, + Airflow for the
  scheduled ingestion), never together with Spark, Flink or Trino.

## 3. Ingestion

- Runtime for Make/CI: image `omnichannel/datahub-ingest` (Python 3.11 + `acryl-datahub[datahub-rest,
  postgres,kafka,iceberg]==1.7.0.19`) on the compose network; recipes in `datahub/recipes/`, secrets
  from environment variables only.
- Scheduling (owner decision, 2026-11-19): DAG `datahub_ingest` (paused at creation, not in the
  `spark_driver` pool) calls the same pinned CLI from a separate virtualenv `/opt/datahub-venv` inside
  the Airflow image. The DataHub Airflow plugin is **not** used: `pip install
  acryl-datahub-airflow-plugin==1.7.0.19` with the Airflow 3.3.2 constraints fails with
  `ResolutionImpossible` (tested in the project image).
- Sources (all read-only, profiling and sampling off everywhere):
  - PostgreSQL `sources` schemas `walmart`, `warehouse` with role `datahub_ro` = schema `USAGE` only,
    no table `SELECT`: column metadata comes from the catalog, so the role structurally cannot read
    `walmart.customers` rows. Other databases (`airflow`, `lakekeeper`, `mlflow`) are not catalogued.
  - Iceberg via the Lakekeeper REST catalog with MinIO user `trino_local` (read-only `lakehouse`);
    namespaces `bronze`, `silver`, `gold`; `benchmark` and `_smoke_*` tables excluded.
  - Kafka topic metadata only (stream topics, DLQs, CDC topic); internal topics excluded; no payload
    reads. The topics are JSON without registry subjects, so DataHub warns once per topic and the Kafka
    run is the only one not executed with `--strict-warnings`.

## 4. Lineage

- Dataset lineage is emitted by `datahub/lineage/` from the project's contracts and ledgers, not
  inferred from Spark at runtime: raw/PostgreSQL → Bronze → Silver → Gold → DP3, Gold → PostgreSQL
  `warehouse`, Kafka → Flink output → stream Bronze, `walmart.orders` → CDC topic → CDC Bronze → CDC
  Silver. The ledgers' pins make it checkable, and no job depends on DataHub being up.
- Airflow DataFlow/DataJob entities and their inputs/outputs are emitted from the DAG/task definitions
  by the same tooling (the plugin is incompatible, see §3).
- Emission is idempotent (stable URNs, upserted aspects, sorted upstreams, fixed audit time).
- MinIO prefixes (`raw/<channel>`, `raw/stream/<channel>`) have no recipe; the emitter declares them as
  `s3` datasets. `gold.dim_channel` is built from the static channel registry and has no upstream;
  G1 treats it as the only static Gold table.

## 5. Metadata as code and assertions

- `datahub/metadata/` holds domains, glossary terms, tags (`PII`, `raw`, `gold`, `quarantine`), owners,
  descriptions (from `docs/data-model.md`) and tiers; applied idempotently.
- Every object with raw names/emails (Bronze raw tables, silver quarantine `_raw`, stream payload
  topics/DLQs, `walmart.customers`) carries tag `PII`.
- G1 (`datahub/assertions/lineage_g1.py`, GraphQL through the frontend or the network-internal GMS):
  every Gold table has a Silver upstream, every Silver table a Bronze upstream, the warehouse tables
  Gold upstreams, stream/CDC tables their upstreams; checked against the ledgers; exit code ≠ 0 when an
  edge is missing. A negative test removes an edge on a test entity.

## 6. Consequences

- A new profile `governance`, the image `datahub-ingest`, a virtualenv in the Airflow image
  (~400–500 MB larger) and two PostgreSQL roles; no change to pinned versions or ports (frontend 9002
  was already reserved).
- Lineage depends on the ledgers staying the source of truth; a new pipeline must extend the emitter.
- Without metadata-service auth, any container on the compose network could write metadata; acceptable
  locally, to revisit for any shared deployment (Phase 10).
