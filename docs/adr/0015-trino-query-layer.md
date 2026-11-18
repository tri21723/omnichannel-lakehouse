# ADR-0015: Read-only Trino query layer (Phase 6)

- Status: Accepted for implementation (Phase 6).
- Context: Gold/DP3 (Iceberg via Lakekeeper REST) and the PostgreSQL warehouse are complete
  and orchestrated (ADR-0009…0013). Phase 6 adds interactive SQL for analysts (plan flows Q1–Q3)
  without changing any published contract. Lakekeeper runs unauthenticated locally (ADR-0006),
  Bronze keeps raw payloads, and `walmart.customers` holds raw names and emails.

## 1. Runtime

- Trino 484 (`trinodb/trino:484`, ADR-0001), single node (coordinator schedules work on itself),
  profile `query`, compose file `docker/compose.query.yml`, port `127.0.0.1:8083` → 8080.
- `mem_limit` 2.5 GiB, JVM heap fixed at 1.5 GiB (`-Xmx1536M`), `query.max-memory` 1 GiB.
- `make up-query` starts PostgreSQL, MinIO, Lakekeeper and Trino only; Spark and Airflow are not
  needed (Phase 6 preflight measured ~0.7 GiB for the three dependencies).
- Configuration lives in `docker/query/trino/` and is mounted read-only. Secrets are never in
  files: catalog properties use `${ENV:…}` and the values come from `.env`.

## 2. Catalogs (Q1, Q2)

| Catalog | Connector | Access | Credentials |
|---|---|---|---|
| `iceberg` | Iceberg, `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=${LAKEKEEPER_URI}/catalog`, warehouse `local_lakehouse`, vended credentials off; native S3 (`fs.s3.enabled`, MinIO endpoint, path-style, region from `.env`) | read-only | MinIO user `trino_local` (List/Get on `lakehouse` only) |
| `postgresql` | PostgreSQL, `jdbc:postgresql://postgres:5432/sources` | read-only | role `trino_ro` (SELECT on `walmart`, `warehouse`) |

The default `tpch`, `tpcds`, `memory` and `jmx` catalogs of the image are not shipped.

## 3. Security model (local development)

- No Trino authentication; the port is bound to loopback only. Anyone on the host can query,
  so the layer must be safe to expose read-only.
- Writes are blocked twice: file-based access control makes every catalog `read-only` (no
  CREATE/INSERT/DELETE/UPDATE/ALTER/DROP, no procedures), and the storage credentials cannot
  write (`trino_local` read-only S3 policy, `trino_ro` SELECT-only grants). Negative tests are
  part of `make smoke-query`.
- Raw PII is never returned:
  - `iceberg.bronze` is hidden (raw payloads embed customer names and emails; the data model keeps
    raw data restricted).
  - `postgresql.walmart.customers.full_name` and `email` are masked by column rules
    (`'[redacted]'`), so `SELECT *` still works in DBeaver but returns no raw values.
  - Silver/Gold/warehouse only contain HMAC hashes (`name_hash`, `email_hash`).
- `iceberg.benchmark` is hidden (not a published contract).
- Visible schemas: `iceberg.silver`, `iceberg.gold`, `postgresql.walmart`, `postgresql.warehouse`,
  plus `information_schema` and the `system` catalog read-only.

## 4. Reading consistently

- Every Iceberg and warehouse table is multi-dataset: queries filter `_dataset_run_id`.
- Trino reads the latest snapshot by default; during a running `batch_daily` it can observe Gold
  updated before DP3. Reproducible queries pin snapshots from the ledgers with
  `FOR VERSION AS OF <snapshot_id>`; reconciliation (`make verify-query`) always pins.
- Money stays DECIMAL(18,2) per channel currency (USD, VND); never sum across currencies or cast
  to double. Timestamps are UTC.

## 5. Consequences

- No new service outside the architecture; ports and existing pins unchanged.
- Changing access rules requires updating `rules.json` and the negative tests together.
- Sharing Trino beyond localhost needs TLS + authentication (password file or OAuth2) and a
  separate ADR.
