# Phase 6 handoff: query layer (Trino + DBeaver)

Phase 5 is complete locally (`v0.5-orchestration`): Airflow 3.3.2 runs the batch chain per
logical date and is the only scheduler of Spark jobs. This page lists what Phase 6 can rely on.
It does not implement Phase 6.

## 1. Data available for queries

| Source | Location | Grain / notes |
|---|---|---|
| Iceberg Bronze/Silver/Gold | catalog `lh` via Lakekeeper REST (`LAKEKEEPER_URI`, port 8181), bucket `lakehouse` | Every table is multi-dataset: filter `_dataset_run_id`. Gold: `dim_*`, `fact_*`, `feat_*`, `lbl_*` (ADR-0009/0010) |
| Benchmark tables | `lh.benchmark.*` | Not a published contract; exclude from Trino catalogs or views shown to users |
| PostgreSQL warehouse | database `sources`, schema `warehouse` | 30-day window per dataset run (ADR-0011), `NUMERIC(18,2)`, PK/FK enforced |
| PostgreSQL OLTP | `sources.walmart` | Raw source tables, read-only for Trino |

Datasets present: `186ae107d0793d2ee237` (small fixture), `b1e64afaff0176de180a` (default
reference), `5f552019ee41c24af95f` (orchestration e2e fixture). Logical dates 2026-01-01..03.

## 2. Credentials and permissions already provisioned (ADR-0004)

- PostgreSQL role `trino_ro` (`TRINO_RO_DB_PASSWORD`): `CONNECT` on `sources`, `SELECT` on
  `walmart.*` and `warehouse.*`, including future warehouse tables created by `spark_writer`.
  Write attempts are denied (`make test-warehouse-dq` checks this).
- MinIO user `trino_local` (`TRINO_S3_ACCESS_KEY` / `TRINO_S3_SECRET_KEY`): read-only on the
  `lakehouse` bucket. Path-style access and the region from `.env` are required.
- Port 8083 is reserved for Trino (AGENTS.md §5); DBeaver connects to Trino, not directly to
  MinIO.

## 3. Consistency rules for readers

- Spark jobs pin inputs by snapshot ID from completed ledgers under `/runtime/*/ledger.json`.
  A Trino query of `latest` may observe a table mid-chain (for example Gold updated, DP3 not
  yet). For reproducible reads use `FOR VERSION AS OF <snapshot_id>` with the IDs in the Gold
  and DP3 ledgers, or query after `batch_daily` for that date has succeeded.
- Monetary columns are DECIMAL(18,2) per channel currency (USD, VND); never sum across
  currencies. PII is only available as HMAC hashes in Silver/Gold.
- Do not run Trino-driven writes into `lh.*`; Spark/Airflow own all publications.

## 4. Orchestration hooks

- `batch_daily` success for a date means Bronze → warehouse completed and verified for that
  date. A Phase 6 freshness or federation check can be added as a downstream DAG that reads,
  never writes, and does not use the `spark_driver` pool.
- Keep the resource budget: core + batch + orch already use up to ~10.5 GiB of `mem_limit` on
  the ~11.7 GiB host. Trino needs its own memory budget; stop `orch` (or lower limits with an
  ADR) when running the query profile on this host.
