# Downstream Data Warehouse Export validation — Phase 4E

Date: 2026-11-16. Local DoD complete; milestone `v0.4e-warehouse`.
Downstream PostgreSQL Data Warehouse export (`dp4_warehouse`) implemented and verified across all 8 Kimball star-schema tables in database `sources`, schema `warehouse.*`.
See [ADR-0003](adr/0003-data-contracts.md), [ADR-0011](adr/0011-warehouse-export.md), [physical DDL](../sql/postgres/003_warehouse.sql), [data model](data-model.md), and [Features & Warehouse handoff](features-warehouse-handoff.md).

## Runtime, Ownership & Security Boundary

- **Runtime Environment:** Spark 3.5.5, Python 3.11.11, Java 17, Iceberg 1.9.2, Lakekeeper 0.9.3 REST Catalog, MinIO S3, PostgreSQL 16.15 (`postgresql-42.7.5.jar`).
- **Source Pinning:** `dp4_warehouse` reads strictly read-only from completed Gold publication `/runtime/gold/publication/ledger.json` (`publication_id`: `46d63f72f5aba0aec78bc2601491a2bb5db092662feb551ea61fb16640acd37c`) via explicit `option("snapshot-id", ...)`, verifying content digests before slicing.
- **Least-Privilege Credentials:** `SPARK_WRITER_DB_PASSWORD` and `WAREHOUSE_JDBC_URL` are scoped exclusively to the `spark-submit` driver container in `docker/compose.batch.yml`; Spark workers never receive database write credentials. Role `trino_ro` holds `USAGE` on schema `warehouse` and `SELECT` on all 8 tables, with all DML writes (`INSERT`/`UPDATE`/`DELETE`) denied.
- **Single-Transaction Atomicity:** All 8 `warehouse.*` tables for a run are synchronized inside one UTC JVM PostgreSQL JDBC transaction (`conn.setAutoCommit(False)`), verified in-transaction before `conn.commit()`, and recorded independently at `/runtime/warehouse/export/ledger.json` (`export_id`: `3200f17c533abb1cd921321c5b645f211ee62e09d7e855493c9d3fdf8dcb19a8`) without mutating Gold or DP3 ledgers.

## Multi-Dataset Coexistence & PostgreSQL Verification

Both the small fixture (`186ae107d0793d2ee237`) and default reference (`b1e64afaff0176de180a`) datasets coexist in PostgreSQL `warehouse.*` at `run_date = 2026-01-17` (window `[2025-12-19T00:00:00Z, 2026-01-18T00:00:00Z)`):

| Table (`warehouse.*`) | Small Fixture (`186ae...`) | Default Reference (`b1e64...`) | PostgreSQL Total | Physical PK |
| --- | ---: | ---: | ---: | --- |
| `dim_date` | 15 | 17 | 32 | `(_dataset_run_id, date_key)` |
| `dim_channel` | 5 | 5 | 10 | `(_dataset_run_id, channel_key)` |
| `dim_customer` | 25 | 1,000 | 1,025 | `(customer_key)` |
| `dim_seller` | 88 | 100 | 188 | `(seller_key)` |
| `dim_product` | 88 | 100 | 188 | `(product_key)` |
| `fact_orders` | 375 | 15,000 | 15,375 | `(order_key)` |
| `fact_order_items` | 375 | 15,000 | 15,375 | `(order_item_key)` |
| `fact_returns` | 30 | 1,500 | 1,530 | `(return_key)` |

### Exact Cent-Level Monetary Reconciliation (`NUMERIC(18,2)`)

Bi-directional reconciliation (`make reconcile-warehouse` and `make test-warehouse-dq`) confirmed `0.00` discrepancy between pinned Iceberg `lh.gold.*` slices and PostgreSQL `warehouse.*` across all `(channel, currency)` buckets:

| Dataset Run | Channel \| Currency | `fact_orders.net_amount` | `fact_order_items.net_line_amount` | `fact_returns.refund_amount` |
| --- | --- | ---: | ---: | ---: |
| `186ae107d0793d2ee237` | `amazon \| USD` | 20,379.33 | 20,379.33 | 1,178.74 |
| `186ae107d0793d2ee237` | `lazada \| VND` | 472,120,000.00 | 472,120,000.00 | 35,250,750.00 |
| `186ae107d0793d2ee237` | `shopee \| USD` | 17,496.90 | 17,496.90 | 893.68 |
| `186ae107d0793d2ee237` | `tiktok_shop \| USD` | 21,625.30 | 21,625.30 | 840.28 |
| `186ae107d0793d2ee237` | `walmart \| USD` | 19,025.93 | 19,025.93 | 1,183.01 |
| `b1e64afaff0176de180a` | `amazon \| USD` | 750,463.24 | 750,463.24 | 75,185.52 |
| `b1e64afaff0176de180a` | `lazada \| VND` | 18,890,249,250.00 | 18,890,249,250.00 | 1,985,094,000.00 |
| `b1e64afaff0176de180a` | `shopee \| USD` | 740,954.86 | 740,954.86 | 75,255.59 |
| `b1e64afaff0176de180a` | `tiktok_shop \| USD` | 748,669.24 | 748,669.24 | 72,616.35 |
| `b1e64afaff0176de180a` | `walmart \| USD` | 758,926.04 | 758,926.04 | 78,478.17 |

## Sliding-Window Pruning, Transactional Rollback & Regression Evidence

1. **Sliding-Window Pruning (`make test-warehouse-recovery`):**
   - Exporting small fixture (`186ae107d0793d2ee237`) at `run_date = 2026-01-17` produced `375` orders, `375` items, `30` returns.
   - Advancing `run_date = 2026-02-01` (window `[2026-01-03T00:00:00Z, 2026-02-02T00:00:00Z)`) pruned all `2026-01-01..2026-01-02` orders (`0` stale rows remaining in PostgreSQL), leaving `125` orders, `125` items, and `10` returns while reconciling 100% against Gold and preserving `b1e64afaff0176de180a` intact.
2. **Three-Point JDBC Transaction Rollback:**
   - Fault injections at `fail_after="dimensions"`, `fail_after="facts"`, and `fail_after="completion"` raised `WarehouseFaultInjectionError`, recorded `attempt.json` `status = "failed"`, executed `conn.rollback()` leaving prior committed PostgreSQL row counts intact (`125` orders / `10` returns), and preserved `/runtime/warehouse/export/ledger.json` byte-for-byte.
   - Re-running at `run_date = 2026-01-17` cleanly restored `375` orders, `375` items, `30` returns.
3. **Referential Integrity, PII & `trino_ro` Access Control (`make test-warehouse-dq`):**
   - Passed all `10` orphan foreign-key checks (`0` orphans), zero prohibited PII columns, valid `NUMERIC(18,2)` schemas across monetary columns, and live `trino_ro` `SELECT` success / `DELETE` denial.
4. **Restart Persistence & Full Regression:**
   - Executed `make restart-core` and `make restart-lakehouse`, followed by `make verify-warehouse`, `make reconcile-warehouse`, `make smoke-core`, `make verify-sources`, `make verify-lakehouse`, `make smoke-bronze`, `make verify-silver`, `make verify-gold`, `make verify-features`, `make verify-labels`, and `make check`. All checks and unit tests passed.

## Review corrections — 2026-11-16

See [review validation](review-4cde-validation.md) for overflow, horizon endpoint,
transactional completion and full-row reconciliation fixes and their local evidence.
