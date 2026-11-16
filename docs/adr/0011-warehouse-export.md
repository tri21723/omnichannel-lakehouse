# ADR-0011: Downstream Data Warehouse Export (`dp4_warehouse`)

- Status: Accepted for implementation (Phase 4E).
- Context: Phase 4C delivered the Kimball dimensional star schema (`lh.gold.{dim_*,fact_*}`)
  in Apache Iceberg, and Phase 4D added ML feature/label tables (`dp3_*`). Phase 4E delivers
  `dp4_warehouse` (owned by `dp4` per ADR-0003) to synchronize a 30-day operational slice of
  Gold into PostgreSQL database `sources`, schema `warehouse.*` for relational BI serving
  and Phase 6 Trino federated queries.

## 1. Ownership Boundaries and Publication Pinning

- Per ADR-0003, `dp2` owns the 8 Iceberg tables `lh.gold.{dim_date,dim_channel,dim_customer,
  dim_product,dim_seller,fact_orders,fact_order_items,fact_returns}` and
  `/runtime/gold/publication/ledger.json`.
- `dp4_warehouse` consumes Gold strictly read-only from a completed Gold publication:
  - `/runtime/gold/publication/ledger.json` must have `status == "completed"` and pass SHA-256
    integrity verification against its content-addressed archive `publication-<sha256>.json`.
  - All 8 Gold tables are loaded with explicit Iceberg `option("snapshot-id", ...)` pinning
    recorded in the ledger, never `latest`. Expired or missing snapshots fail closed.
  - Rows are strictly filtered to `_dataset_run_id == args.run_id`.
- `dp4_warehouse` never writes back to Apache Iceberg or modifies `/runtime/gold/publication`
  or `/runtime/dp3/publication`.
- Export state is tracked independently under `/runtime/warehouse/export/`:
  - `attempt.json`: Atomic pre-execution marker (`running` / `failed` / `completed`).
  - `export-<sha256>.json`: Immutable content-addressed export manifest.
  - `ledger.json`: Recoverable file mirror of the committed PostgreSQL export state with `runs[<run_id>]` recording
    `gold_publication_id`, `run_date`, `window_start`, `window_end`, per-table row counts,
    and monetary control totals (`NUMERIC(18,2)` grouped by `(channel, currency)`).

## 2. 30-Day Sliding Window Definition

For a logical run date `run_date` (`DATE` in UTC; defaulting to the maximum active order/return
date for `_dataset_run_id` in Gold or supplied via `--run-date YYYY-MM-DD`), the export
sliding window is the half-open UTC interval:

$$[W_{\text{start}}, W_{\text{end}}) = [\text{run\_date} - 29\text{ days } 00:00:00\text{ UTC},\; \text{run\_date} + 1\text{ day } 00:00:00\text{ UTC})$$

Per-table synchronization rules for the active `_dataset_run_id`:
1. **Dimensions (`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`):**
   Synchronize all rows belonging to `_dataset_run_id` from the pinned Gold snapshots so
   every dimension key referenced by facts (including `first_order_ts`, `order_date_key`,
   and `return_date_key`) resolves cleanly against physical PostgreSQL foreign keys.
2. **`fact_orders`:** Include rows of `_dataset_run_id` where
   `order_ts >= W_start AND order_ts < W_end`.
3. **`fact_order_items`:** Include rows of `_dataset_run_id` where
   `order_ts >= W_start AND order_ts < W_end` and whose `order_key` exists in sliced
   `fact_orders`.
4. **`fact_returns`:** Include rows of `_dataset_run_id` where
   `return_ts >= W_start AND return_ts < W_end` and whose `(order_key, order_item_key)`
   belong to the sliced `fact_orders` and `fact_order_items`. Requiring parent order
   membership prevents orphan foreign keys when an older order slides out of the 30-day
   window while a late return remains within `[W_start, W_end)`.

When `run_date` advances on subsequent schedules, any existing PostgreSQL rows for
`_dataset_run_id` that slide outside `[W_start, W_end)` (or were retracted upstream in Gold)
are deleted within the same database transaction.

## 3. Physical PostgreSQL DDL and Multi-Dataset Coexistence

All 8 star-schema tables reside in database `sources`, schema `warehouse` (owned by role
`spark_writer`, DDL managed idempotently via `sql/postgres/003_warehouse.sql`):

- **Multi-dataset coexistence:** In Gold Iceberg, surrogate keys for `dim_customer`,
  `dim_product`, `dim_seller`, `fact_orders`, `fact_order_items`, and `fact_returns` already
  hash `_dataset_run_id`. However, `date_key` (`YYYYMMDD`) and `channel_key` are shared across
  runs. Therefore, physical primary keys on `warehouse.dim_date` and `warehouse.dim_channel`
  are defined as composite keys `(_dataset_run_id, date_key)` and
  `(_dataset_run_id, channel_key)`.
- **Referential integrity:** Every fact-to-dimension and return-to-order/item relationship is
  enforced via explicit PostgreSQL `FOREIGN KEY` constraints (using composite FKs for
  `dim_date` and `dim_channel`).
- **Monetary & PII guards:** All money columns (`gross_amount`, `discount_amount`,
  `net_amount`, `unit_price`, `discount`, `gross_line_amount`, `net_line_amount`,
  `refund_amount`) use `NUMERIC(18, 2)` with `CHECK (<col> >= 0)`, preserving original
  `currency` (`USD`, `VND`). `warehouse.dim_customer` stores only salted HMAC hashes
  (`name_hash`, `email_hash`, `_pii_key_version`), never clear PII.
- **Query indexing & read-only access:** Indexes cover `(_dataset_run_id, order_ts)`,
  `(_dataset_run_id, return_ts)`, and `(channel, currency)`. Schema default privileges
  created by `docker/core/grants.psql` automatically grant `SELECT` on all tables owned by
  `spark_writer` in `warehouse` to `trino_ro`.

## 4. Single-Connection Transactional JDBC Write & Least-Privilege

- **Credential isolation:** Only the `spark-submit` driver container receives
  `SPARK_WRITER_DB_PASSWORD` and `WAREHOUSE_JDBC_URL` (`jdbc:postgresql://postgres:5432/sources`).
  Spark worker containers (`spark-worker1`, `spark-worker2`) never hold database write
  credentials.
- **Single-transaction atomicity:** Distributed `DataFrame.write.jdbc(...)` opens independent
  connections and transactions per partition/table, risking torn partial state if a later
  table fails. Instead, after Spark evaluates, slices, and reconciles the 8 pinned DataFrames,
  the driver collects bounded slice rows and opens one JVM PostgreSQL JDBC connection as role
  `spark_writer` with `setAutoCommit(False)` and `SET LOCAL TIME ZONE 'UTC'`:
  1. Ensure idempotent schema/table existence and verify schema alignment (`sql/postgres/003_warehouse.sql`).
  2. Delete prior rows scoped to `WHERE _dataset_run_id = ?` in reverse foreign-key order:
     `fact_returns` -> `fact_order_items` -> `fact_orders` -> `dim_product` ->
     `dim_seller` -> `dim_customer` -> `dim_channel` -> `dim_date`.
  3. Insert the new slice using batched `PreparedStatement` calls in forward foreign-key order:
     `dim_date`, `dim_channel`, `dim_customer`, `dim_seller`, `dim_product` ->
     `fact_orders` -> `fact_order_items` -> `fact_returns`, preserving exact `java.math.BigDecimal`
     for `NUMERIC(18,2)` and UTC `java.sql.Timestamp` / `java.sql.Date`.
  4. Verify in-transaction row counts before calling `conn.commit()`. Any exception (or test
     fault injection before commit) triggers `conn.rollback()`, leaving prior committed rows in
     `warehouse.*` untouched and preserving `/runtime/warehouse/export/ledger.json` byte-for-byte.

## 5. Idempotency and Reconciliation

- Re-running `dp4_warehouse` with an identical `(_dataset_run_id, gold_publication_id, run_date)`
  already marked `completed` performs a read-back verification against PostgreSQL `warehouse.*`
  and skips rewriting (`skipped=True`) when row counts and monetary totals match.
- Exact reconciliation compares PostgreSQL `warehouse.*` against the pinned Gold Iceberg
  window slice for every `_dataset_run_id`: row counts per table, zero orphan foreign keys,
  zero clear PII, and exact cent-level (`0.00`) sums per `(channel, currency)`.

## Review amendment — 2026-11-16

warehouse.export_state (singleton JSONB ledger) is the authoritative completion and
is written inside the data transaction, after full-row fingerprint and monetary
read-back. File manifests are derived mirrors, recoverable after a post-commit crash.
Read-only CLI verification loads database completion first. Legacy file-only entries
are reconciled exactly against their pinned Gold input; subsequent writes migrate
metadata without dropping/recreating data tables. The existing POSIX writer lock
still serializes local exports; distributed multi-writer operation is not claimed.

Recovery also tolerates malformed file mirrors: the writer queries committed database
metadata before parsing any file. With no database completion, invalid legacy JSON
still fails closed; database errors never fall back to potentially stale files.
