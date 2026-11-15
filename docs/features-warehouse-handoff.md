# Features & Warehouse handoff — Phase 4D & 4E contracts

This document specifies the input contracts, schemas, point-in-time correctness, and operational rules for downstream processing phases:
- **Phase 4D:** Feature Store & Return Label Generation (`dp3_features`, `dp3_return_labels`).
- **Phase 4E:** Downstream Data Warehouse Export (`dp4_warehouse`).

---

## 1. Phase 4D: Feature Store & Return Labels (`dp3_features`)

### 1.1 Source Selection & Pinning
- **Source Namespace:** `lh.gold.*` (`dim_customer`, `dim_product`, `dim_seller`, `dim_channel`, `fact_orders`, `fact_order_items`, `fact_returns`).
- **Pinning Mechanism:** Read `/runtime/gold/publication/ledger.json`. Require `status == "completed"`.
- Readers must bind to explicit Iceberg snapshot IDs recorded in the Gold publication ledger (`tables[<table_fqdn>]["snapshot"]`). Never query `latest` dynamically.
- Filter strictly by `_dataset_run_id` to prevent cross-contamination between coexisting datasets.

### 1.2 Point-in-Time Correctness & Leakage Prevention
- **Cutoff Rule:** Any feature calculation as of logical timestamp $T_{\text{as\_of}}$ must satisfy:
  $$\text{order\_ts} < T_{\text{as\_of}}$$
- No post-cutoff updates, orders, or profile changes may be included in feature aggregations.
- Lineage columns (`_dataset_run_id`, `_gold_publication_id`, `_as_of_ts`, `_policy_version`) must be captured on every feature row.

### 1.3 Return Label Generation Contract (`dp3_return_labels`)
- **Observation Horizon:** Return window $W_{\text{return}}$ (standard: 14 days or 30 days).
- **Maturity Condition:** An order is mature for label generation if and only if:
  $$\text{order\_ts} + W_{\text{return}} \le T_{\text{observation\_end}}$$
  Immature orders (within the active return window) must be marked with `is_mature = false` and excluded from supervised model training sets to avoid false negatives.
- **Label Definition:**
  - Target: `is_returned \in \{0, 1\}`.
  - An order is labeled `is_returned = 1` if a matching record exists in `lh.gold.fact_returns` for that `order_key` (or `order_item_key` for item-level predictions).
  - Target: `refund_ratio = refund_amount / net_amount`.
- **Target Iceberg Tables:**
  - `lh.features.customer_features_daily`
  - `lh.features.product_features_daily`
  - `lh.features.order_return_labels`

---

## 2. Phase 4E: Data Warehouse Export (`dp4_warehouse`)

### 2.1 Scope & Architecture
- **Source:** `lh.gold` Iceberg tables (pinned from `/runtime/gold/publication/ledger.json`).
- **Target:** PostgreSQL analytics schema (`analytics.*`) serving reporting and BI layers.
- Per ADR-0003, `dp4_warehouse` performs read-only export from Gold; it never writes back to Iceberg.

### 2.2 30-Day Sliding Window
- For a scheduled run at `run_date` (UTC), the export window is defined as:
  $$[\text{run\_date} - 29\text{ days},\, \text{run\_date} + 1\text{ day}) \quad \text{UTC}$$
- All fact records (`fact_orders`, `fact_order_items`, `fact_returns`) with timestamps within this 30-day window are selected.
- Dimension tables (`dim_customer`, `dim_product`, `dim_seller`, `dim_channel`, `dim_date`) are synchronized fully or via high-watermark upsert.

### 2.3 Idempotency and Atomic Publication
- **Staging Table Strategy:** Export data is written to temporary staging tables (e.g., `analytics.stg_fact_orders`).
- **Atomic Transaction:** Inside a single PostgreSQL transaction (`BEGIN ... COMMIT`):
  1. Delete existing slice for the 30-day date range:
     ```sql
     DELETE FROM analytics.fact_orders
     WHERE order_ts >= :window_start AND order_ts < :window_end
       AND _dataset_run_id = :run_id;
     ```
  2. Insert staged records:
     ```sql
     INSERT INTO analytics.fact_orders SELECT * FROM analytics.stg_fact_orders;
     ```
- **Monetary Reconciliation:**
  - `sum(net_amount)` in PostgreSQL `analytics.fact_orders` must exactly equal `sum(net_amount)` in `lh.gold.fact_orders` for the exported window.
  - No precision loss or floating-point conversions (`NUMERIC(18,2)` in PostgreSQL).

---

## 3. Publication Ledgers & Downstream Artifacts

| Phase | Artifact Location | Status | Consumer |
| --- | --- | --- | --- |
| 4B (Silver) | `/runtime/silver/publication/ledger.json` | Completed | 4C (Gold) |
| 4C (Gold) | `/runtime/gold/publication/ledger.json` | Completed | 4D (Features), 4E (Warehouse) |
| 4D (Features) | `/runtime/features/publication/ledger.json` | Planned | ML Training & Scoring |
| 4E (Warehouse) | `/runtime/warehouse/export/ledger.json` | Planned | Metabase / Superset / BI |
