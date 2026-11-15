# Features & Warehouse handoff — Phase 4D & 4E contracts

This document specifies the input contracts, schemas, point-in-time correctness, and operational rules for downstream processing phases:
- **Phase 4D:** Feature Store & Return Label Generation (`dp3_features`, `dp3_return_labels`).
- **Phase 4E:** Downstream Data Warehouse Export (`dp4_warehouse`).

---

## 1. Phase 4D: Feature Store & Return Labels (`dp3_features`, `dp3_return_labels`)

### 1.1 Source Selection & Publication Ownership
- **Source Namespace:** `lh.gold.*` (`dim_customer`, `dim_product`, `dim_seller`, `dim_channel`, `dim_date`, `fact_orders`, `fact_order_items`, `fact_returns`).
- **Pinning Mechanism:** Read `/runtime/gold/publication/ledger.json`. Require `status == "completed"` and verify the content-addressed archive `publication-<sha256>.json`.
- Readers must bind to explicit Iceberg snapshot IDs recorded in the Gold publication ledger (`tables[<table_fqdn>]["snapshot"]`). Never query `latest` dynamically.
- Filter strictly by `_dataset_run_id` and `not is_deleted` to prevent cross-contamination between coexisting datasets.
- **Ownership Boundary (ADR-0003):** `dp2` owns the 8 dimensional/fact tables (`dim_*`, `fact_*`) and `/runtime/gold/publication/ledger.json`. `dp3` owns the 3 feature/label tables (`lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`, `lh.gold.lbl_order_return`) and records its own publication ledger in `/runtime/dp3/publication/ledger.json`.

### 1.2 Point-in-Time Correctness & Dual-Timestamp Leakage Prevention
- **Order Cutoff Rule:** Any order-based feature calculation as of logical timestamp $T_{\text{as\_of}}$ must satisfy:
  $$\text{order\_ts} < T_{\text{as\_of}}$$
- **Return Cutoff Rule:** Because customer returns occur $1..14$ days after `order_ts`, any return-based feature (`returned_orders_30d`, `return_count_30d`, `refund_amount_30d`, `return_rate_30d`) must satisfy **both**:
  $$\text{order\_ts} < T_{\text{as\_of}} \quad \land \quad \text{return\_ts} < T_{\text{as\_of}}$$
- **No Synthetic Return Quantity:** Per ADR-0009, `fact_returns` has no `return_qty` column. Product return metrics use return line count and refund amount (`DECIMAL(18,2)`), never an invented quantity.
- **Currency Preservation:** All monetary features (`spend_7d`, `spend_30d`, `aov_30d`, `net_revenue_7d`, `net_revenue_30d`, `refund_amount_30d`) preserve the channel's native `currency` (`USD` or `VND`) in `DECIMAL(18,2)`.
- Lineage columns (`_dataset_run_id`, `_gold_publication_id`, `_record_hash`, `_policy_version`) must be captured on every feature and label row.

### 1.3 Return Label Generation Contract (`dp3_return_labels`)
- **Observation Horizon:** Configurable return window $H$ (`--horizon-days`, default: `14` days).
- **Maturity Condition:** An order placed at `order_ts` is mature as of $T_{\text{as\_of}}$ if and only if:
  $$\text{order\_ts} + \text{INTERVAL } H \text{ DAYS} \le T_{\text{as\_of}}$$
- **Right-Censoring Rule (ADR-0003):**
  - When `is_mature = false`, label fields (`is_returned`, `first_return_ts`, `days_to_return`, `return_line_count`, `refund_amount`, `refund_ratio`) must be set to `NULL` (never `0`), preventing false-negative label leakage and positive-only selection bias.
  - When `is_mature = true`, only returns satisfying $\text{order\_ts} \le \text{return\_ts} \le \text{order\_ts} + \text{INTERVAL } H \text{ DAYS}$ and $\text{return\_ts} < T_{\text{as\_of}}$ are aggregated to the order grain (`order_key`) before joining `fact_orders`, yielding `is_returned \in \{0, 1\}`.
- **Target Iceberg Tables (in `lh.gold` per `AGENTS.md` §6 and `docs/data-model.md`):**
  - `lh.gold.feat_customer_daily` (`spark/jobs/dp3_features/main.py`)
  - `lh.gold.feat_product_daily` (`spark/jobs/dp3_features/main.py`)
  - `lh.gold.lbl_order_return` (`spark/jobs/dp3_return_labels/main.py`)

---

## 2. Phase 4E: Data Warehouse Export (`dp4_warehouse`)

### 2.1 Scope & Architecture
- **Source:** `lh.gold` Iceberg tables (pinned from `/runtime/gold/publication/ledger.json`).
- **Target:** PostgreSQL database `sources`, schema `warehouse.*` serving reporting and BI layers.
- Per ADR-0003, `dp4_warehouse` performs read-only export from Gold; it never writes back to Iceberg.

### 2.2 30-Day Sliding Window
- For a scheduled run at `run_date` (UTC), the export window is defined as:
  $$[\text{run\_date} - 29\text{ days},\, \text{run\_date} + 1\text{ day}) \quad \text{UTC}$$
- Fact records (`fact_orders`, `fact_order_items`, `fact_returns`) within this 30-day window are selected, and any rows outside the window for that run are removed atomically.
- Referenced dimension tables (`dim_customer`, `dim_product`, `dim_seller`, `dim_channel`, `dim_date`) are synchronized transactionally.

### 2.3 Idempotency and Atomic Publication
- **Transactional Replace:** Inside a single PostgreSQL transaction (`BEGIN ... COMMIT`), replace the target slice for the selected `_dataset_run_id` and enforce exact `NUMERIC(18,2)` reconciliation against `lh.gold.*`.

---

## 3. Publication Ledgers & Downstream Artifacts

| Phase | Artifact Location | Target Namespace / Schema | Consumer |
| --- | --- | --- | --- |
| 4B (Silver) | `/runtime/silver/publication/ledger.json` | `lh.silver.*` | 4C (`dp2_gold`) |
| 4C (Gold Star Schema) | `/runtime/gold/publication/ledger.json` | `lh.gold.{dim_*,fact_*}` | 4D (`dp3_*`), 4E (`dp4_warehouse`) |
| 4D (Features & Labels) | `/runtime/dp3/publication/ledger.json` | `lh.gold.{feat_*,lbl_*}` | ML Training & Trino |
| 4E (Warehouse Export) | `/runtime/warehouse/export/ledger.json` | PostgreSQL `warehouse.*` | DBeaver / BI |
