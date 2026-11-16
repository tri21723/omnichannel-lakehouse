# Feature Store & Return Labels validation — Phase 4D

Date: 2026-11-15. Local DoD complete; milestone `v0.4d-features`.
Feature Store (`lh.gold.feat_customer_daily`, `lh.gold.feat_product_daily`) and supervised Return Labels (`lh.gold.lbl_order_return`) implemented and verified on Iceberg v2.
See [ADR-0003](adr/0003-data-contracts.md), [ADR-0010](adr/0010-features-and-return-labels.md), [data model](data-model.md), and [Features & Warehouse handoff](features-warehouse-handoff.md).

## Runtime and Pinning Mechanics

- **Runtime Environment:** Spark 3.5.5, Python 3.11.11, Java 17, Iceberg 1.9.2, Lakekeeper 0.9.3 REST Catalog, MinIO S3 storage, PostgreSQL 16.
- **Pinning Inputs:** `dp3_features` and `dp3_return_labels` pin inputs strictly from the completed Gold publication ledger `/runtime/gold/publication/ledger.json` (`publication_id`: `46d63f72f5aba0aec78bc2601491a2bb5db092662feb551ea61fb16640acd37c`) via `option("snapshot-id", ...)` across the Gold dimensional and fact tables.
- **Isolated Ownership Boundary (ADR-0003):** `dp2_gold` owns the 8 star-schema tables (`dim_*`, `fact_*`) and `/runtime/gold/publication/ledger.json`. `dp3` reads those snapshots read-only and maintains its own atomic publication ledger at `/runtime/dp3/publication/ledger.json` (plus immutable `publication-<sha256>.json` archives).
- **Audit Lineage:** Every feature and label record carries `_dataset_run_id`, `_gold_publication_id`, `_record_hash`, and `_policy_version` (`dp3-policy-v1`).

## Schema, Grains & Point-in-Time Semantics

Three tables are published in `lh.gold` (matching `sql/iceberg/gold_contract.sql`, Iceberg v2, Parquet ZSTD, partitioned by `as_of_date`):

1. **`lh.gold.feat_customer_daily`**
   - **Grain:** `(_dataset_run_id, channel, customer_id, as_of_date)`
   - **Surrogate Key:** `customer_feature_key = sha256(_dataset_run_id:channel:customer_id:as_of_date)`
   - **Metrics:** Recency (`last_order_ts`, `days_since_last_order`), Frequency (`lifetime_order_count`, `orders_7d`, `orders_30d`), Monetary in native channel `currency` (`DECIMAL(18,2)`: `lifetime_net_amount`, `spend_7d`, `spend_30d`, `aov_30d`), and Return behavior (`returned_orders_30d`, `refund_amount_30d`, `return_rate_30d` in `DECIMAL(18,4)`).
2. **`lh.gold.feat_product_daily`**
   - **Grain:** `(_dataset_run_id, channel, product_id, as_of_date)`
   - **Surrogate Key:** `product_feature_key = sha256(_dataset_run_id:channel:product_id:as_of_date)`
   - **Metrics:** Velocity (`orders_7d`, `orders_30d`, `units_sold_7d`, `units_sold_30d`), Revenue in `DECIMAL(18,2)` (`gross_revenue_7d`, `gross_revenue_30d`, `net_revenue_7d`, `net_revenue_30d`, `discount_amount_30d`), and Return impact (`return_count_7d`, `return_count_30d`, `refund_amount_7d`, `refund_amount_30d`, `return_rate_30d`). Per ADR-0009/ADR-0010, no synthetic `return_qty` is invented.
3. **`lh.gold.lbl_order_return`**
   - **Grain:** `(_dataset_run_id, order_key, horizon_days, as_of_date)`
   - **Surrogate Key:** `order_label_key = sha256(_dataset_run_id:channel:order_id:horizon_days:as_of_date)`
   - **Metrics:** `is_mature` (`order_ts + INTERVAL horizon_days DAYS <= as_of_ts`), right-censored target `is_returned`, `first_return_ts`, `days_to_return`, `return_line_count`, `refund_amount` (`DECIMAL(18,2)`), and `refund_ratio` (`DECIMAL(18,4)`).

## Multi-Dataset Coexistence & Ledger Evidence

Both the small fixture (`186ae107d0793d2ee237`) and default reference (`b1e64afaff0176de180a`) datasets coexist in `lh.gold.{feat_customer_daily,feat_product_daily,lbl_order_return}` with zero cross-run key collisions:

| Table (`lh.gold.*`) | Small Fixture (`186ae...`) | Default Reference (`b1e64...`) | Total Rows | Iceberg Snapshot ID | Data Files / Bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `feat_customer_daily` | 75 (`25 × 3 dates`) | 3,000 (`1,000 × 3 dates`) | 3,075 | `4341835489947190054` | 3 files / 420,194 B |
| `feat_product_daily` | 264 (`88 × 3 dates`) | 300 (`100 × 3 dates`) | 564 | `994335655257263056` | 3 files / 131,644 B |
| `lbl_order_return` | 1,125 (`125+250+375+375`) | 45,000 (`5k+10k+15k+15k`) | 46,125 | `8047363412205820992` | 4 files / 5,738,267 B |

### Content Digest Summary (`/runtime/dp3/publication/ledger.json`)
- `lh.gold.feat_customer_daily`: `rows="3075"`, `h1="1783284790233587944615"`, `h2="1796282456736639860773"`
- `lh.gold.feat_product_daily`: `rows="564"`, `h1="323815635006345546855"`, `h2="332742025973171831263"`
- `lh.gold.lbl_order_return`: `rows="46125"`, `h1="26534300113471726892943"`, `h2="26528803225688629928638"`

## Point-in-Time Leakage & Maturity Right-Censoring Verification

Verified via `make test-dp3-dq` on both `186ae107d0793d2ee237` and `b1e64afaff0176de180a`:

1. **Dual-Timestamp Leakage Injection Test:**
   - Synthetic future orders (`order_ts = 2026-01-04 00:00:00 UTC >= as_of_ts`) and future returns (`return_ts = 2026-01-04 00:00:00 UTC >= as_of_ts` attached to existing orders) were injected into the Gold frames for evaluation date `2026-01-03`.
   - Recomputing `feat_customer_daily`, `feat_product_daily`, and `lbl_order_return` at `2026-01-03` produced zero symmetric row differences (`exceptAll == 0`) against the published `2026-01-03` tables.
2. **Maturity Censoring Boundary Test (`horizon_days = 14`):**
   - At `as_of_date = 2026-01-03`: 100% of orders (`375` in small fixture, `15,000` in default reference) have `is_mature = false` and `is_returned IS NULL` (along with `first_return_ts`, `days_to_return`, `return_line_count`, `refund_amount`, `refund_ratio` all `NULL`). Zero false-negative `0` labels exist on immature orders.
   - At `as_of_date = 2026-01-18` (post-14-day observation horizon): 100% of orders (`375` and `15,000`) have `is_mature = true` and `is_returned IN (0, 1)`. The count of mature returned orders (`is_returned = 1`) matches Gold `fact_returns` exactly: **`30` orders** for `186ae107d0793d2ee237` and **`1,500` orders** for `b1e64afaff0176de180a`.
3. **Point-in-Time Training Join Test:**
   - Joining mature labels (`lbl_order_return` at `2026-01-18`) with `feat_customer_daily` and `feat_product_daily` as of the order date (`DATE(order_ts) == feat.as_of_date`) confirmed `order_ts < feat.as_of_ts` across all joined training rows with zero future leakage.

## Fault Injection Recovery & Restart Persistence

- **Recovery (`make test-dp3-recovery`):** Tested three fault injection points (`fail_after="features"`, `fail_after="labels"`, `fail_after="completion"`). In all three cases, `attempt.json` transitioned to `failed`, the prior `/runtime/dp3/publication/ledger.json` was preserved byte-for-byte, and a subsequent clean run recovered atomically to `status = "completed"`.
- **Restart & Full Regression:** Executed `make restart-lakehouse` followed by read-only verifiers (`make verify-features`, `make verify-labels`) and full regression (`make smoke-core`, `make verify-sources`, `make verify-lakehouse`, `make smoke-bronze`, `make verify-silver`, `make verify-gold`). All checks and 138 unit tests in `make check` passed.

## Review corrections — 2026-11-16

See [review validation](review-4cde-validation.md) for overflow, horizon endpoint,
transactional completion and full-row reconciliation fixes and their local evidence.
