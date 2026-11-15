# Gold validation — Phase 4C

Date: 2026-11-15. Local DoD complete; milestone `v0.4c-gold`.
Gold dimensional star schema in `lh.gold` implemented and verified on Iceberg v2.
See ADR-0003, ADR-0008, `docs/data-model.md`, and `docs/gold-handoff.md`.

## Runtime and Pinning Mechanics

- **Runtime Environment:** Spark 3.5.5, Python 3.11.11, Java 17, Iceberg 1.9.2, Lakekeeper 0.9.3 REST Catalog, MinIO S3 storage, PostgreSQL 16. Local resource limits: two worker instances, 2-core / 2-GiB executor allocations.
- **Pinning Inputs:** Inputs are pinned strictly from completed Silver publication ledger `/runtime/silver/publication/ledger.json` by snapshot ID. Readers resolve exact snapshot IDs for each of the 5 Silver entities (`customers`, `orders`, `order_items`, `products`, `returns`).
- **Fail-Closed Semantics:** Missing or expired snapshots raise immediate fatal errors; there is no fallback to `latest`.
- **Audit Lineage:** Every Gold dimension and fact table carries standard audit lineage columns:
  - `_dataset_run_id`: Run identifier isolating datasets.
  - `_silver_publication_id`: SHA-256 fingerprint of the source Silver publication.
  - `_silver_snapshot`: Source Iceberg snapshot ID from which the row was materialized (`dim_customer`, `dim_product`, `fact_orders`, `fact_order_items`, `fact_returns`).
  - `_record_hash`: Deterministic SHA-256 hash of canonical business fields.
  - `_policy_version`: Policy version string (`gold-policy-v1`).

## Schema & Grains

Eight Gold tables are published in namespace `lh.gold` (matching `sql/iceberg/gold_contract.sql`):

### 1. Dimensions (5)
- `dim_date`: Calendar date lookup table (`date_key`, `calendar_date`, `year`, `quarter`, `month`, `day_of_month`, `day_of_week`, `is_weekend`, `_dataset_run_id`).
- `dim_channel`: Omnichannel sales channel lookup (`channel_key`, `channel`, `channel_name`, `ingestion_mode`, `_dataset_run_id`).
- `dim_customer`: Customer profile dimension (`customer_key`, `channel`, `customer_id`, `name_hash`, `email_hash`, `country`, `first_order_ts`, `updated_ts`, `_pii_key_version`, `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`, `_record_hash`, `_policy_version`).
- `dim_product`: Product catalog dimension (`product_key`, `seller_key`, `channel`, `product_id`, `seller_id`, `title`, `category`, `brand`, `updated_ts`, `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`, `_record_hash`, `_policy_version`).
- `dim_seller`: Derived ID-only seller dimension (`seller_key`, `channel`, `seller_id`, `_dataset_run_id`, `_silver_publication_id`, `_record_hash`, `_policy_version`).

### 2. Facts (3)
- `fact_orders`: Grain: 1 row per sales order (`order_key`, `order_date_key`, `channel_key`, `customer_key`, `channel`, `order_id`, `customer_id`, `order_ts`, `updated_ts`, `status`, `currency`, `gross_amount`, `discount_amount`, `net_amount`, `item_count`, `total_qty`, `shipping_country`, `is_deleted`, `fulfillment_type`, `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`, `_record_hash`, `_policy_version`). Partitioned by `days(order_ts)`.
- `fact_order_items`: Grain: 1 row per order line item (`order_item_key`, `order_key`, `order_date_key`, `channel_key`, `customer_key`, `product_key`, `seller_key`, `channel`, `order_id`, `line_no`, `product_id`, `seller_id`, `order_ts`, `updated_ts`, `status`, `currency`, `qty`, `unit_price`, `discount`, `gross_line_amount`, `net_line_amount`, `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`, `_record_hash`, `_policy_version`). Partitioned by `days(order_ts)`.
- `fact_returns`: Grain: 1 row per customer return line (`return_key`, `order_key`, `order_item_key`, `order_date_key`, `return_date_key`, `channel_key`, `customer_key`, `product_key`, `seller_key`, `channel`, `return_id`, `return_line_no`, `order_id`, `line_no`, `reason`, `return_ts`, `updated_ts`, `currency`, `refund_amount`, `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`, `_record_hash`, `_policy_version`). Partitioned by `days(return_ts)`.

## Multi-Dataset Coexistence

Both the small fixture (`186ae107d0793d2ee237`) and default reference (`b1e64afaff0176de180a`) datasets coexist cleanly within the same Iceberg tables in `lh.gold.*`:

| Gold Table | Small Fixture (`186ae...`) | Default Reference (`b1e64...`) | Total In `lh.gold.*` | Primary Key Column |
| --- | ---: | ---: | ---: | --- |
| `dim_date` | 15 | 17 | 32 | `date_key` |
| `dim_channel` | 5 | 5 | 10 | `channel_key` |
| `dim_customer` | 25 | 1,000 | 1,025 | `customer_key` |
| `dim_product` | 88 | 100 | 188 | `product_key` |
| `dim_seller` | 88 | 100 | 188 | `seller_key` |
| `fact_orders` | 375 | 15,000 | 15,375 | `order_key` |
| `fact_order_items` | 375 | 15,000 | 15,375 | `order_item_key` |
| `fact_returns` | 30 | 1,500 | 1,530 | `return_key` |

### Surrogate Key Hashing and Collision Prevention
Surrogate keys for entity tables are computed via SHA-256 salting with `_dataset_run_id` and natural business keys (e.g., `sha2(concat_ws(':', _dataset_run_id, channel, customer_id), 256)`). Calendar dates and channel dimensions use standardized deterministic keys. Cross-run surrogate key collisions are verified to be zero (`_runs == 1` across all entity dimensions and facts).

## Exact Monetary Reconciliation

All monetary calculations strictly use `DECIMAL(18,2)` without floating-point casts:
1. **Line-Item Arithmetic:**
   - `gross_line_amount = (qty * unit_price).cast("decimal(18,2)")`
   - `net_line_amount = (qty * unit_price - discount).cast("decimal(18,2)")`
2. **Order-to-Item Reconciliation:**
   - `sum(fact_order_items.net_line_amount) == fact_orders.net_amount` for every order.
   - `sum(fact_order_items.gross_line_amount) == fact_orders.gross_amount`.
   - `sum(fact_order_items.discount) == fact_orders.discount_amount`.
   - `sum(fact_order_items.qty) == fact_orders.item_count`.
3. **Silver-to-Gold Three-Way Reconciliation:**
   - Grouped by `(channel, currency)`, `sum(fact_orders.net_amount) == sum(silver.orders.total_amount)`.
   - Zero discrepancy across all channels (`walmart`, `amazon`, `shopify`, `lazada`, `shopee`) and currencies (`USD`, `VND`).
4. **Return Ceilings:**
   - `fact_returns.refund_amount <= fact_orders.net_amount` for all returned orders.

## PII Isolation & Privacy Protection

- Zero cleartext names or email addresses enter Gold schemas, logs, or publication ledgers.
- `dim_customer` contains only SHA-256 HMAC hashes (`name_hash`, `email_hash`).
- All customer surrogate keys are opaque 64-character SHA-256 hex strings.

## Fault Injection and Recovery

Verified via automated test target `make test-gold-recovery`:
1. **Failure Injection After Dimensions (`fail_after="dimensions"`):**
   - Injected runtime failure interrupts execution immediately after committing dimension tables.
   - `attempt.json` records `{"status": "failed", "error": "Injected dimensions failure"}`.
   - `ledger.json` is preserved completely unmodified.
2. **Failure Injection After Facts (`fail_after="facts"`):**
   - Injected runtime failure interrupts execution immediately after committing `fact_order_items`.
   - `attempt.json` records `{"status": "failed", "error": "Injected facts failure"}`.
   - `ledger.json` is preserved completely unmodified.
3. **Atomic Recovery:**
   - Full publication is re-triggered without errors.
   - All 8 tables are verified, `attempt.json` status becomes `completed`, and `ledger.json` is updated atomically.

## Verification Targets

- `make check`: 110/110 unit tests passing, clean Ruff/Black/Yamllint/Sqlfluff.
- `make verify-gold`: Read-back verification across all 8 tables and metadata summaries.
- `make test-gold-dq GOLD_RUN_ID=<run_id>`: Surrogate key hashes, item-to-order rollups, return bounds, multi-currency segregation.
- `make reconcile-gold GOLD_RUN_ID=<run_id>`: Exact three-way reconciliation against Silver and Bronze totals.
- `make test-gold-recovery`: Failure injection and atomic recovery suite.
