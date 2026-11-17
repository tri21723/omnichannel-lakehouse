# ADR-0009: Gold star schema, surrogate keys and publication contract

- Status: Accepted for implementation (Phase 4C).
- Context: Phase 4B completed the normalized, deduplicated, and privacy-protected Silver
  layer for five e-commerce channels. Phase 4C constructs the dimensional Gold layer
  (Kimball star schema) on Apache Iceberg (`lh.gold`) owned by `dp2` per ADR-0003.

## Source selection and publication pinning

Gold strictly consumes from an immutable completed Silver publication ledger
(`/runtime/silver/publication/ledger.json` and its immutable content-addressed archive
`publication-<sha256>.json`).
- Gold requires `status == "completed"` and verifies the archive digest
  (`hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest()`).
- All five Silver entity tables (`orders`, `order_items`, `customers`, `products`, `returns`)
  are loaded using explicit Iceberg `snapshot-id` options from the ledger evidence.
- Never fall back to `latest` snapshots or attempt files. Expired or missing snapshots fail closed.
- Multi-dataset isolation: multiple dataset runs (such as the small fixture `186ae107d0793d2ee237`
  and default reference `b1e64afaff0176de180a`) intentionally coexist in the same Gold tables.
  Gold jobs require an explicit `--run-id` and filter all Silver inputs on `_dataset_run_id == args.run_id`.

## Dimensional model and surrogate keys

Gold consists of 8 tables in namespace `lh.gold`:
- Dimensions: `dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`.
- Facts: `fact_orders`, `fact_order_items`, `fact_returns`.

### Surrogate key strategy
All surrogate keys are deterministic, reproducible across runs, and channel/run-scoped:
- `customer_key = sha256(concat_ws(':', _dataset_run_id, channel, customer_id))`
- `product_key = sha256(concat_ws(':', _dataset_run_id, channel, product_id))`
- `seller_key = sha256(concat_ws(':', _dataset_run_id, channel, seller_id))`
- `channel_key = sha256(concat_ws(':', 'channel', channel))`
- `order_key = sha256(concat_ws(':', _dataset_run_id, channel, order_id))`
- `order_item_key = sha256(concat_ws(':', _dataset_run_id, channel, order_id, cast(line_no as string)))`
- `return_key = sha256(concat_ws(':', _dataset_run_id, channel, return_id, cast(return_line_no as string)))`
- `date_key = cast(date_format(calendar_date, 'yyyyMMdd') as int)` (UTC calendar date)

Surrogate keys are 64-character lowercase hex strings (except `date_key` which is an integer `YYYYMMDD`).
This avoids integer sequence collisions in distributed Spark writes and ensures that natural keys
shared across different channels (or runs) never collide.

### Dimension specifications
1. `dim_date`: Calendar dimension covering every distinct UTC date present in `order_ts`,
   `first_order_ts`, and `return_ts` (accounting for returns occurring after the order date).
   Attributes: `date_key`, `calendar_date`, `year`, `quarter`, `month`, `day_of_month`,
   `day_of_week` (1=Monday .. 7=Sunday ISO), `is_weekend`.
2. `dim_channel`: 5 standard channels (`walmart`, `amazon`, `tiktok_shop`, `shopee`, `lazada`)
   with attributes `channel_key`, `channel`, `channel_name`, `ingestion_mode`.
3. `dim_customer`: Current-profile dimension derived from `silver.customers`. Carries `customer_key`,
   `channel`, `customer_id`, `name_hash`, `email_hash`, `country`, `first_order_ts`, `updated_ts`,
   `_pii_key_version`. No raw `full_name` or `email` is ever stored in Gold.
4. `dim_product`: Product profile derived from `silver.products`. Carries `product_key`, `seller_key`,
   `channel`, `product_id`, `seller_id`, `title`, `category`, `brand` (nullable), `updated_ts`.
5. `dim_seller`: Derived ID-only dimension from valid `(channel, seller_id)` combinations across
   `silver.products` and `silver.order_items`. Attributes: `seller_key`, `channel`, `seller_id`.
   Per ADR-0003 and the Gold handoff, no synthetic seller name, address, or country is invented.

## Fact tables and measures

1. `fact_orders`:
   - Grain: One row per `(channel, order_id)`.
   - Keys: `order_key`, `order_date_key`, `channel_key`, `customer_key`.
   - Measures/Attributes: `gross_amount`, `discount_amount`, `net_amount` (`DECIMAL(18,2)`),
     `item_count`, `total_qty` (`INT`), `status`, `currency`, `shipping_country`, `is_deleted`,
     `fulfillment_type` (nullable).
   - Rule: `net_amount` equals `total_amount` from Silver, and equals the exact sum of line net amounts.
2. `fact_order_items`:
   - Grain: One row per `(channel, order_id, line_no)`.
   - Keys: `order_item_key`, `order_key`, `order_date_key`, `channel_key`, `customer_key`,
     `product_key`, `seller_key`.
   - Measures/Attributes: `qty`, `unit_price`, `discount`, `gross_line_amount` (`qty * unit_price`),
     `net_line_amount` (`qty * unit_price - discount`), `status`, `currency`.
3. `fact_returns`:
   - Grain: One row per `(channel, return_id, return_line_no)`.
   - Keys: `return_key`, `order_key`, `order_item_key`, `order_date_key`, `return_date_key`,
     `channel_key`, `customer_key`, `product_key`, `seller_key`.
   - Measures/Attributes: `refund_amount` (`DECIMAL(18,2)`), `reason`, `return_ts`, `currency`.
   - Rule: Cumulative refund amount per line cannot exceed `net_line_amount`. Returns are selected
     by order line membership; `return_ts >= order_ts`. Return quantity is N/A (not provided by sources).

## Chasm trap and multi-currency handling

- Order facts and return facts are modeled as separate fact tables. Downstream BI queries
  aggregating orders and returns must aggregate each fact table to the desired grain before
  joining to prevent Cartesian multiplication (fan-out / chasm trap).
- Monetary amounts are strictly `DECIMAL(18,2)`.
- No FX rate conversion is assumed; financial aggregates must always group by `(channel, currency)`.

## Partitioning and storage

All tables are Iceberg v2 format with ZSTD-compressed Parquet.
- `fact_orders`: `PARTITIONED BY (DAYS(order_ts))`
- `fact_order_items`: `PARTITIONED BY (DAYS(order_ts))`
- `fact_returns`: `PARTITIONED BY (DAYS(return_ts))`
- Dimensions (`dim_date`, `dim_channel`, `dim_customer`, `dim_product`, `dim_seller`): Unpartitioned.

## Publication ledger, idempotency and recovery

Gold publication is controlled by a POSIX file lock and managed under `/runtime/gold/publication/`:
- `attempt.json`: Records running/failed publication attempts.
- `publication-<id>.json`: Immutable archived snapshot inventory.
- `ledger.json`: Atomic completed state pointing to valid snapshots of all 8 Gold tables.
- Updates use `MERGE INTO` scoped by `_dataset_run_id`:
  - When recomputing a run (or applying Silver corrections/retractions), all prior rows of that
    `_dataset_run_id` in Gold are replaced with the newly computed dataset state.
  - Rows belonging to other dataset runs are preserved intact.
  - Re-running with the same Silver publication is idempotent and can be safely skipped if
    read-back verification passes.
- Verification prior to ledger publication confirms schema, surrogate key uniqueness, FK integrity,
  monetary reconciliation, and PII protection.

Count/quantity narrowing amendment (2026-11-17): Gold policy v2 checks item_count
and total_qty before narrowing aggregates to INT, rejecting positive wraparound
as well as negative overflow. Gold skip now requires the matching policy_version;
old run scopes recompute when requested. Physical column types remain unchanged.
