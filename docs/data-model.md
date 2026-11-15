# Data model v1

Channels: walmart, amazon, tiktok_shop, shopee, lazada.
IDs are strings, timestamps are UTC, amounts use decimal (never float).
Keep original currency; FX conversion remains deferred. See ADR-0003.

| Silver entity | Natural key | Important fields |
| --- | --- | --- |
| orders | channel, order_id | customer_id, order_ts, updated_ts, status, currency, total_amount, is_deleted |
| order_items | channel, order_id, line_no | product_id, seller_id, qty, unit_price, discount |
| customers | channel, customer_id | name_hash, email_hash, country, first_order_ts |
| products | channel, product_id | title, category, brand, seller_id |
| sellers (deferred) | channel, seller_id | no standalone source profile |
| returns | channel, return_id, return_line_no | order_id, line_no, return_ts, reason, refund_amount |

All joins include channel. Order item line_no is local to an order. Return line
numbers distinguish multiple items in one return. Cross-channel identity resolution
is outside this version. Synthetic PII is hashed at Silver; raw data stays restricted.

Bronze preserves payload and source schema with `_raw`, `_ingest_ts`, `_source_file`,
`_channel`, `_batch_id`. JDBC provenance needs a source-table identifier.
Bronze keeps intentional source duplicates; replaying a batch must not append it again.
Invalid records go to `silver.quarantine_*` with `_error_reason` and provenance.
Silver dedup uses updated_ts and canonical normalized SHA-256 as defined in ADR-0008. CDC ordering must use source position, not ingestion time.

Gold dimensions: dim_date, dim_channel, dim_customer, dim_product, dim_seller.
Gold facts: fact_orders (one row per channel/order), fact_order_items (per order line),
fact_returns (per return line). Surrogate keys use deterministic 64-char SHA-256 hex strings
(and integer YYYYMMDD for date_key). See ADR-0009 and sql/iceberg/gold_contract.sql.
Features: feat_customer_daily and feat_product_daily keyed by channel/entity/as_of_date.
Labels: lbl_order_return keyed by channel/order/observation horizon, with maturity flag.

Warehouse export covers [run_date - 29 days, run_date + 1 day) in UTC. It must publish
atomically and remove rows outside that interval. Re-running the same logical date
must preserve the same business result for the same source snapshot.

## Implemented OLTP contract (Phase 1)

`sql/postgres/001_walmart.sql` creates customers, products, orders, order_items and
returns. Walmart IDs are source-local primary keys; channel is added at ingestion.
Only orders is published to CDC initially. Sellers remain embedded seller_id values;
a standalone OLTP seller table is deferred until generator requirements justify it.
Five Silver entity tables are implemented; Gold remains proposed.

## Implemented generator payload (batch-v1)

Orders embed `customer`, `items[].product` and `returns[]`; each order has one line
in this version. Walmart load splits these into the existing five OLTP tables.
Amounts are strings with two decimal places in JSONL, NUMERIC(18,2) in PostgreSQL.
Amazon drift, Shopee timestamps and Lazada optional fields differ by channel.
See [generator contracts](generators.md) and ADR-0005 for dataset boundaries.

Phase 4A physical Bronze contract and source separation: see [Bronze contract](bronze-contract.md)
and [ADR-0007](adr/0007-bronze-ingestion.md).

## Implemented Silver contract (Phase 4B)

All five entities use channel-scoped keys; orders retain nullable fulfillment_type.
Customers contain name_hash/email_hash and _pii_key_version, never clear full_name/email.
All entity fields, audit types and partitions are in
[physical DDL](../sql/iceberg/silver_contract.sql). Audit identifies dataset, logical
date, channel, source table/snapshot/batch, pinned input, source record fingerprint,
child ordinal/parent, policy and normalized record hash.

The five private _contrib_* tables preserve active per-slice candidates; five
quarantine_* tables preserve rejected occurrence references with sorted reason codes.
Neither stores raw PII. Profile versions use source updated_ts; items/returns inherit
order updated_ts. Customer first_order_ts is the global minimum of active valid orders.
See [ADR-0008](adr/0008-silver-contract.md) for correction/replay/PII policies and
[Gold handoff](gold-handoff.md) for consistent snapshot consumption.

## Implemented Gold contract (Phase 4C)

Eight Iceberg tables in `lh.gold`: five dimensions (`dim_date`, `dim_channel`, `dim_customer`,
`dim_product`, `dim_seller`) and three facts (`fact_orders`, `fact_order_items`, `fact_returns`).
Surrogate keys are deterministic 64-char lowercase SHA-256 hex strings scoped by `_dataset_run_id`
and `channel`, ensuring multi-dataset isolation without cross-run collisions. `date_key` is an
integer `YYYYMMDD` in UTC covering all order, customer first order, and return dates.

`dim_seller` is an ID-only dimension derived from valid product and order item sellers, with no
invented attributes. `dim_customer` preserves HMAC-SHA256 privacy hashes (`name_hash`, `email_hash`).
All money amounts are `DECIMAL(18,2)` grouped by `(channel, currency)` with no cross-currency conversion.
Orders and returns are modeled as separate fact tables to prevent Cartesian fan-out on joins.
Audit columns record `_dataset_run_id`, `_silver_publication_id`, `_silver_snapshot`,
`_record_hash`, and `_policy_version`. See [ADR-0009](adr/0009-gold-star-schema.md) and
[physical DDL](../sql/iceberg/gold_contract.sql).
