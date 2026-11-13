# Phase 4B handoff: Bronze to Silver

Phase 4A supplies raw tables and completion records. Silver transforms are the next
implementation phase; none are claimed by this handoff.

## Select a completed input

Use an explicit dataset run ID and logical date. Read /runtime/bronze/<batch-id>.json
and require status=completed, the expected selection/source and matching batch ID.
New records include contract=bronze-v1, selection, attempt/timestamps, table snapshot
IDs, raw content summaries and metrics. Earlier records without selection are legacy;
re-run full ingestion to populate the metadata before relying on them for orchestration.
`make verify-bronze` remains compatible with legacy completion records.

Read each table using its recorded snapshot ID, then filter by run/channel/date/source
and batch ID. Check the snapshot still exists and content reconciles. Never infer a
multi-table transaction from the latest state of several tables. Failed/running attempts
are not consumable. A NULL snapshot is allowed only for an initially empty table with
zero recorded rows; construct an empty typed input instead of reading a latest snapshot.
Do not expire snapshots referenced by completion until a retention protocol exists.

## Authoritative inputs and mappings

| Channel/source | Bronze input | Silver preparation |
| --- | --- | --- |
| Walmart JDBC | walmart_orders_raw, walmart_order_items_raw, walmart_customers_raw, walmart_products_raw, walmart_returns_raw | Parse row JSON by table; join on channel and source keys |
| Walmart JSONL | walmart_orders_export_raw | Reconciliation/export replay only; exclude from authoritative Silver orders |
| Amazon JSONL | amazon_orders_raw | customer_id = coalesce(customer_id, buyer_id); retain fulfillment_type when present |
| TikTok Shop JSONL | tiktok_shop_orders_raw | Parse canonical embedded customer/items/product/returns |
| Shopee JSONL | shopee_orders_raw | Parse yyyy/MM/dd HH:mm:ssZ timestamps including +0700, convert to UTC |
| Lazada JSONL | lazada_orders_raw | Nullable product brand; preserve VND and decimal strings |

JSONL orders embed customer, items[].product and returns[]. JDBC splits them across
five tables. Customers/products repeat between logical-date JDBC slices by design;
returns are selected through order membership, even when return_ts is later than
run_date. Do not count these repetitions as new business entities.

## Required Silver behavior

- Preserve channel-scoped natural keys from data-model.md in all joins and dedup.
- Bronze retains source duplicates: default JSONL 3,120 rows/channel represents 3,000
  unique orders. Silver must implement and test deterministic updated_ts dedup with
  an explicit tie-break rule before publishing MERGE output.
- Parse amounts as DECIMAL(18,2), never float; preserve currency. No FX conversion or
  cross-currency totals are supported by the current dataset.
- _parse_status invalid/blank is a quarantine candidate. valid means JSON syntax only;
  scalar JSON, missing fields and impossible values still require schema/business DQ.
- Hash customer name/email before Silver publication. Choose/version normalization
  and hashing/key policy in the Silver ADR; keep raw/quarantine PII access restricted.
- Quarantine retains lineage and _error_reason, with reconciliation accounting for
  every selected Bronze row. Never silently discard malformed or unmapped payloads.
- Pin input run/date/snapshots. Replaying against changed latest Bronze tables must
  not silently change an already selected source snapshot.
- Synthetic returns can occur 1–14 days after order time. Feature/label as-of cutoffs
  and maturity belong to later jobs; Bronze completion does not mean labels matured.

Start Phase 4B with Walmart orders/customers/items, then expand to all entities and
channels. Gold dimensions/facts remain owned by dp2 (ADR-0003); dp4 exports existing
Gold to PostgreSQL. A detailed Phase 4B checklist should precede implementation.
