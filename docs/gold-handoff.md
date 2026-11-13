# Gold handoff — Phase 4C input contract

Silver is implemented for five channels and five entities. Gold dimensions/facts are
next; features/labels and warehouse export remain separate phases. Per ADR-0003,
dp2 owns Silver plus Gold dim/fact; dp3 owns features/labels; dp4 only exports Gold.

## Select one completed publication

Read /runtime/silver/publication/ledger.json once, require status=completed and pin
its immutable publication archive before processing. Resolve each of the five
lh.silver.<entity> tables from that same ledger's tables map and read Iceberg with
option snapshot-id. A NULL snapshot means typed empty data only when content rows=0.
Archive identity is SHA-256 of json.dumps(ledger, sort_keys=True).encode() using
Python's default JSON separators/escaping, as in silver_publish.py; the archive is
publication-<identity>.json. Hashing the pretty-printed ledger bytes yields a different
identity. Never fall back to latest, including after failure/restart or snapshot expiration.
Do not treat attempt.json as completion. Keep referenced snapshots until retention
and downstream replay requirements have been designed.

Choose dataset run explicitly using _dataset_run_id. Both the small fixture and
default reference intentionally coexist for the same three logical dates. Gold
should not silently combine independent datasets. A natural key cannot be claimed
by two runs; joins always include channel. Current default run:
b1e64afaff0176de180a. Small run: 186ae107d0793d2ee237.

## Grains and keys

| Table | Grain / natural key after channel | Relationships / use |
| --- | --- | --- |
| orders | one order_id | customer_id → customers; fact_orders |
| order_items | one order_id, line_no | order → orders, product_id → products; fact_order_items |
| customers | one customer_id, current profile | dim_customer; first_order_ts is minimum across active orders |
| products | one product_id, current profile | dim_product; seller_id retained |
| returns | one return_id, return_line_no | order_id/line_no → items; fact_returns |

Opaque IDs are STRING, timestamps UTC, monetary amounts DECIMAL(18,2). Item/return
updated_ts inherits the parent order version; customer/product has its own version.
Profile tables represent latest current state, not SCD2 or historical as-of state.
Sellers is deferred: derive an ID-only dimension if explicitly designed, never invent
seller_name/country. Choose surrogate key and unknown-member policies in Gold ADR/DDL.

## Money, dates and privacy

Preserve currency from orders. Sum totals/refunds only within channel/currency; no
FX rate exists. Line net = qty * unit_price - discount, where discount is for the
whole line. Orders equal the exact sum of valid lines. Unique return lines have a
cumulative refund capped at paid line value. Returns may occur after run_date, are
selected by order membership and carry their actual UTC return_ts. No source return
quantity exists. Avoid multiplication when joining order and return facts; aggregate
at the intended grain before joining.

Customers expose name_hash, email_hash and _pii_key_version, not full_name/email.
HMAC identity is domain separated by channel/field; it does not imply cross-channel
customer identity. Keep the local key stable; key/policy migration requires reviewed
complete backfill. Gold must preserve the same privacy boundary and avoid raw Bronze
payloads or customer hashes in logs.

## Corrections, quarantine and accounting

Read only completed materialized snapshots. Private _contrib_* tables are internal
Silver state; quarantine_* has source references/reasons and no raw PII. A changed
input replaces one contribution slice, including rows that become invalid or absent.
Shared profile contributions from other dates survive. Missing source rows do not
mean CDC deletion: is_deleted remains false. Gold must support recomputation from a
new completed Silver publication without appending duplicate keys.

Ledger slices contain selection, source input snapshots, policy/key version and
revision. Ledger tables contain output snapshots, hashes, row and file/byte counts.
Accounting is per input occurrence, separately per exploded entity:
input = valid + rejected; valid = local winners + duplicate losers. These are not
global distinct row counts, especially for repeated embedded profiles. Capture the
Silver publication identity and selected run in every Gold completion.

## Reproducible baseline

For the default run: orders=15,000; order_items=15,000; customers=1,000;
products=100; returns=1,500. Small run: 375/375/25/88/30 respectively. These values
come from source key comparisons, not an assumption of one profile per order.
Run make verify-silver and make reconcile-silver with the selected SILVER_RUN_ID
before developing Gold. See silver-validation.md for local evidence and CI limits.

Scope limitations: serialized local writer and full-table contribution reduction;
no distributed concurrency guarantee or large-scale benchmark. Airflow/Trino,
streaming/CDC, DataHub and cloud provisioning are outside Phase 4B.
