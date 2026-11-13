# ADR-0008: Silver input and entity state contract

Status: accepted design; input reader under implementation. Walmart orders/customers/items normalization is implemented in validation mode;
publishing is not yet implemented. No Silver tables are created by this ADR/DDL.

## Source selection and schema

CLI selects run-id/run-date/channel/mode plus an explicit pinned input ID for replay.
Walmart uses the entire 5-table JDBC completion; other channels use one JSONL orders
completion. Walmart export cannot be authoritative. Legacy completion without selection
must be refreshed through full Bronze ingestion before capture.

Each capture validates completion/contract/selection/batch/table inventory and hashes
canonical immutable references. Local pin files use content-addressed names and atomic
create-only publication. Readers use Iceberg snapshot-id, then filter run/channel/date/
source/batch and compare raw content summary. No latest fallback. Initially empty
snapshots require zero evidence and create typed empty input.

Entity keys are channel plus: orders.order_id; items.order_id/line_no;
customers.customer_id; products.product_id; returns.return_id/return_line_no.
IDs remain STRING, temporal fields UTC TIMESTAMP, amounts DECIMAL(18,2), quantities
positive INT. Required IDs cannot be missing/null/blank. Currency is a required
three-uppercase-letter source code; do not infer FX. Strings are not implicitly
coerced to IDs from booleans/containers. Invalid syntax/scalar JSON and bad casts
are rejected with stable reason codes. Nonzero fractional digits beyond scale 2
are rejected, not rounded; range overflow is rejected. Optional brand can be NULL.
Orders status allowlist initially paid/pending/shipped/delivered/cancelled/refunded;
unknown values are quarantined pending explicit mapping. is_deleted stays false;
absence is not a CDC delete event.

Items/returns inherit parent order updated_ts as version time because the source
has no update timestamp for those entities. Customers/products use their own source
updated_ts. Orders/customer/items are the first implementation slice. Sellers is
explicitly deferred: retain seller_id, do not invent seller name/country.

## Corrections, ordering and scope

Retain normalized per-slice contributions privately in Silver (_contrib_<entity>).
Scope is run/channel/date/source. Contribution rows contain hashed PII only and lineage,
never raw payload. Persist source revision/pin identity in the publishing ledger.
Full replaces the selected contribution, including retraction when a former valid row
becomes invalid/disappears. Incremental skips only a successfully published identical
pin/policy/key version; otherwise processes a new explicit source revision.

Recompute affected keys from committed contributions plus the new slice. Thus shared
customer/product rows referenced by other dates survive correction of one slice.
Choose winners by updated_ts DESC, canonical normalized payload SHA-256 DESC. Exclude
attempt timestamps and ingest order from canonical tie-break input. Exact duplicates
count as losers. Equal version/hash yields identical business values; provenance is
chosen deterministically from source-table/file and record fingerprint.

Older-date replay cannot overwrite a newer business version. A previously published
superseded pin cannot silently reactivate an obsolete slice revision: require a new
explicit correction revision. Conflicting reuse of a natural key by independent dataset
runs is rejected before publish; generator run-prefixed IDs do not collide.

Use one local Silver writer lock and a committed ledger manifest. New contribution
and output snapshots may commit separately, but the ledger publishes their references
only after all entity/quarantine reconciliation. Recovery reads prior committed ledger
snapshots, not uncommitted table heads. Consumer reads only recorded completed output
snapshots. Current table heads are not a multi-table transaction.

## PII, DQ and storage

PII policy v1: Unicode NFKC and trim/collapse whitespace for names; trim/lowercase
email. HMAC-SHA256 with a required secret >=32 bytes, domain separated by field and
channel; record key version. Null optional values remain NULL. Missing key fails
before writes. Key rotation needs explicit backfill/revision; never mix key versions
silently. Secret transport and runtime enforcement are Phase 4B-06 tasks.

Quarantine stores stable record identity, sorted reason codes and references to the
restricted Bronze snapshot/raw record rather than copying raw PII. For exploded
entities track parent fingerprint and child position. Check accounting separately:
input occurrences = rejected + valid candidates; candidates = winners + duplicate
losers. Parent failure and rejected children must be explained without double counting.

Main entity tables use Iceberg v2/Parquet/ZSTD; orders partition by days(order_ts),
returns by days(return_ts); small shared entities/items initially unpartitioned.
Contributions partition by logical run date. Schema evolution is reviewed/explicit;
CREATE IF NOT EXISTS is not a migration mechanism. Gold stays outside this phase.

Reference checked 2026-11-13:
https://iceberg.apache.org/docs/1.9.2/spark-queries/

P4B-04 implementation notes: discount is an absolute amount for the complete line,
so line value = qty * unit_price - discount, with discount bounded by line value.
Orders require at least one valid item and an exact DECIMAL sum match. Items whose
order is rejected produce a missing_valid_parent rejection. Full cross-entity DQ
and invalid-child cascade rules remain P4B-05 work. first_order_ts is currently the
minimum valid order timestamp in the selected slice; global first-order aggregation
is a later contribution-reduction responsibility.

Candidate _record_hash is computed on parsed normalized fields before enrichment.
P4B-07 must finalize the canonical hash after derived fields before publishing/dedup;
no persisted Silver state relies on this provisional candidate hash yet.
