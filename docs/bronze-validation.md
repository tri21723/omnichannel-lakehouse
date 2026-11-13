# Bronze validation — Phase 4A

## Baseline checked 2026-11-13

- make check: 27 tests pass; lint pass.
- make up-batch, verify-runtime, smoke-core, verify-lakehouse, verify-sources: pass.
- Dataset b1e64afaff0176de180a: 15,600 JSONL rows; PostgreSQL Walmart
  customers=200, products=20, orders=3000, order_items=3000, returns=300.
- Existing snapshots remain readable; source verification reports changed=0.
- Host: 11GiB RAM, 8.5GiB available before jobs, 84GiB free workspace disk.
- Small reproducible config: generators/fixtures/bronze.yaml (25 orders/day,
  3 days, all channels, duplicate/drift, chunks of 10). Run ID is derived by generator.

Hosted GitHub Actions remains unverified due to reported account billing lock.
The baseline above precedes the implementation evidence below.

## Initial Walmart vertical slice

`make smoke-bronze` passed on 2026-11-13, default dataset, UTC date 2026-01-01.
Full ingestion, full replay, incremental and verify ran sequentially for both sources.
JSONL: 1,040 physical rows retained. JDBC: orders=1000, order_items=1000,
customers=200, products=20, returns=100. Read-back uses exact multiset difference
and row-count/content summaries. Replays retain identical raw/provenance summaries.
JSONL incremental logs bronze_skipped; JDBC refreshes a new snapshot intentionally.

JDBC readers import an exported snapshot before queries and assert repeatable-read
and read-only settings. Raw DB rows are serialized by PostgreSQL row_to_json (no
Python floating-point conversion), then staged before Spark lazy actions.
Initial smoke completed with bronze_matrix_passed (all_slices=false).

Replay first exposed truncated Iceberg file metrics preventing overwrite-by-filter.
A single MERGE delete marker plus insert rows resolved it without resetting tables.
See ADR-0007. Expanded matrix and failure evidence will be added when completed.

## Extended matrix and recovery

- make integration-bronze: pass, all_slices=true, default dataset. All 15 JSONL
  run/date/channel slices ingest/replay/incremental/verify. Total 15,600 rows,
  3,120 rows/channel with original 3,000 order IDs plus 120 duplicate rows retained.
- All 3 Walmart JDBC dates pass the same matrix: 3,000 orders and 3,000 items total.
  Customers/products are scoped per date and intentionally repeat between date slices.
- make test-bronze-recovery: pass. Both worker container hosts read the same /staging
  probe. The dedicated _smoke_slice_isolation table proves empty replacement and
  different-run preservation without altering source data.
- Injected failure immediately after a table commit leaves completion running.
  Verify rejects the partial attempt; full replay restores all 5 tables and completion.
- Completion now includes started/completed timestamps and a one-way fingerprint of
  the exported snapshot token; the token itself is neither logged nor persisted.
- make check: 41 tests pass, including malformed/blank retention, UTF-8 rejection,
  traversal/foreign URI rejection, bounded staging, checksum and lock conflict.

Remaining: concurrent source mutation/connection-failure tests, dedicated end-to-end
malformed fixtures, larger-scale measurements and hosted workflow execution.
These remain unchecked in TODO; no Phase 4A milestone tag has been created.

## Restart and source regression

make restart-lakehouse followed by verify-bronze (read-only JDBC slice) passes
without regenerating business data. Referenced snapshots remain present.
make smoke-core, verify-sources and verify-lakehouse pass after Bronze: original
source run still reconciles with changed=0; Phase 3 smoke remains 3 rows with
original snapshot 9213428472453640956.

## CI fixture and workflow

Small config moved to generators/fixtures/bronze.yaml so the existing generator
image can use it. Published run 186ae107d0793d2ee237: 390 JSONL rows (78/channel);
Walmart has 75 orders/items, 5 customers, 16 referenced products and 6 returns.
The workflow bronze.yml is manual and uses this small config, bounded runtime,
recovery checks and restart verification. Hosted execution remains unverified.

Small-fixture make smoke-bronze also passed (JSONL 26 rows and JDBC 25 orders/items
for its first day). Afterward, read-only verify of the original default run still
passes with its 1,000 JDBC orders and original completion references. Both dataset
runs coexist on the same logical date without replacing one another.

## Snapshot mutation and broken connections — 2026-11-13

make test-bronze-snapshot: pass. Dedicated snapshot-probe-v1 rows across all five
Walmart tables are updated after snapshot export and between readers; an order/item
is inserted concurrently. The exported snapshot sees only the original order and
original fields in every related table. A subsequent snapshot sees both orders.
Nullable product brand and exact JSON text monetary serialization are preserved.

Closing either a reader or the snapshot coordinator causes bounded failure; every
opened connection closes and a new attempt stages successfully with a fresh snapshot.
Fixture rows are cleaned by their exclusive non-generator prefix. No dataset rows
are changed. Cleanup also closes the connection if rollback itself fails (unit test).
make check: 43 tests pass.

## Real MinIO input/fault matrix and completion bookkeeping

make test-bronze-input passes with immutable transport fixtures under dedicated
run IDs: valid inventory/checksum with 4 rows (2 valid JSON, 1 malformed, 1 blank).
_raw and parse status survive MinIO staging and Iceberg read-back. Missing manifest,
missing data file, bad checksum, wrong date/channel, a 1-byte staging budget and
a wrong catalog URI all fail without changing the previous target snapshot.
Retries restore a valid completion. Accepted attempts now explicitly persist failed
status after staging/write failure; no completed flag or timestamp is carried forward.
CLI unit tests confirm exit 1 and safe reason codes without copying exception data.

Completion also includes explicit selection/contract, elapsed time, row throughput,
staged wrapper bytes and current table file counts/bytes. Example small JDBC fixture:
66 entity rows, 22,118 staged bytes, 18.859 seconds / 3.500 rows/s including read-back
verification. Current orders table had 4 data files / 79,150 bytes across all runs.
These are local smoke timings with two 1GiB executors, not a scale benchmark.
No compaction, expiry or orphan cleanup ran.

Recovery tests also cover an initially empty table and reject NULL slice identity
without changing its target snapshot. Snapshot lookup now uses the main ref, avoiding
ambiguity in history entries with equal timestamps. make check: 47 tests pass.

## Raw drift and CI diagnostics

make test-bronze-contract passes read-only on the default reference dataset. Each
channel has 3,120 physical / 3,000 unique order IDs. Amazon first-day buyer_id and
third-day customer_id/fulfillment_type survive; Shopee +0700 string format survives;
Lazada VND and missing brand survive. JDBC orders remain 3,000 unique rows.

The manual Bronze workflow now runs snapshot and real-input fault tests in addition
to replay/recovery/restart. ci_run.sh preserves the child exit code and prints only
allowlisted structured events via ci_events.py. Unit redaction and a local wrapper
probe confirm raw synthetic private output is suppressed while exit 7 is retained.
Raw logs are not uploaded; hosted workflow execution is still unverified.
