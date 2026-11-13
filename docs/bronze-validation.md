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
