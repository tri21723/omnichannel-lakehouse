# ADR-0007: Bronze slices, staging and completion

Status: accepted for implementation; runtime validation tracked in bronze-validation.md.

- CLI selects run/date/channel/source. A source slice is replaced by explicit Iceberg
  MERGE predicate, including run ID and source type, never by day alone.
- `full` replaces the selected slice. `incremental` skips only a completed slice whose
  input fingerprint and current target content still match. It does not mean CDC.
  For JDBC the source is always read (OLTP can change); since Phase 5 an incremental run
  skips without writing when the snapshot evidence (per-table rows and stream SHA-256) equals
  the completed slice, so daily replays do not cascade new pins downstream (ADR-0013). A
  changed row order only causes a re-ingest, never a false skip.
- JSONL goes to `<channel>_orders_raw`, except Walmart exports use
  `walmart_orders_export_raw`. JDBC owns `walmart_<entity>_raw` for Silver.
- Raw UTF-8 lines exclude the physical LF/CRLF terminator. Blank and malformed JSON
  lines remain rows with blank/invalid parse status; invalid encoding fails ingestion.
- A shared staging volume is mounted at the same path in driver and both workers.
  Driver downloads exact manifest keys, validates SHA/size/count, then creates bounded
  JSON wrappers carrying original raw text and S3 URI. Workers read wrappers read-only.
- A POSIX flock in the persistent runtime volume serializes supported Bronze writers
  and verifiers. Locks release on process exit; crash files are not locks. This is a
  local single-host contract, not distributed cloud locking.
- Completion is an atomic JSON file per batch in runtime volume, with attempt status,
  source identity, target snapshot IDs and content summaries. A new running attempt
  invalidates prior completed status before any target write. Consumers must validate
  completion and referenced snapshots, not treat current table state as multi-table atomic.
- Audit includes run/date/source/batch plus file OR source-table provenance. Parse
  status is valid/invalid/blank; Silver owns business validation and quarantine.
- JDBC uses read-only repeatable-read transactions and explicit snapshot coordination;
  five sequential eager readers import one snapshot; concurrent mutation and broken
  reader/coordinator tests pass. Parallel partitioned ingestion remains deferred.
- Stage and completion are local infrastructure. Snapshot retention must preserve
  snapshots referenced by completion; no expiry/cleanup is introduced in this phase.

Sources reviewed 2026-11-13:
- https://iceberg.apache.org/docs/1.9.2/spark-writes/
- https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION

JDBC scope (implemented and snapshot-tested): run prefix plus UTC order date selects orders.
Items and returns join those order IDs, regardless of return date. Customers/products
are distinct referenced rows within the slice; a shared entity may appear in several
logical dates. JDBC raw is typed row JSON (DECIMAL retained without float conversion,
UTC timestamps, explicit NULL). All 5 tables must be read under the same exported
snapshot before completion. Snapshot tokens are ephemeral and must never be logged.

Runtime finding: overwrite-by-filter failed on replay because default truncated column
metrics could not prove all rows in a data file matched the 20-character run ID.
Use one atomic MERGE with a single delete marker matched to the target slice and
unmatched insert rows. This preserves intentional duplicate input rows and supports
empty slices without depending on file metric bounds. Local flock prevents concurrent
Bronze commits; other tools must not write these tables while Bronze is running.

Snapshot fault tests receive the generator credential only through the separate
compose.bronze-test.yml override. Normal Spark services retain reader-only DB access.
Only snapshot-probe-v1 fixture IDs are inserted/updated/deleted by the test.
All reads are eager on the driver with fetch size 1000 and bounded staging; no
parallel Spark JDBC partitions are enabled or claimed.
