# ADR-0007: Bronze slices, staging and completion

Status: accepted for implementation; runtime validation tracked in bronze-validation.md.

- CLI selects run/date/channel/source. A source slice is replaced by explicit Iceberg
  overwrite filter, including run ID and source type, never by day alone.
- `full` replaces the selected slice. `incremental` skips only a completed slice whose
  input fingerprint and current target content still match. It does not mean CDC.
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
  no concurrent/parallel ingestion consistency claim until integration evidence exists.
- Stage and completion are local infrastructure. Snapshot retention must preserve
  snapshots referenced by completion; no expiry/cleanup is introduced in this phase.

Sources reviewed 2026-11-13:
- https://iceberg.apache.org/docs/1.9.2/spark-writes/
- https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION

JDBC scope (implementation pending): run prefix plus UTC order date selects orders.
Items and returns join those order IDs, regardless of return date. Customers/products
are distinct referenced rows within the slice; a shared entity may appear in several
logical dates. JDBC raw is typed row JSON (DECIMAL retained without float conversion,
UTC timestamps, explicit NULL). All 5 tables must be read under the same exported
snapshot before completion. Snapshot tokens are ephemeral and must never be logged.
