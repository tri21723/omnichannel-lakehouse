# Phase 4 handoff: manifest ingestion and replay

Status: design contract; no business Bronze job is implemented.

CLI: dp1_bronze --run-id ID --run-date YYYY-MM-DD --channel CHANNEL --mode full|incremental.
run-id is mandatory; it selects s3://raw/manifests/ID/stream_manifest.json. Run date
selects a date within that manifest. Reject channel/date mismatch, missing objects,
invalid inventory or checksums before writing. Never scan every run/date wildcard.

File ingestion uses boto3 with Spark service credentials to copy exact manifest keys
to bounded disk staging, then Spark reads local JSONL. Stage only selected partitions;
measure disk size and impose a limit before scaling. No S3A filesystem is assumed.
Preserve source duplicates and unknown fields in _raw. Atomic replacement unit:
(dataset run ID, channel, logical date, source type). Use an Iceberg overwrite filter
or equivalent checked commit; do not deduplicate source business keys at Bronze.

Walmart source of truth for Silver is JDBC, not a union with JSONL exports.
Default full-run reconciliation: 3,120 physical JSONL rows vs 3,000 unique JDBC orders.
For date-scoped runs use manifest partition row totals and order_ts UTC date filters;
never compare a full-run count with a single-day load. Filter order IDs by the exact
run prefix, not just dates, because several datasets may coexist in sources.

Consistent JDBC snapshots: initial single-table vertical slice uses a single JDBC
partition/connection with REPEATABLE READ for the read transaction. When ingesting
several related tables or enabling parallel JDBC reads, implement exported PostgreSQL
snapshot coordination and verify every reader imports it before reading; do not claim
cross-table consistency until this is tested. Immutable source export fallback needs
an explicit ADR if JDBC snapshot coordination proves unsuitable.

Provenance: _source_file for JSONL; _source_table for JDBC; _channel;
_dataset_run_id; deterministic _batch_id from run/channel/date/source; _ingest_ts UTC.
Unused provenance fields are NULL. Idempotency concerns business rows and partition
boundaries; ingestion timestamps and snapshot IDs may differ after replacement.

Draft DDL: sql/iceberg/bronze_contract.sql. SQL fixture tables are separate.
Publish an ingestion completion record only after all selected target tables reconcile.
Per-table Iceberg commits do not imply a multi-table transaction; failed partial runs
must remain incomplete and rerunnable. Quarantine is a Silver responsibility; malformed
raw payloads must remain captured with an explicit raw parse-status contract.
