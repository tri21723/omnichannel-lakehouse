# ADR-0014: Iceberg snapshot retention — dry-run reporting only

- Status: Accepted for reporting. Snapshot expiry, orphan removal and compaction stay disabled.
- Context: every stage pins its inputs by Iceberg snapshot ID (ADR-0007…0012). Expiring a
  pinned snapshot would break replay, verification and reconciliation. The plan schedules a
  weekly `maintenance_iceberg` DAG; Phase 4F/5 explicitly excluded automatic deletion.

## Decision

1. **Pinned snapshots are never candidates.** A snapshot is pinned if any JSON document under
   `/runtime` (ledgers, `publication-*.json` archives, Bronze completion records, export
   manifests, benchmark dataset manifests) holds its ID as an integer under a key containing
   `snapshot`. Unreadable JSON files are listed in the report so a broken pin source is visible.
2. **Candidate rule (report only):** not pinned, not referenced by a branch/tag (`refs`), older
   than `retention_days` (default 7) before the run's logical time, and not among the newest
   `keep_last` (default 5) snapshots of the table.
3. **Dry run only.** `spark/jobs/maintenance/iceberg_report.py` reads `snapshots`, `refs` and
   `files` metadata tables for `bronze`, `silver`, `gold`, `benchmark` and writes
   `/runtime/maintenance/iceberg-report-<date>.json` with per-table snapshot counts, pinned
   counts, candidates and small files (< 8 MiB). It never calls `expire_snapshots`,
   `remove_orphan_files` or `rewrite_data_files`.
4. Airflow DAG `maintenance_iceberg` (`@weekly`, paused at creation, pool `spark_driver`) runs
   the report; `make iceberg-report` runs it manually.

## Enabling real maintenance later

Requires a follow-up ADR and explicit approval: a retention window agreed per layer, a
pre-expiry pin check inside the same job, `expire_snapshots` with `retain_last`, an orphan
policy that respects in-flight writers, and replay/verify evidence afterwards. Never expire
snapshots referenced by the ledgers or manifests above.
