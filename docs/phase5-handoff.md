# Phase 5 handoff: orchestrating the batch jobs with Airflow

Phase 4 (Bronze → Silver → Gold → DP3 → warehouse, plus the skew benchmark) is complete
locally. This document is the operational contract Airflow DAGs must respect. It does not
implement Phase 5; DAG names follow AGENTS.md (`dp1_bronze`, `dp2_silver`, …).

## 1. Global constraints

- **One Spark driver at a time.** Every job runs in the `spark-submit` container with the
  fixed name `omnichannel-spark-driver` (also `spark.driver.host`). Two concurrent
  `docker compose run --name omnichannel-spark-driver` calls fail. Use Airflow
  `parallelism=1` (AGENTS.md §4) or a single-slot pool for every Spark task, and
  `max_active_runs=1` per DAG.
- **Resources (pinned):** 2 workers × 1 executor × 1 core × 1 GiB (`spark.cores.max=2`),
  driver container `mem_limit` 1536 MiB, `SPARK_SHUFFLE_PARTITIONS=16`. Phase 4F found no
  reason to change production session defaults (see [skew benchmark](benchmarks/skew.md)).
- **Code is baked into the image.** After code changes run `make build-batch`; DAGs should
  record the Spark image ID with each run (the benchmark already does via `BENCH_IMAGE_ID`).
- **Inputs are pinned, never latest.** Each stage reads the completed ledger of its upstream
  stage and Iceberg snapshots by ID; ledgers live in the `lakehouse-config` volume under
  `/runtime/<stage>/…`. Never delete that volume, buckets or the `sources` database.
- **Logical date:** pass Airflow `ds` / `data_interval_start` as `--run-date`/`--as-of-date`;
  never `datetime.now()` inside jobs.
- **Exit codes:** non-zero means the stage did not publish; ledgers keep the last completed
  publication, so a retry is safe. Structured JSON events on stdout can be parsed with the
  allowlist in `docker/batch/ci_events.py`.

## 2. Job contracts

| DAG / job | Command (via `make spark-submit JOB=…`) | Input (pinned) | Output | Replay behaviour |
|---|---|---|---|---|
| `dp1_bronze` | `BRONZE_ARGS='--run-id R --run-date D --channel C --source jsonl\|jdbc --mode full'` | source manifest in MinIO `raw/`, PostgreSQL `sources.walmart` snapshot | `lh.bronze.*`, `/runtime/bronze/<batch>.json` | `full` replaces the slice via MERGE (no duplicates); `incremental` skips a completed slice (ADR-0007) |
| `dp2_silver` | `SILVER_ARGS='--run-id R --run-date D --channel C --mode full\|incremental --fail-on-reject'` | completed Bronze records | `lh.silver.*`, `quarantine_*`, `/runtime/silver/publication/ledger.json` | full and incremental skip a successfully published identical pin (ADR-0008) |
| `dp2_gold` | `GOLD_ARGS='--run-id R --mode full'` | Silver publication ledger | 8 `lh.gold.dim_*/fact_*`, `/runtime/gold/publication/ledger.json` | skips when Silver publication and policy (`gold-policy-v2`) are unchanged |
| `dp3_features` | `FEATURES_ARGS='--run-id R --all-dates\|--as-of-date D --mode full'` | Gold publication | `feat_customer_daily`, `feat_product_daily`, `/runtime/dp3/publication/ledger.json` | per table/date provenance; identical scope skips (`--force` recomputes) |
| `dp3_return_labels` | `LABELS_ARGS='--run-id R --all-dates --horizon-days 14 --mode full'` | Gold publication | `lbl_order_return`, DP3 ledger | label policy v3 per date; horizon change recomputes |
| `dp4_warehouse` | `WAREHOUSE_ARGS='--run-id R --run-date D --mode full'` | Gold publication | PostgreSQL `warehouse.*`, `warehouse.export_state`, file mirror ledger | one JDBC transaction; identical export verifies fingerprints and skips |

Read-only checks to run after each publish: `make verify-gold`, `verify-features`,
`verify-labels`, `verify-warehouse`, `reconcile-gold`, `reconcile-warehouse` (see runbook).

## 3. Observed durations (local, 2026-11-17, default reference 15,000 orders)

| Step | Observed | Suggested Airflow `execution_timeout` |
|---|---|---|
| Gold publish (full) | ~121 s (ledger `duration_seconds`) | 15 min |
| Gold replay (skip + verify) | 64–91 s | 10 min |
| DP3 features / labels publish | ~56 s (ledger) | 15 min |
| DP3 replay (skip) | 44–63 s | 10 min |
| Warehouse export (default, full) | ~931 s | 45 min |
| Warehouse replay (skip, small fixture) | ~39 s | 10 min |
| `verify-*` / `reconcile-*` | 13–99 s | 10 min |

Durations include ~15–20 s Spark application start-up. Warehouse export is driver-side JDBC
and dominates the batch path; it is the first candidate for future tuning (out of 4F scope).
Use `retries=1–2` with a delay of a few minutes: publishes are idempotent, and a stale
`running` attempt is recovered by the next run (see ADR-0008/0010/0011 and the runbook).

## 4. Dependencies between DAGs

`dp1_bronze` (all channels/sources of a date) → `dp2_silver` → `dp2_gold` →
{`dp3_features`, `dp3_return_labels`} → `dp4_warehouse`. Each downstream task must read
the upstream ledger produced by the same logical date's run; prefer ledger/publication-ID
sensors over time-based waits. DP3 and warehouse can run in either order after Gold but not
concurrently (single driver).

## 5. Benchmark tooling (manual only)

`make benchmark-*` targets are for manual/CI evaluation, not scheduled DAGs. They write only
`lh.benchmark.*` and `/runtime/benchmark/`, never Gold or ledgers, and must not overlap a
scheduled Spark run.
