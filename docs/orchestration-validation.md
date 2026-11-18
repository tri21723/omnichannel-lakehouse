# Orchestration validation (Phase 5)

Evidence for [ADR-0013](adr/0013-airflow-orchestration.md). Commands run sequentially on
core + batch (+ orch from P5-04); raw logs stay local.

## P5-01 — Preflight baseline (2026-11-18)

- Start point: tag `v0.4f-benchmark` + plan commit `17ddc95`; `make check` green; Spark image
  rebuilt from source after the Phase 5 Spark changes.
- `verify-gold` (both datasets), `verify-features` (small), `verify-labels` (default),
  `verify-warehouse` and `test-review-regressions` (22 cases): all rc=0 (11–62 s each).
- Ledger SHA-256 before Phase 5 (unchanged since Phase 4F):

| Ledger | SHA-256 |
|---|---|
| `silver/publication/ledger.json` | `7abd65660a49b1665e6304c10d025e3f79d9bf26e0066f01004ddf773b4fe512` |
| `gold/publication/ledger.json` | `332aa2908ccd0c8788d88c31742e3d9e71805b43b6e4b41bd01417a56d7a5df3` |
| `dp3/publication/ledger.json` | `011ba719eddb289a0a04ba10aa4b8ec710b62e2b63a80b856948e89a79201a6b` |
| `warehouse/export/ledger.json` | `62b2e92f5ddabd9161c06ad001620c93d4c69304bff89c5ca601b6a21b7c4750` |

- Slices per dataset and date: Bronze JSONL for 5 channels + Walmart JDBC; Silver 5 channels;
  Gold/DP3/warehouse per run (dates 2026-01-01..03).
- Memory (`docker stats`, 5 s samples during the verifies): peak per container — workers
  781/796 MiB, driver 623 MiB, MinIO 258 MiB, master 210 MiB, PostgreSQL 156 MiB,
  Lakekeeper 14 MiB; sum of peaks ≈ 2.8 GiB. `mem_limit` total for core + batch ≈ 7.5 GiB on a
  ~11.7 GiB Docker host, leaving room for an Airflow container capped at 3 GiB (standalone +
  one driver JVM) while the `spark-submit` service is idle.
- Measured job durations used for timeouts: see [Phase 5 handoff](phase5-handoff.md) §3
  (Gold ~121 s, DP3 ~56 s, warehouse ~931 s full on the default dataset, replays 39–91 s).

## P5-03 — Shared driver lock

- `python -m spark.lib.driver_lock -- <cmd>` wraps every Spark submission in the Makefile.
- With a container holding `/runtime/locks/spark-driver.lock`, `make verify-warehouse` printed
  `{"event": "spark_driver_busy", ...}` and failed (inner exit 75); after releasing the lock the
  same command passed (`warehouse_verified`).
