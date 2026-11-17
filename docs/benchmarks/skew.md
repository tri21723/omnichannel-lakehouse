# Skew benchmark — Phase 4F

Design and rules: [ADR-0012](../adr/0012-skew-benchmark.md). Baseline evidence:
[`baseline-4f.json`](baseline-4f.json). Raw artifacts (trial JSON, event logs, plans, logs)
stay local under `/runtime/benchmark/experiments/<id>/`; this page and the exported
`skew-<experiment>.{json,csv,md}` files are the committed, redacted summary.

## Conclusion

- **No production tuning change.** The production session (P0: Spark 3.5 defaults with AQE on
  and the 10 MB broadcast threshold) was the only setting that was consistently faster than the
  B0 baseline on the 1M join workload: paired speedup **1.25×** in `focus-1m` (7 rounds,
  0.89–1.40) and 1.29× / 1.34× (control / skew) in `matrix-1m`. The pipeline already runs
  with it, so nothing is changed and no rollback is needed.
- **Skew is real at task level but not the bottleneck on this cluster.** On `l1m_skew80` the
  B0 join stage has a slowest/median task ratio of 13–18 (slowest task 1.9–3.8 s), yet the job
  runs on 2 executor cores and the hot data is five keys of ~16% each, so the straggler stays
  shorter than each core's share of the stage (~7 s of executor time per core). Fixed
  per-stage costs dominate wall time.
- **AQE skew join (B1/B1T):** with default thresholds (256 MB) nothing is split. With 4 MiB
  thresholds the final plan shows `SortMergeJoin(skew=true)` and the hot partitions are split
  (16 → 35 tasks, slowest task ~3.4 s → ~1.8 s), but the extra stage materialization costs more
  than it saves: paired 0.80× in `focus-1m`. B1's 1.48× in `matrix-1m` did not reproduce
  (0.94× over 7 rounds) and is treated as noise.
- **Broadcast (B2)** removes the join shuffle (shuffle write 155 → 84 MiB, ratio → ~4) but on
  its own is not reliably faster (0.96×, 0.75–1.18 in `focus-1m`); combined with AQE (P0) it is.
- **Salting (B3)** is correct (identical outputs, distinct-order counts preserved) and halves the
  task skew ratio, but adds shuffle volume and a second aggregation: 0.74–0.98× everywhere.
- **More shuffle partitions (B4_32/B4_64)** never helped and 64 partitions was consistently
  slower (0.67–0.85× on W2, 0.84–1.04× on W1): with ≤ 100 keys most partitions are empty.
- **Aggregate-only skew (W2)** is absorbed by Spark's partial (map-side) aggregation; no variant
  changes it beyond noise (0.98–1.07× except B4_64).
- 15k and 105k runs are overhead-dominated (query 8–17 s regardless of variant) and are kept as
  smoke evidence only.

This is a valid "no improvement" outcome under ADR-0012 §10: the method, correctness gate and
limits are documented; no speedup target was assumed. Skew handling would matter with more
executor cores than hot keys, much larger hot partitions, or a dimension too large to broadcast.

## Results

All 240 planned trials (smoke 12, ci 12, matrix-100k 56, matrix-1m 112, focus-1m 48) finished
with status `ok` and passed the correctness gate; `make verify-benchmark` reported 0 problems
for every experiment. No spill, failed or speculative task was recorded. Smoke additionally keeps
24 `*.attemptN.json` artifacts from two development runs before the metric fix; they are
excluded from the tables and counted as retried artifacts.

**Reading the tables.** "Speedup vs B0" divides cell medians and is sensitive to host drift.
In `matrix-1m` the second and third rounds were ~1.5–2× slower for *every* variant (setup time
15 s → 20–25 s at the same time), and the rotated order placed B0 late in those rounds, which
inflated the B0 median. "Paired speedup" compares each variant with B0 inside the same round and
is the figure used for conclusions; `focus-1m` re-measured the skewed join with 7 rounds for
that reason.

### focus-1m — skewed join, 7 rounds (primary evidence)

| Variant | Query s median (min–max) | Paired speedup median (min–max) | Shuffle-stage task max/median | Slowest shuffle task s | Shuffle write MiB | Final plan |
|---|---|---|---|---|---|---|
| B0 | 13.03 (10.20–14.28) | 1.00 | 13.31 | 1.90 | 155.3 | SortMergeJoin |
| B1 | 13.20 (12.21–15.20) | 0.94 (0.83–0.99) | 14.12 | 2.08 | 155.3 | SortMergeJoin |
| B1T | 16.37 (13.78–20.21) | 0.80 (0.51–0.95) | 13.88 | 2.12 | 155.5 | SortMergeJoin(skew=true), AQEShuffleRead |
| B2 | 12.10 (10.59–18.64) | 0.96 (0.75–1.18) | 3.97 | 1.17 | 84.5 | BroadcastHashJoin |
| B3 | 15.85 (13.23–18.91) | 0.81 (0.65–0.88) | 6.33 | 1.57 | 165.5 | SortMergeJoin (salted) |
| P0 | 10.23 (9.65–14.53) | **1.25 (0.89–1.40)** | 1.24 | 2.11 | 84.4 | BroadcastHashJoin, AQEShuffleRead |

### matrix-1m — paired speedup vs B0 (3 rounds + warm-up)

| Workload / dataset | B1 | B1T | B2 | B3 | B4_32 | B4_64 | P0 |
|---|---|---|---|---|---|---|---|
| W1 join, control | 0.96 | 0.91 | 1.06 | 0.74 | 0.87 | 0.84 | **1.29** |
| W1 join, skew 80% | 1.48* | 0.91 | 1.27 | 0.98 | 1.26 | 1.04 | **1.34** |
| W2 aggregate, control | 1.03 | 0.99 | – | – | 0.83 | 0.67 | 0.84 |
| W2 aggregate, skew 80% | 0.98 | 1.07 | – | – | 0.99 | 0.85 | 1.05 |

\* not reproduced in `focus-1m` (0.94×). B0 task skew on the skewed join: ratio 14.6, slowest
task 3.8 s; control: ratio 6.9, slowest task 2.2 s (hash collisions of ~95 keys over 16
partitions).

Full per-cell tables (medians, min–max, shuffle, spill, plan evidence):
[matrix-1m](skew-matrix-1m.md) · [focus-1m](skew-focus-1m.md) ·
[matrix-100k](skew-matrix-100k.md) · [smoke](skew-smoke.md) · [ci](skew-ci.md); machine-readable
`skew-*.json` / `skew-*.csv` next to them.

## Environment

| Item | Value |
|---|---|
| Host | WSL2, Docker Engine 29.5.2, 4 CPU, ~11.7 GiB for Docker |
| Spark | 3.5.5 standalone, Iceberg 1.9.2 REST catalog (Lakekeeper 0.9.3), MinIO S3 |
| Resources per trial | 2 executors × 1 core × 1 GiB (`spark.cores.max=2`), driver container 1.5 GiB |
| Benchmark commit / image | `b8e6cad` / `sha256:b0aaea1c5310…` (matrices, smoke, ci); `db8dd55` / `sha256:d19acf3fdfcc…` (focus-1m, adds paired speedup only) |
| Gold input | publication `332aa290…` (gold-policy-v2), run `b1e64afaff0176de180a` (15,000 lines); CI uses `186ae107d0793d2ee237` |

## Datasets (synthetic amplification, measured)

Lines of the pinned Gold run are replicated with new deterministic keys and reassigned to
products (ADR-0012 §1). This is **not** an end-to-end ingestion of 1M orders.

| Dataset | Lines | Product keys | Top key share (all) | Hot share per channel | Max/median rows per key |
|---|---|---|---|---|---|
| `s15k_control` | 15,000 | 95 | 1.2% | 5.8–6.0% | 1.13 |
| `s15k_skew80` | 15,000 | 100 | 16.1% | 79.6–80.6% | 75.5 |
| `m105k_control` | 105,000 | 95 | 1.2% | 5.4–5.8% | 1.10 |
| `m105k_skew80` | 105,000 | 100 | 16.1% | 79.6–80.4% | 75.0 |
| `l1m_control` | 1,005,000 | 95 | 1.1% | 5.3–5.4% | 1.03 |
| `l1m_skew80` | 1,005,000 | 100 | 16.0% | 79.9–80.1% | 76.0 |
| `ci_control` / `ci_skew80` | 15,000 | 83 / 88 | 1.5% / 16.3% | 6.6–7.3% / 79.4–81.6% | 1.22 / 69.9 |

The hot product is one key per channel, so the skew is five hot keys (each ~16% of all
lines), not a single key. Every dataset passed its ground-truth check (rows = source × factor,
one line per order, zero unmatched products, hot share within tolerance) at build and verify
time; an identical rebuild was skipped.

## Method

- Workloads: **W1** join `items ⋈ dim_product` on `(product_key, run)` then aggregate by
  channel/currency/category/seller; **W2** aggregate by channel/currency/seller without a join.
- Variants: B0 baseline (AQE off, broadcast off, 16 partitions); B1 AQE skew join with default
  thresholds; B1T AQE skew join with 4 MiB thresholds; B2 broadcast hint; B3 salting N=8;
  B4_32/B4_64 shuffle partitions; P0 the production session (AQE on, 10 MB broadcast).
- Each trial is its own Spark application; `query` = full materialization into the `noop`
  sink; speedup = B0 median / variant median of `query` seconds. Matrix runs use 1 warm-up
  plus 3 measured repetitions with the variant order rotated; smoke/CI use 1 repetition.
- Every trial passed the correctness gate before entering the tables: identical schema, row
  count and unique keys vs B0, two-way `exceptAll` empty, exact DECIMAL totals per
  channel/currency equal to the input, zero unmatched lines.
- Metrics come from the trial's event log, restricted to `bench:query` jobs. "Shuffle-stage
  task max/median" is the worst ratio among stages that read shuffle data.

## Caveats

- One laptop-class host under WSL2; OS page cache and MinIO caching are not controlled, so no
  trial is "cold". Differences of a few seconds at 15k/105k are within run-to-run noise.
- 15k and 105k runs are dominated by fixed costs (Iceberg planning, task scheduling); their
  timings show overhead, not skew handling.
- Amplified data reuses real money/quantity values but not real order timing or customers.
- Results are for this workload shape (≤ 100 keys, tiny dimension); they do not generalise
  to high-cardinality skew or large dimensions that cannot be broadcast.

## Reproduce

```sh
make build-batch
make test-benchmark
make benchmark-dataset            # all datasets; skipped when unchanged
make verify-benchmark-dataset
make benchmark-skew BENCH_CONFIG=matrix-1m     # also smoke, ci, matrix-100k
make verify-benchmark BENCH_CONFIG=matrix-1m
make report-benchmark BENCH_CONFIG=matrix-1m
```

Retention: keep `/runtime/benchmark/datasets/*.json` and the Gold snapshots they pin while
results are referenced; experiment directories may be removed after the report is committed.
Never expire Gold/DP3/warehouse ledger snapshots for benchmark cleanup.
