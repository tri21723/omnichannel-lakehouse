# ADR-0012: Skew benchmark workload and measurement protocol (Phase 4F)

- Status: Proposed (draft). Workload, equivalence and metric rules are fixed here;
  resource numbers and dataset scales are confirmed after P4F-01 baseline and P4F-03 preflight.
- Context: Gold (ADR-0009) is published from a seeded generator whose hot product/seller
  share is configurable (`skew_top_seller_share`, ADR-0005). Phase 4F must show, with
  repeatable measurements, whether AQE, broadcast, salting or partition tuning help on
  skewed data without changing results. Production jobs and ledgers stay untouched.

## 1. Input and isolation

- Source is one completed Gold publication (`/runtime/gold/publication/ledger.json`) read by
  exact Iceberg `snapshot-id` and verified against its ledger content hash, filtered to one
  `_dataset_run_id` (default reference `b1e64afaff0176de180a`, 15,000 order lines).
- Benchmark datasets are derived from that pinned input once (`make benchmark-dataset`) into
  `lh.benchmark.items_<dataset>` / `products_<dataset>`. Their snapshot IDs and content hashes
  are recorded in `/runtime/benchmark/datasets/<dataset>.json`; every trial reads exactly those
  snapshots and the build skips when spec and Gold publication are unchanged.
- The benchmark never writes Gold/DP3/warehouse tables or ledgers. Per-variant results go to
  `lh.benchmark.res_<experiment>_<dataset>_<workload>_<variant>` and the B0 reference to
  `lh.benchmark.ref_<experiment>_<dataset>_<workload>`. Artifacts go under
  `/runtime/benchmark/experiments/<experiment_id>/` (trials, event logs, plans, logs) and are
  not committed; only the redacted summary exported by `make report-benchmark` is.
- Code lives in `spark/jobs/benchmark_skew/`; reusable helpers in `spark/lib/benchmark_*.py`;
  configs (JSON, validated against allowlists) in `spark/jobs/benchmark_skew/configs/`;
  reports in `docs/benchmarks/`.

### Why synthetic amplification

End-to-end ingestion (generator → Bronze → Silver → Gold) of 1M orders does not fit the local
budget: on this host (4 CPU, ~11.7 GiB for Docker) the 15k default reference already takes
~2 min for Gold and ~15 min for the warehouse export. The benchmark targets Spark join and
aggregate behaviour, so lines of the pinned Gold run are replicated `factor` times with new
deterministic keys (`sha2(key || ':' || replica)`) and reassigned to products: a line goes to
`p00` when `xxhash64(order_key) mod 1e6 < hot_share * 1e6`, otherwise to
`p01 + xxhash64(order_key, 'cold') mod 19`, mirroring the generator semantics in §2.
Money and quantity come from the source line, dimension rows are the run's real
`dim_product` rows. Reports call these datasets "synthetic amplification", never
"1M orders ingested end-to-end". Ground truth checked at build and verify time:
rows = source rows × factor, one line per order, no unmatched product, per-channel hot share
within tolerance of the spec.

| Dataset | Factor | Hot share | Purpose |
|---|---|---|---|
| `s15k_control` / `s15k_skew80` | 1 | 0 / 0.8 | smoke and CI |
| `m105k_control` / `m105k_skew80` | 7 | 0 / 0.8 | medium scale |
| `l1m_control` / `l1m_skew80` | 67 | 0 / 0.8 | large scale (~1.0M lines) |

## 2. Data shape (from the generator)

- Each channel has 20 products `p00..p19`, product `pNN` is sold by seller `<channel>-sNN`
  (one seller per product). Each order has exactly one line.
- `skew_top_seller_share = s` assigns `round(s * n)` orders per channel/day to `p00`; the rest
  are uniform over `p01..p19` (`p00` is never drawn by the uniform branch), so the hot-key
  share is exactly `round(s * n) / n` per channel/day, e.g. `s = 0.80` gives 80% on `p00`.
- Control uses `s = 0` (≈ 1/19 per key). Skew uses `s = 0.80`. Everything else (seed, days,
  channel mix, dup/return/drift settings) is identical inside a scale.
- Key cardinality is tiny (≤ 100 product keys per dataset), so hash partitioning already
  concentrates rows; this is the realistic "hot SKU" case, not a high-cardinality skew.
  Measured distribution (top-key share, max/median rows per key, rows per partition) is
  recorded per dataset and compared with the config.

## 3. Workloads

Both workloads filter `_dataset_run_id = :run` on every input.

**W1 — skew join + aggregate (primary).**

```sql
SELECT f.channel, f.currency, p.category, p.seller_id,
       COUNT(*)                     AS line_count,
       COUNT(DISTINCT f.order_key)  AS order_count,
       SUM(CAST(f.qty AS BIGINT))   AS total_qty,
       SUM(f.net_line_amount)       AS net_amount,      -- DECIMAL(28,2)
       SUM(f.gross_line_amount)     AS gross_amount
FROM gold.fact_order_items f
JOIN gold.dim_product p
  ON f.product_key = p.product_key AND f._dataset_run_id = p._dataset_run_id
GROUP BY f.channel, f.currency, p.category, p.seller_id
```

- Join key `product_key` carries the hot key; grouping key `seller_id` carries it too.
- `dim_product` must be unique on `(product_key, _dataset_run_id)`; the job checks this
  before every trial and fails otherwise (salting replication must not create fan-out).
- Inner join semantics: fact rows with NULL/unknown `product_key` are counted separately
  as `unmatched_lines` and must be 0 for the benchmark datasets; they are not silently dropped.
- Output money columns are cast to `DECIMAL(38,2)` in every variant so that the one-step and
  two-step (salted) aggregates have an identical schema.

**W2 — skew aggregate only.** Same aggregation grouped by
`channel, currency, seller_id` directly on `fact_order_items` (no join), to separate
aggregate skew from join skew. AQE skew-join handling is not expected to help W2.

Output grain is one row per group key; NULL group values are kept as their own group.
Monetary sums stay DECIMAL (no float); `avg_net_per_order` is computed in the report as
`net_amount / order_count` from the final row, never by averaging partial averages.

## 4. Variants

| ID | Change vs B0 | Notes |
|---|---|---|
| B0 | baseline: `spark.sql.adaptive.enabled=false`, `autoBroadcastJoinThreshold=-1`, shuffle partitions 16 | must show a SortMergeJoin + Exchange in the executed plan |
| B1 | AQE on + `skewJoin.enabled=true`, default thresholds (256 MB, factor 5), coalescing off | record whether final plan contains `skew=true` |
| B1T | B1 with thresholds sized for local data (`skewedPartitionThresholdInBytes=4m`, factor 2, advisory 4m) | shows whether AQE can split once thresholds fit the data |
| B2 | broadcast `dim_product` (hint), AQE off | record dim size; skip with reason if over budget |
| B3 | salting, AQE off, broadcast off | see §5 |
| B4_32 / B4_64 | shuffle partitions 32 / 64 (B0 is the 16 point), AQE off, broadcast off | partition change stays inside measured time |
| P0 | current production session (`spark/lib/session.py`: Spark 3.5 defaults, AQE on, 10 MB broadcast, 16 partitions) | reference only |

W2 (no join) runs B0, B1, B1T, B4_32, B4_64 and P0; broadcast and salting do not apply.

Each variant changes one dimension from B0. A combined variant is allowed only in the final
round from variants that individually helped, and is labelled as such.

## 5. Salting rules

- Salt factor `N` (default 8, allowlist 2–64). Salt is deterministic:
  `salt = pmod(xxhash64(order_key), N)` so all lines of one order share a salt, which keeps
  `COUNT(DISTINCT order_key)` additive across salts.
- Dimension side: `dim_product` exploded with `sequence(0, N - 1)`; join on
  `(product_key, _dataset_run_id, salt)`.
- Two-step aggregate: partial by `(group keys, salt)` computing count, distinct-order count,
  qty and money sums; final by group keys summing partials. Salt never appears in output.
- Optional hot-key-only salting (salt = 0 for cold keys) is a separate labelled variant.

## 6. Correctness gate

Every trial output is compared with the B0 output of the same experiment:

- identical schema and grain; same row count; group keys unique;
- two-way `exceptAll` with multiplicity equals empty (distributed, no driver collect);
- exact DECIMAL totals per `(channel, currency)` equal; line count equals fact row count;
- checksums are supplementary only.

A trial without a passing gate is reported as `correctness_failed` and excluded from speedup,
not deleted.

## 7. Measurement protocol

- One Spark application per trial, same resources as production (2 executors × 1 core × 1g,
  driver as in `spark/lib/session.py`). Variant settings are applied at session build and
  recorded from `spark.conf` after start (effective config).
- Phases timed separately: `setup` (session + pinned reads + dimension uniqueness check),
  `query` (full materialization of W1/W2 into Spark's `noop` sink, job description
  `bench:query`), `verify` (result table write + correctness gate). Speedup uses `query`
  only; startup is reported, not compared. Event-log metrics are limited to `bench:query` jobs.
- The `noop` sink computes every output column (no bare `count()`) without sink I/O noise,
  identically across variants. The verify phase re-executes the same plan into
  `lh.benchmark.res_*`, which is what the gate compares.
- Order: 1 warm-up (discarded, recorded) + ≥ 3 measured repetitions per cell; variant order is
  rotated per repetition. No other Spark job runs concurrently. OS/page cache is not controlled
  and is stated as a caveat; no trial is called "cold".
- Reported: median and min/max of `query` seconds, speedup `B0_median / variant_median`,
  per-stage task duration median/p95/max, max/median ratio, shuffle read/write bytes,
  memory/disk spill, failed/killed/speculative task counts, physical plans (initial/final).

## 8. Event-log metrics rules

- Event logging is enabled only for benchmark applications (`spark.eventLog.enabled=true`,
  `spark.eventLog.compress=false`, dir under `/runtime/benchmark/experiments/<experiment>/eventlogs`).
- Successful work = tasks whose end reason is `Success`, from stage attempts that completed
  without failure. Failed, killed and speculative-loser attempts are counted as overhead and
  never added to successful-work metrics.
- A stage with zero successful tasks, or a median of 0 ms, yields ratio `null` with a reason;
  a metric absent from the log is `null` ("unavailable"), never 0.
- Truncated final line is tolerated and flagged; malformed lines in the middle are counted and
  make the trial `metrics_incomplete`.
- Raw event logs and plans can contain SQL and config values; reports only export allowlisted
  numeric fields and redacted config keys (`spark.redaction.regex` plus an explicit allowlist).

## 9. Resource budget (to confirm in P4F-01)

- Docker host observed at planning time: 4 CPU, ~11.7 GiB RAM; Spark workers 2.5 GiB limit each.
- Per-trial timeout: 15 min (smoke), 45 min (largest scale). Event logs + output per experiment:
  ≤ 2 GiB. Stop increasing scale when a trial exceeds timeout, OOMs, or free disk < 10 GiB;
  record the failed cell instead of dropping it.
- Planned scales (total orders): 15k smoke, 100k medium, 1M large if preflight allows.

## 10. Consequences

- Production defaults change only if a variant shows a measured gain with a passing gate,
  through a follow-up ADR update and config change, with replay/recovery evidence.
- A "no improvement" result is a valid outcome when the method and limits are documented.
