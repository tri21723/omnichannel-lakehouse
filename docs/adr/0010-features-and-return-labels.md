# ADR-0010: Feature Store, Return Labels and Point-in-Time Correctness

- Status: Accepted for implementation (Phase 4D).
- Context: Phase 4C delivered the Kimball dimensional star schema in Apache Iceberg namespace `lh.gold`.
  Phase 4D implements the feature engineering and supervised return label pipelines (`dp3_features`
  and `dp3_return_labels`) owned by `dp3` per ADR-0003.

## Ownership and publication separation

- Per ADR-0003, `dp2` owns the dimensional star schema (`dim_*`, `fact_*`) and `/runtime/gold/publication/ledger.json`.
- `dp3` owns `feat_*` and `lbl_*` tables in `lh.gold`, consuming Gold strictly read-only from pinned snapshot IDs.
- `dp3` maintains its own independent publication ledger at `/runtime/dp3/publication/ledger.json` (with
  immutable archives `publication-<sha256>.json` and atomic state `attempt.json`), ensuring `dp3` never mutates
  or overwrites the `dp2` ledger.
- Tables are defined in namespace `lh.gold` per the data platform convention (`AGENTS.md` §6 and `docs/data-model.md`):
  - `lh.gold.feat_customer_daily`
  - `lh.gold.feat_product_daily`
  - `lh.gold.lbl_order_return`

## Source pinning and multi-dataset isolation

- Readers load Gold tables (`fact_orders`, `fact_order_items`, `fact_returns`, `dim_customer`, `dim_product`,
  `dim_channel`, `dim_date`) by resolving exact snapshot IDs from `/runtime/gold/publication/ledger.json`.
- The ledger must have `status == "completed"` and match its SHA-256 content digest.
- Datasets are strictly isolated using `_dataset_run_id == args.run_id`.
- Soft-deleted orders (`is_deleted == True`) are filtered out from all feature and label calculations.

## Point-in-time correctness & Dual-timestamp cutoff

Feature snapshots are computed as of an evaluation date `as_of_date` (`DATE` in UTC).
The point-in-time cutoff timestamp is defined as the end-of-day boundary:
$$T_{\text{as\_of}} = \text{as\_of\_date} + \text{INTERVAL 1 DAY } 00:00:00 \text{ UTC}$$

### Dual-timestamp cutoff rule
To prevent both order-level and return-level future leakage:
1. **Order events:** Only orders with $\text{order\_ts} < T_{\text{as\_of}}$ are eligible for aggregation.
2. **Return events:** Returns occur $1..14$ days after `order_ts`. For return metrics in customer/product
   features (`returned_orders_30d`, `return_count_30d`, `refund_amount_30d`, `return_rate_30d`), an event
   is included if and only if **both**:
   $$\text{order\_ts} < T_{\text{as\_of}} \quad \land \quad \text{return\_ts} < T_{\text{as\_of}}$$
   Any return occurring at or after $T_{\text{as\_of}}$ is strictly excluded, even if the parent order was placed before $T_{\text{as\_of}}$.

### Rolling window definitions
- 7-day rolling window: $[T_{\text{as\_of}} - \text{INTERVAL 7 DAYS},\; T_{\text{as\_of}})$
- 30-day rolling window: $[T_{\text{as\_of}} - \text{INTERVAL 30 DAYS},\; T_{\text{as\_of}})$

## Product return metrics and quantity policy
Per ADR-0009 and the generator payload contract, `fact_returns` does not possess a `return_qty` field.
Consequently, product features must never invent or assume return quantities:
- Return frequency is measured by return event line counts (`return_count_7d`, `return_count_30d`).
- Return financial impact is measured by actual refunded monetary amounts (`refund_amount_7d`, `refund_amount_30d`).
- Return rate is computed as $\text{return\_count\_30d} / \text{NULLIF}(\text{orders\_30d}, 0)$.

## Monetary arithmetic and multi-currency preservation
- All monetary fields (`spend_7d`, `spend_30d`, `aov_30d`, `gross_revenue_7d`, `gross_revenue_30d`,
  `net_revenue_7d`, `net_revenue_30d`, `refund_amount_30d`) strictly use `DECIMAL(18,2)`.
- Features preserve native channel currencies (`USD` or `VND`); no unanchored FX conversion is performed.
- Ratios (`return_rate_30d`, `refund_ratio`) use `DECIMAL(18,4)`.

## Return label generation: Horizon, Fan-out, and Maturity right-censoring

### Observation horizon
The return observation horizon $H$ is configurable via `--horizon-days` (default: 14 days).

### Maturity condition
An order is mature as of $T_{\text{as\_of}}$ if and only if:
$$\text{order\_ts} + \text{INTERVAL } H \text{ DAYS} < T_{\text{as\_of}}$$

### Right-censoring policy (ADR-0003)
- When $\text{is\_mature} = \text{false}$:
  - All label attributes (`is_returned`, `first_return_ts`, `days_to_return`, `return_line_count`, `refund_amount`, `refund_ratio`)
    must be set to `NULL`.
  - Setting `is_returned = 0` on immature orders is strictly prohibited, as it introduces false-negative bias into training datasets.
- When $\text{is\_mature} = \text{true}$:
  - Returns are filtered to those occurring within the horizon:
    $$\text{order\_ts} \le \text{return\_ts} \le \text{order\_ts} + \text{INTERVAL } H \text{ DAYS} \quad \land \quad \text{return\_ts} < T_{\text{as\_of}}$$
  - To prevent Cartesian fan-out, returns are aggregated to the order level (`order_key`) before joining `fact_orders`:
    - `first_return_ts = MIN(return_ts)`
    - `return_line_count = COUNT(*)`
    - `refund_amount = COALESCE(SUM(refund_amount), 0.00)`
  - Target labels:
    - $\text{is\_returned} = 1$ if $\text{return\_line\_count} > 0$ else $0$.
    - $\text{days\_to\_return} = \text{DATEDIFF}(\text{DATE}(\text{first\_return\_ts}), \text{DATE}(\text{order\_ts}))$ if returned else `NULL`.
    - $\text{refund\_ratio} = \text{CAST}(\text{refund\_amount} / \text{NULLIF}(\text{net\_amount}, 0) \text{ AS DECIMAL(18,4)})$.

## Surrogate key specifications
Surrogate keys are deterministic 64-character lowercase SHA-256 hex strings scoped by `_dataset_run_id`, `channel`, natural ID, and evaluation parameters:
- `customer_feature_key = sha256(concat_ws(':', _dataset_run_id, channel, customer_id, as_of_date))`
- `product_feature_key = sha256(concat_ws(':', _dataset_run_id, channel, product_id, as_of_date))`
- `order_label_key = sha256(concat_ws(':', _dataset_run_id, channel, order_id, cast(horizon_days as string), as_of_date))`

## Physical storage & Partitioning
All tables are Iceberg v2 format with ZSTD-compressed Parquet, partitioned by `as_of_date`:
- `lh.gold.feat_customer_daily`: `PARTITIONED BY (as_of_date)`
- `lh.gold.feat_product_daily`: `PARTITIONED BY (as_of_date)`
- `lh.gold.lbl_order_return`: `PARTITIONED BY (as_of_date)`

Idempotency and recomputation are managed via `MERGE INTO` scoped by `(_dataset_run_id, as_of_date)`
(or entire `_dataset_run_id` during full backfills), preserving records of coexisting runs.

Boundary clarification (2026-11-16): the return horizon is closed, while events at
the cutoff are excluded. Maturity therefore requires horizon_end < as_of_ts. At
equality labels remain NULL until a later cutoff can observe the endpoint.

Label rows now record return-label-policy-v2. The publication ledger tracks policy
per evaluation date, so a job cannot skip dates computed with old semantics, even
when Gold inputs and horizon are identical. Incremental migration marks only the
requested dates as upgraded.

Publication skip decisions use `table_date_provenance` per run, table and date,
including Gold publication, policy and label horizon. Publishing features cannot
promote labels; incremental refresh cannot promote untouched dates. Full refresh
requires an exact date set before skipping. Legacy ledgers without scoped provenance
recompute requested scopes; table schemas and existing snapshots are unchanged.
