# Phase 4C–4E review fixes — 2026-11-16

Four findings are addressed:

1. Gold rejects NULL/negative monetary aggregates before publishing or accepting a
   completed snapshot, including DECIMAL overflow with ANSI disabled.
2. Closed return horizons become mature only when horizon_end < as_of_ts. At equality,
   labels remain NULL. Policy v2 is tracked per evaluation date; old dates are not
   skipped merely because the Gold publication and horizon match.
3. Warehouse data and the authoritative JSONB completion in warehouse.export_state
   commit on one JDBC connection/transaction. Failure before commit rolls back both.
   Failure after commit leaves database completion readable and file manifests
   recoverable by identical retry. CLI verification reads database completion first.
4. Warehouse verifier/reconciler compares fingerprints of every exported column,
   including nonmonetary fields and audit fields. Canonical rows preserve decimals,
   UTC microseconds, NULLs, booleans and duplicate multiplicity. Read-only queries use
   REPEATABLE READ; counts/totals/constraints remain additional checks.

Local checks completed:

- make check: lint plus 160 unit tests pass.
- make test-review-regressions: three real Spark assertions pass (endpoint censored,
  endpoint observed at next cutoff, overflowing gross rejected with valid net).
- make test-warehouse-review: real PostgreSQL post-commit recovery and nonmonetary
  corruption detection pass. A reserved fixture owns one dimension row, then retracts
  its row and completion; normal dataset rows remain untouched by this fixture.

Existing make test-warehouse-recovery passes all three pre-commit fault points,
window pruning from 375 orders/items + 30 returns to 125 orders/items + 10 returns,
and recovery back to the baseline while preserving the default dataset. Final
make verify-warehouse passes for exactly the two normal runs (small/default), proving
the reserved review fixture's completion was removed after its rows were retracted.
The Gold/DP3 and Warehouse manual workflows now invoke the new regression targets;
only local execution is reported here.

Full-row evidence is read as a bounded per-table JSON result rather than one Py4J
call per cell. Decimal columns travel as text and timestamps as integer microseconds;
this preserves exact values and substantially reduces JDBC bridge calls. The exporter
already collects bounded local slices; this is not a large-scale streaming checksum.

Legacy file-only entries remain supported through exact reconciliation against pinned
Gold. New writes seed database completion and full-row fingerprints without resetting
existing tables. Historical snapshots/archives and source data are preserved. No
runtime version/port change or new hosted CI result is claimed.

## Follow-up review fixes — 2026-11-16

Three additional findings are addressed:

- DP3 freshness now records Gold publication, policy and label horizon per table/date.
  Feature refreshes cannot mark labels current; incremental refreshes cannot promote
  untouched dates. Full-mode skips require the exact date set. Legacy scopes without
  this provenance recompute on their next requested publication. Input rows must
  match the declared source, policy, dates and label horizon before publication.
- Warehouse writers consult committed PostgreSQL metadata before parsing the file
  mirror. Missing and malformed mirrors recover through verified retry without
  rewriting data. Invalid legacy files still fail when no DB completion exists.
- Feature aggregations reject unsupported channel currencies, NULL currency and
  mixed currencies before summing. The existing channel/entity/date grain remains;
  arbitrary-currency support requires a future schema/key migration.

Validation of the updated source (image rebuilt using `make build-batch`):

- `make check`: lint and **164 unit tests passed**, including table/date/horizon
  isolation and the actual publish skip branch with mocked Spark.
- `make test-review-regressions`: **7 real Spark cases passed**, including supported
  USD/VND and rejection of mixed EUR, NULL currency and unknown channels.
- `make test-warehouse-review`: **2 real JDBC scenarios passed**, now corrupting the
  mirror JSON after the post-commit failure before retrying and verifying recovery.
  The fixture's rows/completion are removed and remaining runs verified by the test.

The fixture publish exposed an additional mapping error: the generator emits Shopee
orders in USD, while DP3 previously assigned VND. The mapping now matches the source
contract, with `feature-policy-v2` forcing recomputation of old feature scopes.
The real Spark currency fixture includes Shopee USD and Lazada VND.

These are local results; no new hosted CI run is claimed.

Real Iceberg publish/replay also passed for run `186ae107d0793d2ee237`, evaluation
`2026-01-03`, incremental mode, Gold publication
`46d63f72f5aba0aec78bc2601491a2bb5db092662feb551ea61fb16640acd37c`:

1. Features publish: `skipped=false`, feature policy v2.
2. Labels publish (14-day horizon): `skipped=false`, label policy v2.
3. Identical features replay: `skipped=true`, read-back verification passed.
4. Identical labels replay: `skipped=true`, read-back verification passed.

Only this requested date/run was migrated by the integration check. Other historical
feature/label dates remain preserved and recompute when requested under the new
provenance/policy rules; this is not a claim of a complete historical backfill.

## Legacy full refresh and feature overflow — 2026-11-17

- Full-mode skip checks both retained `table_dates` and `table_date_provenance`.
  Regression starts with legacy dates A/B, migrates B incrementally, and confirms a
  full request for B must replace A/B rather than skip. After replacement, replay
  can skip. Missing date inventory also fails closed for full-mode skip.
- Feature policy v3 validates numeric aggregates before joins/default filling in
  customer orders/returns and product items/returns. DECIMAL cast overflow under
  ANSI=false raises an error; a missing activity group still gets zero metrics.
  Existing v2 scopes recompute when requested, without a physical schema change.

Validation: `make check` passed lint and 165 tests. After `make build-batch`,
`make test-review-regressions` passed 11 real Spark cases, including two valid
6e15 orders overflowing customer/product monetary sums, normal customer totals
(12, AOV 6), and empty activity (zero spend, NULL AOV). The synthetic tests publish
no tables. These are local results.

Policy-v3 Iceberg integration passed on run `186ae107d0793d2ee237`, date
`2026-01-03`, incremental mode: first publish `skipped=false`, identical replay
`skipped=true`, including read-back verification. Other dates/runs are preserved
and migrate only when requested; this is not a full historical backfill.

## Count/quantity INT narrowing — 2026-11-17

Shared `checked_int` checks wide nonnegative aggregates against 2147483647 before
narrowing. It covers Gold item_count/total_qty, customer/product feature counts and
units, and label return_line_count. Positive wraparound cannot pass validation.
The output schema stays INT; out-of-range/NULL aggregates fail explicitly in either
ANSI mode. Gold policy v2, feature policy v4 and label policy v3 prevent old scopes
from skipping recomputation when requested. Gold skip now checks policy_version.

Validation: `make check` passed lint and 165 tests. Rebuilt image passed 22 real
Spark regression cases, including product feature aggregation of three quantities
of 2 billion, boundary values 0/INT_MAX, and rejection of NULL, -1, INT_MAX+1 and
sum=6 billion under both ANSI modes. Expected Spark task errors are caught by the
regression; the final success event confirms all assertions passed. No historical
backfill or new hosted CI result is claimed.

Feature policy-v4 integration passed for run `186ae107d0793d2ee237`, date
`2026-01-03`, incremental mode: first publication `skipped=false`; identical replay
`skipped=true` with read-back verification. This migrates only the requested feature
scope; Gold/label scopes migrate when their jobs are requested.
