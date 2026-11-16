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
