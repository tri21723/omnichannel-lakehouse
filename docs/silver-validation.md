# Silver implementation evidence — Phase 4B in progress

## Contract checkpoint — 2026-11-13

ADR-0008 defines physical draft schemas, input selection, deterministic tie-break,
per-slice contributions/corrections, PII and completed-ledger publication semantics.
DDL is a draft only: no Silver entity table or transform has been applied.
Sellers is deferred because the source lacks a standalone seller profile.

Fixture specification: spark/tests/fixtures/silver_cases.json. It covers missing/null
fields, JSON scalar, decimal overflow/scale, bad date, duplicates/ties and corrections.
Expected outcomes are a specification; transform behavior is not yet tested/implemented.

make check passes: 58 tests (49 baseline + 9 reader/input validation tests). SQLFluff
validates the Spark DDL. Hosted CI success for previous work was user-confirmed;
no hosted result is claimed for these new changes.

Docker Desktop was initially off/not integrated, then enabled by the user. The
existing core/catalog/master/workers were subsequently started through make up-batch.
Runtime-dependent TODO checks remain pending until their logs confirm success.
