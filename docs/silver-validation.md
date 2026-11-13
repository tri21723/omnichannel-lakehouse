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

## Baseline and input reader

After make up-batch: verify-runtime, verify-bronze (default Walmart JDBC day 1),
and test-bronze-contract pass; source counts remain 15,600 JSONL / 3,000 JDBC orders.
The first smoke-silver-input passes with all five tables: pin old references, replay
small Bronze slice, create a new distinct pin, reconcile both against the same raw
content, and reject a fabricated missing snapshot without latest fallback.
Unit tests verify failed/legacy completion rejection, strict table inventory,
selection/batch validation, invalid snapshot IDs, NULL evidence and pin tampering.
The fixture-case file is a specification for upcoming transform tests, not evidence
that normalization/quarantine have already been implemented.

The second real-cluster smoke also passed. All five old snapshot inputs retain their
original _ingest_ts after Bronze replay, while the newly pinned snapshots have new
ingestion timestamps. This distinguishes pinned reads from a silent latest fallback
even when business payloads are identical. Both runs reject snapshot ID 1, which is
not present, and leave the old content-addressed input file unchanged.

Current checks: make check = 58 tests pass, Spark SQL DDL lint pass; baseline runtime,
Bronze verification and raw-contract checks pass after stack startup. P4B-01/02/03
are complete. Next is P4B-04: Walmart entity normalization. No Silver publishing
job, HMAC enforcement, quarantine writer or contribution ledger exists yet.
