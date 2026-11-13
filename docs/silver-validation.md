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

## P4B-04 Walmart normalization — 2026-11-13

make check: lint + 77 tests pass. Strict parser handles missing/null fields, invalid
syntax/scalar JSON, ID types, UTC timestamp offsets, enum/currency, positive INT and
DECIMAL(18,2) overflow/scale/nonfinite values. JSON numbers are parsed as Decimal,
not float. Discounts cannot exceed qty * unit price.

Customer names/emails are HMAC-SHA256 hashed with NFKC normalization and channel/
field domain separation. make env-silver generates a persistent random 32-byte key
in ignored .env; tests confirm repeat initialization does not rotate it, including
a blank setting at EOF. Missing key/version fails before Spark processing.

make test-silver-normalize passes on real Spark executors: valid candidates 1/entity,
rejected orders=2, customers=1, items=2 (5 total). Assertions cover Decimal 10.25, UTC
order timestamp, inherited item updated_ts, slice-local customer first_order_ts and
64-character hashed email. Rejections carry codes/references, not raw/name/email.

make smoke-silver-normalize passes twice with strict expected-order/reject checks:
25 orders, 5 customers, 25 items; rejected=0 each. input = valid + rejected by entity.
These are in-memory candidate frames; no Silver entity/quarantine table or publishing
completion is written. Pin references remain in /runtime/silver/inputs.

Commit 449d5ff records strict types/PII setup. Upcoming P4B-05 adds products/returns
and full relational DQ. Quarantine persistence, global dedup/contributions/correction
ledger and publish recovery remain P4B-06/07. Hosted results for these changes have
not been checked; the user's earlier CI success applies to preceding work.

## Five-entity publication implementation checkpoint

Products/returns parsing, JSONL expansion and five-entity relational DQ are implemented.
The first real Walmart publish/replay passed with 25 orders/items, 5 customers,
9 products and 2 returns. Contributions, materialized entities and quarantine use
separate Iceberg snapshots; the committed publication ledger is the visibility boundary.
Unit suite currently passes 89 tests, including source snapshot ancestry and alias/
embedded-parent validation. Extended matrix and correction/failure tests are ongoing;
this checkpoint does not close Phase 4B.

## Matrix and source reconciliation — 2026-11-13

Small fixture: all 15 channel/date slices and an identical-pin replay passed. Source
key comparisons for every entity yield orders/items/customers/products/returns =
375/375/25/88/30. Default reference: all 15 slices passed, 3,000 orders per channel,
15,000 total. Exact authoritative source keys and DECIMAL totals/refunds by currency
match: 15,000/15,000/1,000/100/1,500. Walmart JSONL is excluded.

Spark DQ fixture checks pass: duplicate occurrences reduce deterministically under
reordering/repartition, identical IDs in two channels stay separate, malformed/scalar
roots generate no phantom children, missing products cascade to dependent records,
and excessive refunds reject explicitly. Original real JDBC correction/recovery
checks pass all three failure boundaries. Extended privacy/catalog/ownership checks
and final restart/regression are still being finalized.

Current make check: 98 tests pass plus Python/SQL/YAML lint. New coverage includes
Unicode/optional field validation, missing key/version, incomplete table inventory
and stale snapshot ancestry. No new hosted CI result is claimed.
