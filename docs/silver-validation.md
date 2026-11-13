# Silver validation — Phase 4B

Date: 2026-11-13. Local DoD complete; milestone v0.4b-silver.
Earlier normalization-only checkpoints have been superseded by five-entity publishing.
See ADR-0008, the physical SQL contract and gold-handoff.md for implemented semantics.

## Runtime and source selection

Local Docker: Spark 3.5.5, Python 3.11.11, Java 17, Iceberg 1.9.2, Lakekeeper 0.9.3,
PostgreSQL 16.15, MinIO. Versions/ports are unchanged. Two workers retain their
2-core/2-GiB resource limits; submitted jobs request two total cores and 1-GiB executors.
Only core and batch profiles are needed. No cloud resources or destructive reset.

Walmart JDBC is authoritative; four other channels use JSONL. Walmart JSONL is
excluded from Silver. Completed Bronze selection/batch/table inventory/content is
validated, then copied into immutable content-addressed pins. Readers use exact
snapshot IDs and filter the selected run/channel/date/source/batch, with no latest
fallback. Missing/expired snapshots fail closed. Old snapshots retain their original
ingestion timestamps after Bronze replay, while newly pinned snapshots change.

## Source key and monetary reconciliation

Both matrices passed all five channels × three dates and identical-pin replay.
Each entity's distinct source keys are compared in both directions to Silver;
monetary totals/refunds use DECIMAL and channel/currency grouping.

| Selected dataset run | orders | order_items | customers | products | returns |
| --- | ---: | ---: | ---: | ---: | ---: |
| small: 186ae107d0793d2ee237 | 375 | 375 | 25 | 88 | 30 |
| default: b1e64afaff0176de180a | 15,000 | 15,000 | 1,000 | 100 | 1,500 |

Default orders = 3,000 per channel. The 15,600 raw JSONL rows include intentional
duplicates; those remain in Bronze. Walmart JSONL is not a sixth authoritative source.
Profile/return counts come from actual source keys, not the order count. Two datasets
coexist on the same logical dates with disjoint run-prefixed business IDs.

Input occurrences summed once per selected input pin (15 pins per matrix), all valid
and zero rejected on the clean reference matrices:

| Dataset | orders | order_items | customers | products | returns |
| --- | ---: | ---: | ---: | ---: | ---: |
| small | 387 | 387 | 327 | 339 | 33 |
| default | 15,480 | 15,480 | 13,080 | 12,540 | 1,559 |

For orders, local duplicate losers = 12 small / 480 default. Embedded profile
occurrences and shared profiles across dates explain why input and global profile
counts differ; do not interpret their difference as invalid rows.

Commands: make integration-silver, make integration-silver-reference and
make reconcile-silver (set SILVER_RUN_ID for the default run). The real Spark
optimizer required eager localCheckpoint before comparing complex aggregate plans
with exceptAll; this preserves currency columns and bounds lineage without a runtime
version change. These are small correctness datasets, not skew/million-row benchmarks.

## DQ, privacy and accounting

make check: Python/SQL/YAML/shell lint and 98 unit tests pass, with no silent integration
skip reported as success. Docker integration is run separately using explicit targets.

make test-silver-dq passes on the latest image:

- Duplicate records reduce to one winner per natural key; conflicting same-version
  business rows select the same canonical hash winner after shuffle/repartition.
- Two channels using identical source IDs retain two distinct orders.
- Malformed/scalar root JSON produces rejected parents and no phantom children.
- Missing products cascade explicit product/order/item/return rejections.
- Excessive cumulative refund rejects rather than inflating monetary totals.

Pure parser tests cover missing/null fields, booleans/containers as IDs, Unicode,
nonfinite/overflow/extra-scale DECIMAL, invalid timestamps, Shopee +0700 crossing UTC
dates, nullable brand, Amazon buyer/customer aliases and conflicting embedded IDs.
Unknown fields stay in Bronze. Required item arrays, missing arrays and empty return
arrays have distinct outcomes. Return timestamps after the logical run date survive.
Return quantity checks are N/A: the source has no return quantity field.

HMAC uses required >=32-byte secret, channel/field domain separation and recorded key
version. make env-silver generates a persistent random local key; repeat initialization
does not rotate it. Missing/invalid key/version fails before Spark starts. Names/emails
never enter Silver schemas, quarantine payloads or structured logs. Quarantine stores
source references/fingerprints with sorted reason codes, one row per rejected physical
occurrence. Input = valid + rejected and valid = local winners + duplicate losers,
separately for every exploded entity. Global profile counts are a different metric.

## Corrections and recovery

The latest real-JDBC recovery suite passes scoped corrections, shared profile
retention, three partial-publish failure points and stricter schema/key/refund
verification. A real unavailable REST catalog preserves the completed publication;
natural-key ownership collision, writer conflict, NULL scope and mixed keys fail closed.

Owned fixture IDs use a separate run and '-silver-fixture-' prefix. No default source
row or published raw export is changed. Source changes are re-ingested/pinned through
Bronze. Checks include newer-date profile followed by older-date replay; invalid order
amount retracting only its own order/item/return; PII rejection references without
clear customer values; restoration resolving quarantine; known superseded and previously
unseen stale pins rejected by source snapshot ancestry; and explicit empty full slices.

Failure injection occurs after a contribution commit, during quarantine publication
and immediately before completion. attempt.json becomes failed, completed ledger
bytes remain unchanged, and its prior snapshots remain readable. Retry rebuilds from
committed contributions, verifies all outputs and atomically publishes a new ledger.
Tables already exactly matching the desired content can reuse their snapshots.
Writer conflicts, mixed PII keys, NULL scope and natural-key ownership are fail-closed.

## Completion and persistence gates

Final source reconciliation passes for both runs after extended recovery. Catalog,
master and both workers were restarted, then verify-silver passed before any Silver
rewrite. smoke-core, verify-sources, verify-lakehouse, verify-bronze with explicit
Walmart JDBC selection and test-bronze-contract all pass. Original default sources
retain 15,600 JSONL rows and 3,000 JDBC orders, with zero changed source rows.
TODO closes all 78 local checklist items; the whole Phase 4 remains open for Gold,
features/labels, export and later batch performance work.

The completed ledger contains all 15 output snapshots, content row/hash summaries,
file counts/bytes, per-input accounting, source snapshot references, policy/key version,
revision and publication duration. Read-back verifies exact schema, key uniqueness,
PII hash format, required keys, quarantine reasons, FK, order-line sum, refund cap and
return timestamps before visibility. Latest table heads are not a multi-table transaction.
Gold consumers must pin the completed ledger and read its snapshot IDs.

## Post-restart publication diagnostics

Read-only verify-silver after restarting catalog/master/both workers passes all 15
tables and 32 slice references (30 dataset slices plus two explicitly empty recovery
fixture slices). Global materialized counts are orders/items/customers/products/returns
= 15,375/15,375/1,025/188/1,530. Recovery fixture business rows are zero.

The last empty-fixture publication took 60.228 seconds on this local stack; its
completed table snapshots reference 38 current data files totaling 7,800,401 bytes
across all 15 tables. These figures exclude retained historical files and are not
a full-matrix duration or throughput benchmark. Per-table rows/hashes/files/bytes
and per-slice accounting remain in the completed ledger; Spark UI supplies ephemeral
stage/shuffle diagnostics. No large shuffle benchmark is claimed.

## CI and practical limits

.github/workflows/silver.yml adds manual Ubuntu 24.04 integration using the small fixture,
90-minute timeout, read-only repository permissions, PII-filtered diagnostics and cleanup
without volume deletion. ci_run.sh only emits allowlisted events/counts/numeric metrics;
raw diagnostics stay in ignored runner-local files. Hosted CI success for preceding
work was user-confirmed; no new hosted result is claimed for this Silver workflow.

This implementation serializes a local writer and reduces/replaces bounded complete
entity tables. It is not a distributed multi-writer design. Publication duration and
file metrics diagnose this local run; no large-scale throughput/shuffle benchmark,
compaction, snapshot expiry or orphan cleanup has been performed. Referenced snapshots
must be retained. No Gold/feature/label/export, orchestration, query, streaming/CDC,
governance or cloud deployment is included in this Phase 4B milestone.
