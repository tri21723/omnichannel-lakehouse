# Batch and Walmart load generators

## Run

```sh
make setup
make env
make up-core
make build-generator
make gen-sources
make verify-sources
make gen-sources        # same run; SQL changed=0, no replacement S3 objects
```

For an existing core install, run `make init-core` once to grant the generator
TEMPORARY on database sources for COPY staging. No schema or data deletion is needed.

Separate operations: `make gen-batch` (S3), `make gen-load` (PostgreSQL).
Local-only: `make gen-local GEN_ARGS='--seed 7 --days 1'` writes under
`.local-data/generated/<run_id>/`. `make verify-sources` verifies existing datasets;
it does not generate missing files or repair business data.

Container overrides use the same arguments for generation and verification:

```sh
make gen-sources GEN_ARGS='--seed 7 --start-date 2026-02-01 --days 2'
make verify-sources GEN_ARGS='--seed 7 --start-date 2026-02-01 --days 2'
```

Other controls are in `generators/config.yaml`. Rebuild the generator image after
changing source/config. For custom config, put YAML under generators/ before building,
then pass `--config generators/<name>.yaml`. Late/burst options are reserved for Phase 7.

The one-shot generator runs with its own S3/DB credentials and a 512MB limit.
It writes local artifacts to the `generator-data` named volume. It does not start
Spark or other batch services. Core must already be initialized and healthy.

## Dataset contract

- 3 days × 1,000 unique orders × 5 channels = 15,000 unique orders by default.
- dup_rate=0.04 adds 600 duplicate rows => 15,600 physical rows, 3,120 per channel.
- 50% of unique orders target one hot seller/product. 10% have a return.
- Amazon changes buyer_id to customer_id and adds fulfillment_type on day index 2.
- Shopee timestamps use +0700; Lazada uses VND and omits optional product brand.
- Each order has one item in v1. Synthetic return timestamps span 1–14 days after
  order time; fixtures include full lifecycle, not an as-of snapshot. Later feature
  and label pipelines must apply event-time cutoffs and label maturity rules.

S3 objects:

```text
s3://raw/<channel>/dt=YYYY-MM-DD/run_id=<id>/part-00000.jsonl
s3://raw/manifests/<id>/stream_manifest.json
```

Manifest includes contract/config, seed, logical generation time, SHA-256/bytes/rows
for every JSONL file, physical and unique counts, duplicate counts, hot seller/return
counts and event-time range. Source duplicates remain in JSONL for Silver dedup tests.

Publishing is create-only (`IfNoneMatch=*`); existing bytes must match the manifest.
Manifest is published last, after each JSONL upload has been read back and verified.
Reconciliation checks actual remote content and file inventory, not only object metadata.
Consumers must select a manifest/run explicitly, especially after changing config.

Walmart loads the exact JSONL exports into five relational tables via COPY staging
and transactional upserts. Repeated entities are deduplicated before upsert; unchanged
rows are not updated. Full-row reconciliation executes inside the transaction.
Only TEMP privilege is added; generator cannot create permanent source tables.

S3 publish and PostgreSQL load are separate transactions. If the SQL load fails after
S3 succeeds, retry the same run; the S3 manifest means S3 completion only.
Different config produces a new dataset and new IDs, not an incremental update.
Future CDC mutation jobs must be a separate explicitly designed workflow.

## Validation and troubleshooting

`make check` includes deterministic replay, config validation, duplicate/skew/return
counts, monetary consistency, schema drift, local corruption, remote corruption,
manifest ordering, upload failure and CLI checks. `make verify-sources` runs S3 and
PostgreSQL reconciliation against the local core stack.

- Permission denied for temporary tables: run `make init-core`.
- Missing env keys: compare names with .env.example; do not log credential values.
- Missing local run: use exactly the same config/CLI overrides as generation.
- Immutable object mismatch: investigate corruption or a contract-version mismatch;
  do not delete or overwrite raw objects to hide the problem.
- Docker helper error: use the documented WSL workaround in docs/runbook.md.

No million-row benchmark is claimed here.

## Stream generator (Phase 7)

`generators/stream/` (ADR-0016) is separate from the batch model so batch run IDs never change
(unit-tested). `StreamConfig` (`generators/stream.yaml`, small fixture
`generators/fixtures/stream-small.yaml`) yields `stream_run_id` = 20 hex of SHA-256 over contract
`stream-v2` + config (v1 had narrower late-class margins).

- Simulated timeline per channel (`events_per_minute`, default 60); TikTok bursts compress spacing by
  `burst_multiplier`. Each record is sent at `event_ts + delay`, and sends are ordered by send time.
- Amazon delay classes (on time ≤ 1 min, late 11–16 min, too late 31 min…`late_max_minutes`) are
  placed only where enough later events exist to move the watermark past them; a stream too short
  for the requested late events is rejected instead of silently producing fewer.
- TikTok retries repeat `event_id` and payload 5 s–5 min after the first send (`dup_rate`).
  Malformed records (`malformed_rate`, four kinds) carry the key `<run>-<channel>-malformed-<n>`.
- Ground truth `s3://raw/manifests/<run>/stream_ground_truth.json`: per channel records, unique
  events, retries, malformed, class counts, SHA-256 of the event-id sets per class, expected window
  counts/amounts for accepted events, burst layout.
- `make gen-stream` publishes once: ground truth (immutable) → Kafka (idempotent producer,
  `acks=all`, key `order_id`, paced by `STREAM_RATE`) → `stream_published.json`. A rerun sends
  nothing; ground truth without the marker (interrupted publish) is refused — use a new seed.
- `make check-stream-source` reads the topics back and compares counts and retry sets with the
  ground truth (read-only). Both accept `GEN_STREAM_CONFIG` and `GEN_STREAM_ARGS`.
