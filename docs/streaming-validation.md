# Streaming validation (Phase 7)

Evidence for Kafka + Flink streaming. Commands run sequentially; raw logs stay local.

## P7-01 — Preflight baseline (2026-11-19)

- Start point: `v0.6-query` + 9 later commits (hosted-CI fixes and evidence, Phase 7 plan),
  all pushed; working tree clean; `make check` green (234 passed, 1 skipped).

### Batch baseline (Phase 7 must not change any of it)

Run IDs recomputed from the generator configs:

| Dataset | Config | Run ID |
|---|---|---|
| small fixture (375 orders) | `generators/fixtures/bronze.yaml` | `186ae107d0793d2ee237` |
| default reference (15,000 orders) | `generators/config.yaml` | `b1e64afaff0176de180a` |
| orchestration e2e (300 orders) | `generators/fixtures/orchestration.yaml` | `5f552019ee41c24af95f` |

Ledger SHA-256, read from the `lakehouse-config` volume through a read-only mount:

| Ledger | SHA-256 |
|---|---|
| `silver/publication/ledger.json` | `51d0f9d6b6484afe85bfcf7fd96feb33029962e003f505cdbbe0a2d024dd49f2` |
| `gold/publication/ledger.json` | `be2e5d29338d192d4c48c45f9d610e9bdd027d39966cac9bdd4d5f0f338bd085` |
| `dp3/publication/ledger.json` | `b2d6892e779ebe1623d10546b278e2dd0a6de60d5ef3440f04c8d48cdc1cc19a` |
| `warehouse/export/ledger.json` | `cfa1cdf5b16f5b3422dbdb171ad8e311d97ec6b19ba598799fd1a597270c217b` |

(DP3 equals the state after the Phase 6 P6-08 publish of 2026-01-10 for the e2e dataset.)

`GeneratorConfig.run_id` hashes the whole model, including the reserved stream fields
(`late_rate`, `late_max_minutes`, `burst_multiplier`). Phase 7 must keep them and their defaults
unchanged; stream settings go into a separate model.

### Object storage

- Buckets: `raw`, `lakehouse`, `checkpoints`. `raw` top level: `_smoke/`, `manifests/` and one
  prefix per channel (`amazon/`, `lazada/`, `shopee/`, `tiktok_shop/`, `walmart/`) used by batch
  Bronze. `checkpoints` holds only `_smoke/`.
- Policies: `generator` (list `raw`; get/put `raw/*`, no delete), `spark` (read `raw/*`;
  read/write/delete `lakehouse/*` and `checkpoints/*`), `trino` and `lakekeeper` as in ADR-0004 and
  ADR-0015.
- Needed for Flink (P7-03): a new user `flink`, not root, allowed to list `raw` and
  `checkpoints`, put/get/delete only under a new top-level `raw/stream/*` (output and
  in-progress parts) and `checkpoints/flink/*`. A separate prefix keeps stream output out of the
  batch channel prefixes; Spark already reads `raw/*`, so Bronze ingestion needs no new grant.

### Memory budget

Host: 4 CPUs, 11,963 MiB visible to WSL. With only `core` running (Spark, Airflow, Trino
stopped): PostgreSQL 48 MiB, MinIO 335 MiB; host used 2,982 MiB, available 8,980 MiB.

| Service (profile `stream`) | Proposed `mem_limit` | Notes |
|---|---|---|
| Kafka (KRaft, single broker) | 1,024 MiB | heap ~512 MiB |
| kafka-ui | 512 MiB | JVM, observation only |
| Flink JobManager | 1,024 MiB | `jobmanager.memory.process.size` below the limit |
| Flink TaskManager | 2,048 MiB | 2 slots, small managed memory (HashMap state) |
| **Total** | **4,608 MiB** | plus core ~1.1 GiB of limits → fits the ~9 GiB available |

Spark, Airflow and Trino stay stopped while Flink is measured; Bronze ingestion of Flink output
runs afterwards with `up-batch` (Kafka/Flink stopped or idle).

### Placeholders to replace

- `make up-stream` and `make gen-stream` still `exit 2` (shared rule with `up-cdc`, `up-gov`).
- `generators/stream/`, `flink/jobs/amazon_late/`, `flink/jobs/tiktok_dedup/` contain only
  `.gitkeep`. The reserved generator fields are not read by any batch code.

## P7-02 — Decisions

[ADR-0016](adr/0016-streaming-kafka-flink.md): Kafka 4.3.2 (KRaft), kafka-ui v1.5.0, Flink 2.2.1 with
`flink-connector-kafka:5.0.0-2.2`, jobs in Java (owner decision; PyFlink would add ~300 MB of
libraries plus Beam/pandas/pyarrow and a Python worker per slot), `kafka-python` for the
generator, JSON Lines output, watermark 2 min / allowed lateness 20 min with margin-separated
delay classes, dedup TTL 30 min. Event contract in [data model](data-model.md#streaming-events-phase-7).

## P7-03 — Stream profile

Images (pulled 2026-11-19):

| Image | Digest | Size |
|---|---|---|
| `apache/kafka:4.3.2` | `sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7` | 227 MiB |
| `kafbat/kafka-ui:v1.5.0` | `sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1` | 212 MiB |
| `flink:2.2.1-java17` | `sha256:55ccb2ad4b98362c3f3503c5a6d7cd00b92eb25e38815ddaf0be5903ade54ffe` | 634 MiB |
| `maven:3.9.16-eclipse-temurin-17` (build only) | `sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584` | 222 MiB |

- `docker/compose.stream.yml` (profile `stream`): Kafka KRaft single node (`HOST` listener
  `127.0.0.1:9092`, `INTERNAL` `kafka:29092`, controller internal), kafka-ui read-only on
  `127.0.0.1:8085`, Flink JobManager on `127.0.0.1:8082`, TaskManager with 2 slots; `mem_limit`,
  healthchecks, named volume `kafka-data`. The built-in `flink-s3-fs-hadoop` plugin is enabled;
  S3 credentials come only from the container environment.
- `make env-stream` appends images, a random KRaft cluster id and the MinIO user `flink` secret
  without rotating existing lines (unit test). `make init-stream` (idempotent, ran twice): 4 topics
  + `_smoke.stream` with the configured partitions/retention, user `flink` with policy
  `raw/stream/*` + `checkpoints/flink/*`.
- `make smoke-stream`, 18 checks, all pass after `up-stream` from a stopped stack (89 s), after a
  second `init-stream` and after `restart-stream`:
  topics and partitions, produce/consume on the internal listener, host port 9092, kafka-ui health,
  Flink 2 slots, the bundled WordCount job writing a file to `s3://raw/stream/_smoke/` through the
  plugin, user `flink` allowed in its prefixes and denied in `raw/_smoke`, `lakehouse` and for
  reading a batch manifest, smoke objects cleaned, the S3 secret absent from Flink logs.
- Memory after the smoke (stream + core): TaskManager 546 MiB, JobManager 298 MiB, kafka-ui
  268 MiB, Kafka 434 MiB, MinIO 323 MiB, PostgreSQL 50 MiB; host used 4.5 GiB, available 7.2 GiB.
- Found while writing the smoke: `mc find` returns nothing for user `flink` outside its prefixes
  (it needs object reads), so a probe based on it passed without testing anything; the probe now
  lists with `mc ls --recursive` and fails when no key is found.

## P7-04 — Stream generator and ground truth

- `generators/stream/` with `StreamConfig` separate from the batch model; unit tests (9) cover:
  the three batch run IDs unchanged, determinism (same seed → identical sends and ground truth;
  another seed differs), counts per configuration, delay ranges and the watermark-reachability
  rule for late events, windows covering exactly the accepted events, the event contract for valid
  records and its violation for malformed ones, rejection of streams too short for the requested
  late events, publish-once semantics (send → marker; rerun skipped; interrupted publish refused)
  and the CLI.
- Found while sizing fixtures: the first defaults (5,000 events at 120/min) left no room for late
  events and silently produced none; the generator now rejects such streams and the default
  density is 60 events per simulated minute.
- Live run (run `e24dea73b7f127da7c71`, small fixture with `--seed 1001`, `STREAM_RATE` 500):
  6,312 records published in 12.6 s; `make check-stream-source` read both topics back and matched
  the ground truth exactly — Amazon 3,000 valid / 3,000 unique / 6 malformed, TikTok 3,300 valid /
  3,000 unique / 300 retries (equal retry-id SHA-256) / 6 malformed. A second `make gen-stream`
  printed `stream_publish_skipped` and sent nothing.
- The canonical small fixture (seed 7, run `d1a1b7eed61ecd0edae9`) is kept unpublished for the live
  Flink acceptance run (P7-05…P7-07), where the jobs must be running before the generator.

## P7-05 / P7-06 — Flink jobs (Java)

- Maven project `flink/jobs/` (Flink 2.2.1 provided, `flink-connector-kafka:5.0.0-2.2` and Jackson
  2.19.4 shaded into a 24 MB jar), built only in Docker (`docker/images/Dockerfile.flink`, build
  stage runs JUnit: `RulesTest` 3, `EventParserTest` 2, `JobArgsTest` 2, all pass). The image
  `omnichannel/flink:2.2.1-jobs` adds the jar to `/opt/flink/usrlib/`.
- `amazon_late`: validate → DLQ (transactional Kafka sink) | classify against the current watermark
  (Flink window boundaries: maxTimestamp = end − 1) → too late to branch `late` | accepted → branch
  `accepted` and 5-min windows with 20-min allowed lateness → branch `windows` (`firing_seq`).
- `tiktok_dedup`: validate → DLQ | keyBy(`event_id`) + ValueState TTL 30 min (processing time) →
  retries to branch `duplicates` | unique → classify (1-min window, no lateness) → `accepted`,
  `late`, 1-min `windows`.
- One `FileSink` per job: JSON Lines, rolled on checkpoint, bucket
  `<channel>/<branch>/run=<id>/date=<d>`; each event line keeps the original event plus
  `_stream_branch`, `_delay_class`, `_watermark_ms`, Kafka partition/offset and `_processed_ms`.
- Lifecycle: `make stream-start` (start offsets `latest` by default), `make stream-stop`
  (`flink stop --drain`: fires all windows, final savepoint commits the files),
  `make verify-stream` (read-only).
- A checkpoint trigger right after submission reports "task not being executed" once per start;
  it is the deployment race, later checkpoints complete.

### Live acceptance and what it found

| Run | Contract / rate | Result |
|---|---|---|
| `d1a1b7eed61ecd0edae9` (seed 7) | v1, 500 rec/s | counts right, 9 of 90 intended late events classified on time — correct for the recorded watermark (rule check passed), watermark lag median 58 s, max 331 s |
| `a8c409e4f0c1ffdc2676` (seed 8) | v1, 50 rec/s | all checks pass, but late events saw up to 88 s of lag with a 1-min margin: lag peaks of ~100–133 s recur every ~30 s of wall time (checkpoints) |
| **`e7a188e49b1e4042ef15` (seed 7)** | **v2 (4-min margins), 50 rec/s** | **0 failures**: Amazon 2,850 on time / 90 late accepted / 60 too late, TikTok 3,000 unique / 300 duplicates / 0 late, final windows equal, DLQ 6 + 6, nothing lost; lag median 28 s, max 100 s |

`make verify-stream` checks per channel: committed files only (no hidden in-progress parts),
no duplicate `event_id` in `accepted`, SHA-256 of the event-id set per class (on time, late
accepted, too late, duplicates) equal to ground truth, the rule per record from `_watermark_ms`,
the last firing of every window, DLQ counts (read_committed) and that accepted + late +
duplicates equals the valid records. It also regenerates the ground truth and refuses to verify
if it differs from the S3 copy. The v1 runs stay in Kafka/S3 as history; they no longer
regenerate under v2 and are not verified again.

## P7-07 — Checkpoints, recovery and exactly-once

Checkpoints every 30 s, EXACTLY_ONCE, HashMap state, `s3://checkpoints/flink/checkpoints`, retained on
cancellation (2 per job). JUnit now includes Flink operator harness tests (`FunctionsTest`: dedup
with processing-time TTL — retry inside the TTL goes to duplicates, after it passes again; classify
records the operator watermark and splits too-late; aggregate in cents): 10 tests, all pass.

| Experiment | Run | What happened | Verify |
|---|---|---|---|
| A: `docker kill` TaskManager 56 s into publishing | `d02620420913a7040f92` (seed 9) | Docker did not restart a killed container (manual stop); `docker start` → both jobs running again 5.7 s after the kill, restored from the last checkpoint | `--failover`: 0 failures — accepted 2,940 unique, late 60, duplicates 300, DLQ 6 + 6 (transactions: no doubled DLQ records), windows consistent, nothing lost |
| B: restart JobManager + TaskManager 56 s into publishing | `e594e90faa530b1e0686` (seed 10) | 0 jobs after the restart (no HA); `make stream-resume` from `chk-2` of each job; running again 38.9 s after the restart | `--failover`: 0 failures, same totals |
| C: `make restart-stream` (Kafka, kafka-ui, Flink) while idle | — | topics, data and group offsets kept | `smoke-stream` 18/18, `check-stream-source` ok, strict `verify-stream` of `e7a188e49b1e4042ef15` ok |

Finding: in A, 53 of 90 intended late-accepted events were classified on time (B: 77), all
correctly for their recorded watermark. Watermarks are not part of checkpoints, so after a restore
the watermark restarts from the minimum (lag up to 739 s right after B's restore). Exactly-once
holds; exact class counts are only expected for failure-free runs (ADR-0016 §7). The verifier got a
`--failover` mode (completeness, disjoint branches, windows equal to the accepted records) with unit
tests showing that a reclassification fails strict mode but passes failover mode, while a lost or
duplicated record fails both.

## P7-08 — Into Bronze

- `make stream-close` (generator credentials: read `raw/stream`, write `raw/manifests`) wrote
  `stream_output_manifest.json` for `e7a188e49b1e4042ef15` (24 files) and `d02620420913a7040f92`
  (23 files); a second close produced the same immutable object.
- `dp1_bronze --source stream` (`make bronze-stream STREAM_RUN_ID=… STREAM_RUN_DATE=2026-02-01`) on
  `e7a188…`: amazon 3,000 rows, tiktok_shop 3,300 rows (`bronze_completed`); second run
  `bronze_skipped` for both; `--verify` `bronze_verified` for both. Content read back with Spark:
  amazon `accepted` 2,940 + `late` 60, tiktok_shop `accepted` 3,000 + `duplicates` 300, every row
  `_parse_status = valid`, accepted event ids distinct (2,940 / 3,000), one consistent audit tuple.
  These equal the Flink output and the ground truth's valid records.
- Airflow: DAG `dp1_stream_bronze` (sensor on the output manifest, two Spark tasks in the
  `spark_driver` pool) ingested `d02620…` for 2026-02-01, run `manual__p708_stream_seed9`
  success; a following `make bronze-stream` for the same slices was skipped and `--verify`
  returned 3,000 / 3,300 rows. `make test-dags` 11 passed.
- Batch unchanged: Silver/Gold/DP3/warehouse ledger SHA-256 identical before and after; Bronze gained
  only the two new completion records per ingested run.
- Not a pipeline issue: an ad-hoc Spark script started without `--name omnichannel-spark-driver`
  waited forever for executors (the workers connect back to that host name); rerun with the name.

## P7-09 — Benchmark

[Streaming benchmark](benchmarks/streaming.md): 105,200 records per run (50k events per channel,
10% retries, 5% late, bursts). Live at 500 rec/s (`66622bf259497404b72c`): processing latency p50
7–45 ms / p95 ≈ 110 ms, committed-file latency ≤ 33 s (30 s checkpoints), checkpoints 16/16 ok
(≤ 0.8 s, state ≤ 2.6 MB), no backpressure. Backlog (`d754917ef62734ed7102`): ≈ 20,000 rec/s per job
on one slot, TikTok dedup operator saturated with light source backpressure. Both runs: relaxed
verification 0 failures; exact class counts differ (0.05% live, much more in backlog) because of
watermark lag, as ADR-0016 §6 predicts.

## P7-10 — CI, documentation and handoff

- Live acceptance packaged as `make stream-acceptance` (`docker/stream/acceptance.sh`): run
  `0c3e204896cac07752c3` (seed 13) — strict verification 0 failures, run closed (26 files).
- Manual workflow `.github/workflows/stream.yml`: env, image builds (JUnit inside), `up-stream`,
  `smoke-stream`, `stream-acceptance`, `restart-stream` + smoke, `up-batch`, `bronze-stream` twice
  (second skips) and `--verify`; CI output only allowlisted events (stream check names, channel/job
  names and counts; unit-tested). The hosted run is not claimed here.
- README ("Chạy streaming"), runbook, data model, ADR-0007/0016, implementation status, CLAUDE.md
  and the [Phase 8 handoff](phase8-handoff.md) are updated.

### Hosted CI fix

The first hosted `Kafka and Flink streaming` run
([44502716790](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44502716790), commit
`27bae0e`) failed in `smoke-stream`: the read-denial probe needed an existing batch manifest, which a
fresh runner does not have (it fails by design when no key is found). The probe now writes its own
object `raw/_smoke/flink_read_probe` with the MinIO root user, checks that user `flink` cannot read
it, and removes it; the check still fails if the probe cannot be created. Local `smoke-stream` 18/18.

Hosted run after the fix: `Kafka and Flink streaming` [44509252927](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44509252927) on commit `ae161cd` (2026-11-19)
succeeded, 15/15 steps in ~12 min on a fresh runner: image builds with JUnit, `up-stream`,
`smoke-stream`, `stream-acceptance` (jobs first, 6,312 records at 50 rec/s, drain, strict
verification against the ground truth of `e7a188e49b1e4042ef15`, close) in 3 min 16 s,
`restart-stream` + smoke, `up-batch`, `bronze-stream` (ingest), `bronze-stream` again (skip) and
`--verify`.
