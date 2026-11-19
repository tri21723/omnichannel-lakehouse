# ADR-0016: Streaming with Kafka and Flink (Phase 7)

- Status: Accepted for implementation (Phase 7).
- Context: the batch chain is complete, orchestrated and queryable (ADR-0007…0015). Phase 7 adds
  the plan's streaming flows S1–S3: order events for Amazon (late arrival) and TikTok Shop
  (duplicates, bursts) go through Kafka and Flink to files that enter Bronze. Preflight
  ([streaming validation](../streaming-validation.md) §P7-01) measured ~9 GiB available with only
  `core` running.

## 1. Runtime and versions

| Component | Pin | Notes |
|---|---|---|
| Kafka | Apache Kafka **4.3.2**, image `apache/kafka:4.3.2` | KRaft, single combined broker/controller |
| kafka-ui | `kafbat/kafka-ui:v1.5.0` | maintained fork of `provectus/kafka-ui` |
| Flink | **2.2.1**, image `flink:2.2.1-java17` | session cluster: 1 JobManager, 1 TaskManager |
| Flink Kafka connector | `org.apache.flink:flink-connector-kafka:5.0.0-2.2` | 5.0.0 supports Flink 2.1/2.2 only, so Flink 2.3 is not used yet |
| Job build | `maven:3.9.16-eclipse-temurin-17` (build stage only) | jar built inside Docker; nothing installed on the host |
| Generator Kafka client | `kafka-python` 3.0.x (pure Python, ~1 MB) | tested against brokers up to 4.3, idempotent producer |

Kafka 4.3.2 (patch of a 6-month-old minor) is preferred over 4.4.0 (released 2026-10-22) because it
is the newest line that kafka-python lists as tested. Flink 2.2.1 is the newest release the Kafka
connector supports. Details and sources: ADR-0001.

### Flink jobs in Java, not PyFlink

PyFlink 2.2.1 supports what is needed (side outputs, state TTL, timers, FileSink), but it pulls
`apache-flink-libraries` (~300 MB), Apache Beam, pandas and pyarrow, grows the image to
~1.5–2 GB and runs a Python worker beside every TaskManager slot. That costs memory in a 4.5 GiB
budget and distorts the throughput/latency measurements Phase 7 must report. Jobs are therefore
written with the Java DataStream API (decision confirmed by the project owner, 2026-11-19).
Pure logic (classification, dedup decisions, aggregation keys) is unit-tested with JUnit inside
the Maven build stage; Python tests cover the generator and the verifier.

## 2. Services (profile `stream`, `docker/compose.stream.yml`)

| Service | Host port | `mem_limit` | Notes |
|---|---|---|---|
| `kafka` | `127.0.0.1:9092` | 1 GiB | listeners: `HOST` (advertised `localhost:9092`), `INTERNAL` (`kafka:29092`), controller (internal only); heap 512 MiB |
| `kafka-ui` | `127.0.0.1:8085` | 512 MiB | read-only mode where supported |
| `flink-jobmanager` | `127.0.0.1:8082` → 8081 | 1 GiB | REST/UI; submits jobs from the built jar |
| `flink-taskmanager` | — | 2 GiB | 2 slots; restart policy `unless-stopped` for the recovery test |

Every service has a healthcheck. `make up-stream` starts `core` + `stream` only; Spark, Airflow
and Trino stay stopped while Flink is measured.

## 3. Topics

- `amazon.order-notifications`, `tiktok_shop.order-webhooks`: 3 partitions, replication 1,
  key `order_id`, `retention.ms` 7 days, `cleanup.policy=delete`.
- Dead-letter topics `<topic>.dlq` (1 partition, 7 days) receive records that fail parsing or
  schema checks, written by Flink with a transactional (exactly-once) Kafka sink.
- Topics are created by an idempotent init step (`kafka-init`); automatic topic creation is off.
- Plaintext, no authentication: the broker is bound to loopback (local development only).
- Schema Registry is out of scope; events carry `schema_version`.

## 4. Event contract (`schema_version` 1)

JSON object per Kafka record: `schema_version`, `event_id` (unique per logical event),
`event_type` (`order_created`, `order_updated`), `stream_run_id`, `channel`, `order_id`,
`event_ts` (UTC, ISO-8601, the business time), `emitted_ts` (UTC, generator send time),
`currency`, `amount` (decimal string), `payload` (channel-specific object). A retry repeats the same
`event_id` and payload. Records without a valid `event_id`, `event_ts`, `channel` or
`schema_version` go to the DLQ. See [data model](../data-model.md#streaming-events-phase-7).

## 5. Generator and ground truth

- A separate `StreamConfig` model; `GeneratorConfig` and its reserved stream fields stay unchanged
  so the three batch run IDs do not change. `stream_run_id` = first 20 hex of SHA-256 over
  contract `stream-v1` + canonical config.
- Same seed → same events, same order, same timestamps. The generator advances a simulated
  clock and emits each event when its delay class says so (§6).
- Ground truth `s3://raw/manifests/<stream_run_id>/stream_ground_truth.json` (not
  `stream_manifest.json`, which is the batch manifest): events sent, unique `event_id`s,
  retries, events per delay class and per window, malformed records. It is written once; a rerun
  with different content is refused.

## 6. Event time, watermarks and lateness (both jobs)

- Event time from `event_ts`. `WatermarkStrategy.forBoundedOutOfOrderness(2 min)` with idle
  timeout 1 min; tumbling event-time windows of 5 min (Amazon) and 1 min (TikTok).
- Amazon: `allowedLateness(20 min)`; events behind that go to a side output persisted as the
  `late` branch. Late firings update a window (each firing carries `firing_seq`; the last one wins).
- Delay classes generated with margins so that classification is deterministic despite partition
  interleaving (delay *d* = generator clock at send − `event_ts`, bound *B* = 2, window 5,
  allowed lateness *L* = 20, minutes):
  with watermark ≈ now − *B* and `event_ts` < window end ≤ `event_ts` + 5:
  - on time: *d* ≤ 1 — the window is still open when *d* ≤ *B* (margin 1);
  - late, accepted: 8 ≤ *d* ≤ 15 — the window has fired when *d* ≥ *B* + 5 = 7 (margin 1) and is
    still within lateness when *d* < *B* + *L* = 22 (margin 7);
  - too late: 30 ≤ *d* ≤ `late_max_minutes` (60) — beyond lateness when *d* ≥ *B* + 5 + *L* = 27
    (margin 3).
  The source watermark is the minimum over partitions (per-split watermarks). When Flink reads
  a backlog, whole fetch batches of one partition can run ahead of another, so the watermark at a
  record depends on runtime interleaving. Therefore:
  - the job classifies each record **before** the window operator against the current watermark
    and writes that watermark into the output record (`_watermark_ms`); the verifier checks the
    rule per record (always) and that nothing is lost (always);
  - acceptance runs are **live**: the job runs first and the generator publishes at a bounded rate,
    so partitions advance together; then class counts must equal ground truth exactly;
  - backlog runs (benchmark) report class agreement but only assert the per-record rule.
  The generator keeps ≥ 30 events per simulated minute (default 60), so the gap inside one of 3
  partitions stays at seconds, far below the 1-minute margin, and late events are only chosen where
  enough later events follow to move the watermark past them.
- TikTok: `keyBy(event_id)`, `ValueState` with TTL 30 min (processing time, `OnCreateAndWrite`,
  never return expired). Retries are emitted within 5 min of the first send, far below the TTL.
  Duplicates go to a `duplicates` side output (not dropped). Limitation: TTL is processing time,
  so a job outage longer than the TTL could let a retry through; documented in the runbook.

## 7. Output, checkpoints and exactly-once

- Checkpoints every 30 s, `EXACTLY_ONCE`, HashMap state backend, stored in
  `s3://checkpoints/flink/<job>/`, retained on cancellation; restart strategy exponential delay.
- File output with `FileSink` (row format, JSON Lines, one line = the original event plus
  `_stream_branch` and processing metadata), rolled on checkpoint: files become visible only when a
  checkpoint commits, in-progress parts stay hidden. Path:
  `s3://raw/stream/<channel>/<branch>/run=<stream_run_id>/date=<event date>/part-…`.
  Branches: Amazon `accepted`, `late`, `windows`; TikTok `accepted`, `duplicates`, `windows`.
- JSON Lines instead of the plan's CSV/Parquet: payloads are nested, batch Bronze already stores
  raw JSON text (`_raw`, ADR-0007), and the Parquet writer would add Hadoop/Parquet jars for no
  analytical gain at the Bronze stage.
- S3 access through the bundled `flink-s3-fs-hadoop` plugin (supports recoverable writes) with a
  dedicated MinIO user `flink`: list `raw` and `checkpoints`; get/put/delete only `raw/stream/*` and
  `checkpoints/flink/*`.
- Kafka offsets are part of the checkpoint (committed back to the group only for monitoring).
  Source → checkpointed state → committed files/transactional DLQ gives exactly-once output.

## 8. Closing a run and entering Bronze

- A run is closed when the generator has finished, consumer lag of both jobs is 0 and a
  checkpoint completed afterwards (`make stream-close`). It then lists the committed files of the
  run and writes `s3://raw/manifests/<stream_run_id>/stream_output_manifest.json` (file, size,
  SHA-256, rows, branch) **after** all files exist.
- `dp1_bronze` gets a `stream` source that ingests from that manifest with the ADR-0007 rules
  (fingerprint, completion record, replay without duplicates) into
  `bronze.amazon_order_notifications_raw` and `bronze.tiktok_shop_order_webhooks_raw`; the branch
  is kept in `_stream_branch`. Bronze stays hidden from Trino (raw payloads). Details in an
  ADR-0007 addendum (P7-08). A DAG `dp1_stream_bronze` uses the `spark_driver` pool.

## 9. Verification

- `make smoke-stream`: produce/consume on a smoke topic, Flink cluster has a TaskManager, MinIO
  user `flink` cannot write outside its prefixes.
- `make verify-stream`: output files vs ground truth per class (accepted/late/too-late,
  unique/duplicates, DLQ), no duplicate `event_id` in `accepted`, read-only.
- Recovery: kill the TaskManager mid-run; after restart the totals still equal ground truth.
- Benchmark (100k events, 10% duplicates, 5% late, bursts): throughput, end-to-end latency,
  checkpoint duration/size, backpressure; reported in `docs/benchmarks/streaming.md`.

## 10. Consequences

- A second language (Java) and a Maven build stage enter the repo under `flink/`.
- `make check` does not run JUnit; `make test-flink` runs it in Docker and CI runs it in the
  stream workflow.
- The raw bucket gains the `stream/` prefix and two Bronze tables; batch data, run IDs and
  ledgers are unchanged.
