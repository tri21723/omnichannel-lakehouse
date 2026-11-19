# Streaming benchmark (Phase 7)

Kafka 4.3.2 + Flink 2.2.1 jobs `amazon_late` and `tiktok_dedup` (ADR-0016) on the local stack.
Machine-readable summary: [streaming.json](streaming.json). Raw samples stay in
`.local-data/stream/bench/`. Measured 2026-11-19 with `docker/stream/bench.py` (Flink REST and
Kafka group lag every 1–2 s) and `make verify-stream` (latency from `emitted_ts`).

## Setup

| Item | Value |
|---|---|
| Host | WSL2, 4 CPU, ~11.7 GiB for Docker; only `core` + `stream` running |
| Kafka | single KRaft broker, 3 partitions per topic, `acks=all` idempotent producer (kafka-python) |
| Flink | session cluster, 1 TaskManager × 2 slots (process 1,800 MiB), parallelism 1 per job |
| Checkpoints | every 30 s, EXACTLY_ONCE, HashMap state, MinIO `s3://checkpoints/flink` |
| Workload | `generators/fixtures/stream-bench.yaml`: 50,000 events per channel → 105,200 records; TikTok 5,000 retries (10%) and 4 bursts at 50× density; Amazon 2,500 late (1,500 accepted, 1,000 too late); 100 malformed per channel |

Two scenarios, each on its own stream run:

- **A — live**: jobs running first, generator paced at 500 records/s (`STREAM_RATE=500`).
- **B — backlog**: 105,200 records published unpaced with the jobs stopped, then both jobs started
  from the publish start timestamp (`STREAM_START=<ms>`).

## Results

| Metric | A live (500 rec/s) | B backlog |
|---|---|---|
| Publish time | 210.4 s | 18.6 s (≈ 5,700 rec/s producer) |
| Flink throughput | follows the producer (no lag build-up) | amazon 50,100 rec in 2.24 s ≈ **22,000 rec/s**; tiktok 55,100 rec in 2.79 s ≈ **19,700 rec/s** (from `_processed_ms` span) |
| Processing latency `emitted_ts → _processed_ms` | amazon p50 7 ms, p95 104 ms, max 3.0 s; tiktok p50 45 ms, p95 117 ms, max 3.0 s | queueing dominates (p50 20–32 s): not a latency measurement |
| Latency to committed file | p50 15–16 s, p95 31 s, max 33 s | p50 41–48 s, max 56 s |
| Checkpoints | 8 + 8 completed, 0 failed; duration avg 194 / 241 ms, max 525 / 809 ms | 1 + 1 completed, 151 / 184 ms |
| Checkpointed state | amazon ≈ 10 KB; tiktok up to 2.6 MB (dedup keys, TTL 30 min) | same order |
| Backpressure (max backPressuredTimeMsPerSecond) | 6 ms/s (none) | 56 ms/s on the TikTok source; dedup operator busy 1,000 ms/s |
| Memory (docker stats) | TM 799 MiB, JM 438 MiB, Kafka 533 MiB, kafka-ui 290 MiB | TM 892 MiB, JM 522 MiB, Kafka 577 MiB |

## Correctness under load

Both runs pass `make verify-stream --failover` with 0 failures: every valid event exactly once in
`accepted` ∪ `late`, no duplicate event id in `accepted`, TikTok duplicates exactly the 5,000 retries
(SHA-256 of the id set equal), DLQ exactly 100 + 100 (read_committed), windows equal to the accepted
records, every record classified correctly for its recorded watermark, nothing lost. So the DoD
items "no duplicates after Flink" and "late events follow the rule" hold at 500 rec/s and in
backlog mode.

Exact class counts (strict mode) do **not** hold at these rates:

| Amazon class | Expected | A live | B backlog |
|---|---|---|---|
| on time | 47,500 | 47,527 | 49,723 |
| late, accepted | 1,500 | 1,478 | 162 |
| too late | 1,000 | 995 | 115 |
| watermark lag p50 / max | — | 54 s / 983 s | 4,764 s / 13,362 s |

The watermark trails the ideal one by (records/s per topic) × (watermark interval + fetch +
checkpoint pauses) ÷ event density; at 500 rec/s that reaches minutes during checkpoints, so 27
records (0.05%) were classified less late than intended, 5 of them accepted into windows instead of
the `late` branch. In backlog mode whole fetch batches of one partition run ahead of the others.
This is the behaviour ADR-0016 §6 predicts; exact class acceptance is done live at 50 rec/s
(`e7a188e49b1e4042ef15`, 0 failures).

## Limits

- One TaskManager and parallelism 1: the backlog throughput is a single-slot figure on a laptop,
  dominated by the TikTok dedup operator; it is not a capacity claim.
- Commit latency is bounded by the 30 s checkpoint interval by design (files become visible on
  checkpoint); a shorter interval trades latency for more small files and checkpoint overhead.
- The source rate sampled every 1–2 s underestimates short peaks; throughput is taken from the
  processing-time span of the output instead.
- No threshold on wall-clock time gates CI; the CI workflow only checks correctness.
