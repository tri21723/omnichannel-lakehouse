# CDC validation (Phase 8)

Evidence for Debezium CDC of `walmart.orders`. Commands run sequentially; raw logs stay local.

## P8-01 — Preflight baseline (2026-11-19)

- Start point: `v0.7-stream` + later commits (hosted-CI evidence, Phase 8 plan), working tree clean;
  `make check` green.

### Batch baseline (Phase 8 must not change any of it)

Ledger SHA-256 (unchanged since Phase 7): Silver `51d0f9d6…dd49f2`, Gold `be2e5d29…38bd085`, DP3
`b2d6892e…dcc19a`, warehouse `cfa1cdf5…70c217b`. Run IDs: `186ae107d0793d2ee237`,
`b1e64afaff0176de180a`, `5f552019ee41c24af95f`.

Walmart JDBC source evidence of the completed Bronze slices (rows, first 12 hex of the stream
SHA-256), read from `/runtime/bronze/*.json`:

| Run / date | customers | orders | order_items | returns | products |
|---|---|---|---|---|---|
| `186ae107…` 01-01 | 5 `fa0a796e544e` | 25 `f2abf05b9629` | 25 `5acf3dd6c4ad` | 2 `ceabad5d43f5` | 9 `a72b8fc50cdb` |
| `186ae107…` 01-02 | 5 `fa0a796e544e` | 25 `a6cd87606409` | 25 `583a7a9bb8d9` | 2 `41dbc8acd4cf` | 10 `e63cb4e9d770` |
| `186ae107…` 01-03 | 5 `fa0a796e544e` | 25 `4f107db1ae6a` | 25 `1cae8200d542` | 2 `d50fd72102fd` | 8 `60e8eb4b561d` |
| `b1e64aff…` 01-01 | 200 `371274acb462` | 1000 `03fb6e33c941` | 1000 `8d59cfcdf618` | 100 `39839a486e38` | 20 `386d3e59d7b7` |
| `b1e64aff…` 01-02 | 200 `371274acb462` | 1000 `9611e3eec965` | 1000 `c07c5231e5e1` | 100 `9cc2fdeb6d11` | 20 `386d3e59d7b7` |
| `b1e64aff…` 01-03 | 200 `371274acb462` | 1000 `777201593716` | 1000 `279e8fe696a3` | 100 `1659c6b6fbc5` | 20 `386d3e59d7b7` |
| `5f552019…` 01-01 | 4 `5a96652813e7` | 20 `319d7a41d223` | 20 `3267b88a9107` | 2 `bd9b25960996` | 8 `96a0658a8d9c` |
| `5f552019…` 01-02 | 4 `5a96652813e7` | 20 `42606bf66371` | 20 `4fb151b1ba60` | 2 `e4db41c6d37f` | 10 `161ef74d6cb0` |
| `5f552019…` 01-03 | 4 `5a96652813e7` | 20 `07626b4a74b7` | 20 `3ebfc0ba9dbb` | 2 `4b797051027e` | 7 `a479cab18410` |

(Two empty slices of `152582d39cce1e0043fc` belong to the Phase 4 snapshot-probe fixture.)

Live guard: `docker/cdc/batch_guard.sql` computes row count + md5 of every Walmart table per batch
dataset prefix; the result is stored as `docker/cdc/batch_guard.baseline` and `make cdc-batch-guard`
fails on any difference (first run: ok). Per prefix: `186ae107…` 75 orders / 75 items / 6 returns /
5 customers / 16 products; `b1e64aff…` 3,000 / 3,000 / 300 / 200 / 20; `5f552019…` 60 / 60 / 6 / 4
/ 16. Bronze JDBC selects rows by the same `<run_id>-walmart-` prefix, so CDC scenarios on their own
prefix cannot change these slices.

### PostgreSQL readiness

| Item | Value |
|---|---|
| Version | 16.15 |
| `wal_level` / `max_replication_slots` / `max_slot_wal_keep_size` | `logical` / 10 / 5GB |
| Replication slots | none |
| Publication | `dbz_pub` **already exists** (core bootstrap `grants.psql`): `FOR TABLE walmart.orders`, insert/update/delete/truncate, owner `platform_admin` |
| `debezium_repl` | `REPLICATION`, not superuser, SELECT on `walmart`, **no CREATE on `sources`** → cannot create publications; the connector must use `dbz_pub` (`publication.autocreate.mode=disabled`) |
| `REPLICA IDENTITY` | `orders` FULL; `customers`, `order_items`, `products`, `returns` default |
| Table owner | `platform_admin` |
| `walmart.orders` rows | 3,135 (75 + 3,000 + 60 for the three datasets; the plan's 15,675 was the all-channel Gold count) |

`walmart.customers` (raw names/emails) is not in the publication, so PII cannot reach Kafka through
this connector.

### Memory budget

`core` + Kafka (no Flink/kafka-ui) + Lakekeeper + Spark master/2 workers, idle: Kafka 478 MiB,
MinIO 395 MiB, workers 186/217 MiB, master 180 MiB, PostgreSQL 68 MiB; host used 4.0 GiB, available
8.0 GiB. With a Spark job (workers ≈ 1 GiB each, driver ≈ 1.5 GiB) and Kafka Connect (planned
`mem_limit` 1 GiB), about 4 GiB stay free; Flink, kafka-ui, Trino and Airflow stay stopped.

### Placeholders

`make up-cdc` still `exit 2`; `spark/jobs/dp1_cdc/` contains only `.gitkeep`.

## P8-02 — Decisions

[ADR-0017](adr/0017-cdc-debezium.md): Debezium 3.6.3.Final from `quay.io/debezium/connect`
(`sha256:99aa63f4…5e1c`; Kafka Connect 4.3.0, Java 25, PostgreSQL JDBC 42.7.13), connector on the
existing `dbz_pub` (`publication.autocreate.mode=disabled`), `walmart.orders` only, decimals as
strings, heartbeat with a logical-message action query, no host port for Connect; Spark gains
`spark-sql-kafka-0-10` 3.5.5 + three jars (SHA-1 pinned); Bronze by `foreachBatch` MERGE on Kafka
coordinates (Iceberg does not document exactly-once for retried micro-batches); Silver keeps the
current state with soft deletes (owner decision). Pulling from quay.io through the Docker Desktop
proxy hit repeated `TLS handshake timeout`s on the quay CDN (also for a tiny busybox image); the
third attempt succeeded.

## P8-03 — CDC profile

- `docker/compose.cdc.yml` (profile `cdc`, merged with core + stream; only `kafka` is started from
  stream): `kafka-connect` (Debezium 3.6.3, distributed, internal topics `_connect.*`, `mem_limit`
  1 GiB, healthcheck, no host port) and `cdc-kafka-init` (topics `platform_cdc.walmart.orders` with 3
  partitions and `__debezium-heartbeat.platform_cdc`). Connector config
  `docker/cdc/connectors/walmart-orders.json` is registered with an idempotent `PUT`; the password is
  `${env:DEBEZIUM_REPL_DB_PASSWORD}` through `EnvVarConfigProvider` with an allowlist for that variable.
- `debezium_repl` may call `pg_logical_emit_message` (default EXECUTE), so the heartbeat action query
  needs no extra table or grant.
- Make: `env-cdc`, `config-cdc`, `up-cdc` (replaces the placeholder), `init-cdc`, `register-cdc`,
  `status-cdc`, `smoke-cdc`, `restart-cdc`; `down`/`logs` include `cdc`. Unit tests (3) for the
  connector file (no secret, orders only, no autocreate, heartbeat excluded, no `topic.creation.*`)
  and `env.py`.
- `make smoke-cdc`, 12 checks + batch guard: connector and task RUNNING, slot `dbz_walmart` active
  with `pgoutput`, publication = `walmart.orders` only, no customers topic, snapshot covered all 3,135
  orders, a probe (insert, two updates, delete on `cdcsmoke-<epoch>` ids with the `generator` role)
  arrives as `c,u,u,d,tombstone` in order, `total_amount` as a decimal string, heartbeat flowing,
  slot lag < 100 MB (632–2,944 bytes observed), password absent from Connect logs and only the
  placeholder stored in `_connect.configs`; `cdc-batch-guard` ok.
- Passed after the first fixes, after `make restart-cdc`, and after `make down` + `make up-cdc` from a
  stopped stack (76 s). Topic totals rose by exactly the 5 probe records per smoke (6,278 → 6,283 →
  6,288): no resnapshot on restart. Kafka Connect uses ~477 MiB.

Found while bringing it up:

1. A connector-level `topic.creation.enable` made Connect demand
   `topic.creation.default.replication.factor` (connector FAILED); removed.
2. The heartbeat action query emits a logical message that Debezium routes to
   `platform_cdc.message`; with broker auto-creation off the producer retried forever and blocked
   every later event. Fixed with `message.prefix.exclude.list=debezium-heartbeat`.
3. Because the snapshot-done offset was never committed while blocked, the restart repeated the
   snapshot (3,135 more `r` records) and the earlier probe changes appear only as the new snapshot's
   state — at-least-once behaviour now documented in ADR-0017.
4. The smoke compared a boolean cast as `t`; PostgreSQL prints `true`.

## P8-04 — CDC scenario generator

- `generators/cdc/` + `generators/cdc.yaml`; unit tests (7): determinism and seed dependence, every
  id in every statement starts with the scenario prefix and never a batch prefix, expected op
  counts, ground truth equal to replaying the transactions in memory, the concurrent pair on the
  same row with B winning, timestamp normalisation, and apply-once semantics against a fake database
  whose row locks really block the second session (skip on rerun, refuse interrupted runs or
  pre-existing scenario rows).
- Live (run `4f8191192d5abf939dbd`, 75 transactions): `make gen-cdc` → `cdc_scenario_applied`,
  `cdc-batch-guard` ok; `make check-cdc-source` → 35 live orders, checksum equal, the 5 deleted ids
  absent; a second `make gen-cdc` → `cdc_scenario_skipped`.
- On `platform_cdc.walmart.orders` the scenario prefix produced exactly the expected events:
  `c` 43, `u` 77, `d` 8 plus 8 tombstones. For the contested row the last two events are A
  (`shipped`, `10.00`) then B (`delivered`, `20.00`): commit order preserved, amounts as decimal
  strings.

## P8-05 — `dp1_cdc` into Bronze

- Spark image: + `spark-sql-kafka-0-10_2.12-3.5.5`, `spark-token-provider-kafka-0-10_2.12-3.5.5`,
  `kafka-clients-3.4.1`, `commons-pool2-2.11.1` (SHA-1 verified in the build, retries on Maven);
  the Airflow image copies the same jars. No Spark version change.
- `spark/lib/cdc.py` (pure, used as the UDF): Debezium envelope → `op`, `ts_ms`, `lsn`, `tx_id`,
  `before`/`after` as JSON text, `_parse_status` (`valid`, `tombstone`, `invalid` + reason: invalid
  key/JSON, bad op, missing LSN, key/image mismatch); MERGE SQL keyed on Kafka coordinates; offset
  contiguity check. 14 unit tests.
- `spark/jobs/dp1_cdc/main.py`: `availableNow`, `foreachBatch` MERGE into
  `lh.bronze.walmart_orders_cdc` (partitioned by day of the Kafka timestamp), checkpoint
  `/runtime/checkpoints/dp1_cdc`; `--mode verify` (read-only): no duplicate coordinates, offsets
  per partition gap-free from Kafka's earliest, no `invalid` record. Make: `bronze-cdc`,
  `verify-bronze-cdc`, `spark-submit JOB=dp1_cdc`.

| Step | Result |
|---|---|
| First run with 500 records per micro-batch, `docker kill omnichannel-spark-driver` after 4 batches (1,994 records written) | killed |
| Rerun, same checkpoint | 4,430 inserted; total 6,424 = every record on the topic (6,288 after the smokes + 136 scenario records) |
| `--mode verify` | ok: 0 duplicates, no gaps, 6,412 valid + 12 tombstones, 0 invalid |
| Rerun | 0 records |
| Full replay with a new checkpoint | 6,424 read, **0 inserted** |
| `--mode verify` again; `cdc-batch-guard`; batch ledgers | ok; ok; SHA-256 unchanged |

Found: the first attempt failed on `TABLE_OR_VIEW_NOT_FOUND cdc_batch` — the MERGE ran on the outer
session while the view was registered on foreachBatch's cloned session; the batch was not committed
in the checkpoint, so the fixed rerun picked it up again.

## P8-06 — Silver current state

- `spark/jobs/dp1_cdc/silver.py` (`make silver-cdc`, `make verify-silver-cdc`): latest valid Bronze
  event per order by `(lsn, offset)` → typed (`typed_row`: row image `before` for deletes,
  DECIMAL(18,2) from the decimal string, UTC timestamps, problems → `_error_reason`) → MERGE into
  `lh.silver.walmart_orders_cdc_current` only when strictly newer; deletes set `is_deleted`, a
  later insert revives the row; untypable or invalid records go to
  `lh.silver.quarantine_walmart_orders_cdc` (MERGE on Kafka coordinates).
- Unit tests: typing (delete uses `before`, +07:00 normalised to UTC, exact money) and seven
  quarantine reasons; the "strictly newer" rule gives the same final state for all 24 arrival orders
  of four events including a duplicate delivery; the MERGE never deletes rows.
- First run: 3,179 orders (3,135 batch orders + 40 scenario + 4 smoke probes), 3,170 live, 9 deleted,
  0 quarantined. Second run `changed: false`. `--mode verify`: one row per order and equal to the
  latest Bronze event per key.
- Found: after rebuilding the Spark image the workers must be recreated (`make up-batch`), otherwise
  executors import the old `spark/lib` (UDF `typed_row` not found).

## P8-07 — Reconciliation

`spark/jobs/dp1_cdc/verify_cdc.py` (`make verify-cdc`, scenario id derived from
`generators/cdc.yaml`; read-only): PostgreSQL `walmart.orders` (JDBC, `spark_reader`) vs Silver and
the scenario ground truth in S3, with the generator's checksum algorithm (cross-tested).

| Run | Result |
|---|---|
| Scenario 1 `4f8191192d5abf939dbd` | 8/8 checks: live ids/count/checksum equal PostgreSQL, deleted ids absent from the source, scenario 35 live + checksum + 5 deletes = ground truth, Bronze ops exactly `c` 43 / `u` 77 / `d` 8 |
| Scenario 2 `f1862db408369e74a13b` (seed 43) end to end | apply 4 s → `bronze-cdc` 136 records (30 s) → `silver-cdc` changed (47 s): 3,205 live = 3,205 source orders, 14 deleted; `verify-cdc` twice ok, exact ops, `r` = 0 |
| Rerun of the whole chain | Bronze 0 records, Silver `changed: false`, `verify-silver-cdc` ok, `verify-bronze-cdc` ok (6,560 rows, 0 duplicates, no gaps, 6,540 valid + 20 tombstones), scenario 1 still exact |
| Batch isolation | `cdc-batch-guard` ok; Silver/Gold/DP3/warehouse ledger SHA-256 unchanged |

Run manually in sequence, a commit in PostgreSQL reached Silver in 77 s (dominated by two Spark
application start-ups); scheduling is P8-09.

## P8-08 — Operations

Tooling: `make cdc-slot-lag`, `cdc-pause`, `cdc-resume`, `cdc-resnapshot` (Kafka signal channel,
topic `platform_cdc.signals` created by `init-cdc`).

| Experiment | Observation |
|---|---|
| A: 60 MB of WAL written in another database (`postgres`, a scratch table created and dropped) | right after: retained 72 MB, flush lag 72 MB; 40 s later the heartbeat had moved `confirmed_flush_lsn` (flush lag 424 B) while `restart_lsn` still retained 72 MB; ~3 min later retained WAL was 28 KB |
| B: connector paused, scenario 3 `ea56b18676b7cf0cf52f` applied | topic +0 while paused, slot flush lag grew to 96 KB (changes kept); after resume +136 records within seconds; `bronze-cdc` 136, `silver-cdc` changed, `verify-cdc` ok with exact ops `c` 43 / `u` 77 / `d` 8, `r` 0 |
| C: `docker compose restart kafka` | connector back to RUNNING without intervention; `smoke-cdc` 12/12; topic +5 (the probe only) |
| D: `make cdc-resnapshot` (blocking) | topic +3,240 = the 3,240 live orders, slot not dropped; `bronze-cdc` 3,245 (+5 probe), Silver changed (newer LSNs, same business values), `verify-silver-cdc` ok, `verify-cdc` scenario 3 ok (`r` 35 = its live orders), whole table equal |
| Batch isolation | `cdc-batch-guard` ok after every step |

Not exercised on purpose: dropping the slot (destructive; procedure in the runbook) and a schema
change of `walmart.orders` (it would change the batch JDBC evidence of the three datasets; procedure
and impact in the runbook and ADR-0017).

## P8-09 — Orchestration and latency

- DAG `dp1_cdc` (`airflow/dags/dp1_cdc.py`): every 10 minutes, paused at creation, `cdc_bronze` →
  `cdc_silver` → `cdc_silver_verify`, all through the `spark_driver` pool and the driver lock;
  `make test-dags` 12 passed (new test for schedule, pause, pool, lock, rendered command). CDC events
  are allowlisted for task XCom/CI output (unit-tested). The Airflow image carries the Kafka jars.
- Live: unpaused at 16:16 UTC; the first scheduled run succeeded in 2 min 4 s. Scenario 4
  `0ec486a6c63bcb84b954` was committed at 16:18:45; the next scheduled run (16:26:29–16:28:14)
  brought it into Silver; `verify-cdc` exact ops and ok; `cdc-batch-guard` ok; DAG paused again.

| Path | Commit → Silver |
|---|---|
| Manual `make bronze-cdc silver-cdc` right after the commit (P8-07) | 77 s |
| DAG every 10 min (measured) | 9 min 29 s ≈ waiting for the next run + ~1 min 45 s of run time |
| DAG every 10 min (expected) | average ≈ 6–7 min, worst ≈ 12 min |

Trade-off: each run holds the Spark driver lock for ~2 minutes; a 5-minute schedule would halve the
wait but block batch jobs ~40% of the time. The interval is a DAG parameter to tune per environment;
CDC changes are durable in Kafka (7 days) and Bronze in the meantime.

## P8-10 — CI, documentation and handoff

- Manual workflow `.github/workflows/cdc.yml`: env, image builds, `up-cdc`, `cdc-batch-guard-record`
  (a fresh runner has its own batch rows; the guard baseline is now configurable), `smoke-cdc`,
  `up-batch`, `gen-cdc`, `bronze-cdc`, `silver-cdc`, `verify-cdc`, reruns, `verify-bronze-cdc`,
  `verify-silver-cdc`, `cdc-slot-lag`, `restart-cdc` + smoke + verify. CDC events are allowlisted.
- Reproduced in an isolated Compose project (own `.env` and volumes) before any hosted run. It found
  a race: `smoke-cdc` right after `up-cdc` saw the new connector UNASSIGNED. `connect.sh wait` now
  polls until connector and task are RUNNING (used by `up-cdc` and the smoke). After the fix — with a
  delete + fresh registration to recreate the race — every step passed: smoke 12/12, scenario
  `4f8191192d5abf939dbd` → Bronze 146 → Silver 35 live / 7 deleted, `verify-cdc` exact, reruns
  unchanged, Bronze/Silver verification, slot lag, restart + smoke + verify, guard ok.
- README ("Chạy CDC"), runbook, data model, ADR-0001/0017, implementation status, CLAUDE.md and the
  [Phase 9 handoff](phase9-handoff.md) are updated. The hosted run is not claimed here.

### Hosted CI fix

The first hosted `Debezium CDC` run
([44570987827](https://github.com/tri21723/omnichannel-lakehouse/actions/runs/44570987827), commit
`ef9443e`) failed after 47 s in `make build-generator build-batch`: `curl` exit 22 while downloading
the Spark jars — an HTTP error from Maven Central that `--retry` does not retry (a 429 would have
been retried for minutes), i.e. the runner IP was refused. `Dockerfile.spark` now tries Maven
Central and then Google's official Maven Central mirror, and pins every jar by SHA-256 (values taken
from the files that had passed Maven's published SHA-512/SHA-1; identical to the jars in the verified
local image). `Dockerfile.flink` retries Maven with `flink/jobs/maven-mirror.xml`. Both builds were
re-run with `--no-cache` and `--add-host repo.maven.apache.org:127.0.0.1`: every Spark jar came from
the mirror and passed its SHA-256 check; the Flink build and its JUnit tests passed.
