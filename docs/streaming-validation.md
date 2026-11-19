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
