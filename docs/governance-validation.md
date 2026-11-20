# Governance validation (Phase 9)

Evidence for DataHub metadata, lineage and assertions. Commands run sequentially; raw logs stay
local.

## P9-01 — Preflight inventory (2026-11-20)

- Start point: `v0.8-cdc` + later commits (hosted-CI fix and evidence, Phase 9 plan), pushed, clean;
  `make check` green. Only `core` + Lakekeeper + Kafka were started for the inventory.
- Baseline that Phase 9 must not change: ledger SHA-256 Silver `51d0f9d6…`, Gold `be2e5d29…`, DP3
  `b2d6892e…`, warehouse `cfa1cdf5…` (unchanged since Phase 7); `make cdc-batch-guard` ok.

### Objects to catalogue

| Platform | Objects | PII |
|---|---|---|
| PostgreSQL `sources.walmart` | `customers` (229), `orders` (3,275 incl. 140 live CDC-scenario orders), `order_items` (3,135), `products` (52), `returns` (312) | **`customers.full_name`, `customers.email`** |
| PostgreSQL `sources.warehouse` | `dim_channel`, `dim_customer`, `dim_date`, `dim_product`, `dim_seller`, `fact_orders`, `fact_order_items`, `fact_returns`, `export_state` | hashes only |
| Other databases | `airflow`, `lakekeeper`, `mlflow`, `postgres` — service metadata, **not** catalogued | — |
| Iceberg `bronze` (16) | `<channel>_orders_raw` ×5 + `walmart_orders_export_raw`, `walmart_{customers,order_items,products,returns}_raw`, `amazon_order_notifications_raw`, `tiktok_shop_order_webhooks_raw`, `walmart_orders_cdc`; 3 `_smoke_*` fixtures (exclude) | **JSONL/stream payloads and `walmart_customers_raw`**; not `walmart_orders_raw` nor CDC (orders have no name/e-mail) |
| Iceberg `silver` (17) | `customers`, `orders`, `order_items`, `products`, `returns`, 5 `quarantine_*`, 5 `_contrib_*`, `walmart_orders_cdc_current`, `quarantine_walmart_orders_cdc` | hashes only (`customers.name_hash`, `email_hash`); quarantine keeps no raw payload (corrected in P9-08) |
| Iceberg `gold` (11) | `dim_*` ×5, `fact_*` ×3, `feat_customer_daily`, `feat_product_daily`, `lbl_order_return` | hashes only |
| Iceberg `benchmark` (103) | benchmark inputs/results — not a contract (exclude or low tier) | derived |
| Kafka | `amazon.order-notifications`, `tiktok_shop.order-webhooks` (+ `.dlq`), `platform_cdc.walmart.orders`; internal: `__consumer_offsets`, `__transaction_state`, `_connect.*`, `__debezium-heartbeat.platform_cdc`, `platform_cdc.signals`, `_smoke.stream` (exclude) | stream payloads and DLQs: **PII** |
| MinIO `raw` | `<channel>/` (batch JSONL), `stream/<channel>/` (Flink output), `manifests/`, `_smoke/` | **raw PII** |
| Airflow | DAGs `dp1_bronze`, `dp1_stream_bronze`, `dp1_cdc`, `dp2_silver`, `dp2_gold`, `dp3_features`, `dp3_return_labels`, `dp4_warehouse`, `batch_daily`, `query_checks`, `maintenance_iceberg` | — |

### Credentials for read-only ingestion

| Need | Existing option | Finding |
|---|---|---|
| PostgreSQL schemas | `trino_ro` (USAGE + SELECT on `walmart`, `warehouse`); `spark_reader` (walmart only) | both can **read `walmart.customers` rows**; metadata ingestion only needs catalog access → propose a new `datahub_ro` with schema `USAGE` and no table `SELECT` (column metadata comes from `pg_catalog`), so it structurally cannot read PII |
| Iceberg metadata | Lakekeeper REST (unauthenticated locally) + MinIO `trino_local` (read-only `lakehouse`) | sufficient: metadata files live in `lakehouse`; no access to `raw` |
| Kafka | broker without auth (loopback) | topic metadata only; no consumer reads of payloads |

### Memory budget

`core` + Lakekeeper + Kafka: Kafka 487 MiB, MinIO 341 MiB, PostgreSQL 75 MiB, Lakekeeper 10 MiB; host
used 4.3 GiB, available 7.7 GiB. A DataHub of ~4–5 GiB (GMS, frontend, search engine) fits only with
Spark, Flink, Trino and Airflow stopped; Airflow (~1.2 GiB) can join for the plugin step.

### Placeholders

`make up-gov` still `exit 2`; `datahub/recipes/` is empty.

## P9-02 — Decisions

[ADR-0018](adr/0018-governance-datahub.md): DataHub v1.7.0.1 + CLI 1.7.0.19, OpenSearch 2.19.3;
minimal services reusing PostgreSQL (database/role `datahub`) and Kafka (topics pre-created by the
system-update job); frontend `127.0.0.1:9002`, GMS without host port; generated secrets; ingestion in a
`datahub-ingest` image and, for the schedule, a separate virtualenv in the Airflow image (owner
decision); lineage emitted from ledgers and DAG definitions; `datahub_ro` with schema `USAGE` only.

Evidence gathered for the decisions:
- `pip install --dry-run acryl-datahub-airflow-plugin==1.7.0.19 -c constraints-3.3.2/constraints-3.11.txt`
  in `omnichannel/airflow:3.3.2-spark3.5.5` → `ResolutionImpossible` (the plugin is not usable).
- The v1.7.0.1 quickstart pre-creates DataHub topics with the system-update job, supports PostgreSQL as
  the metadata store and uses OpenSearch 2.19.3; its defaults contain fixed secrets, which are replaced.

## P9-03 — Governance profile

- `docker/compose.governance.yml` (profile `governance`, merged with core + stream `kafka`):
  `gov-postgres-init`, `datahub-opensearch`, `datahub-system-update`, `datahub-gms`,
  `datahub-frontend`, `datahub-ingest`; each with `mem_limit`, long-running services with healthchecks,
  OpenSearch on its own volume `datahub-opensearch`.
- Image digests (pulled 2026-11-20): `datahub-gms@sha256:74e15e98…0173`,
  `datahub-frontend-react@sha256:99513cc1…310d`, `datahub-upgrade@sha256:c3db54d8…a24c`,
  `opensearch@sha256:e96cc6ae…0cdd`.
- Bootstrap: `make init-gov` (idempotent) creates database/role `datahub` and `datahub_ro` (schema
  `USAGE` only, `SELECT` on `walmart.customers` = false). DataHub's topics are pre-created by
  `datahub-system-update`; the broker keeps `auto.create.topics.enable=false`.
- Secrets: `make env-gov` fills missing *or empty* keys only (a fresh `.env` copied from `.env.example`
  has empty values), never rotates; the UI login file `.local-data/governance/user.props` is derived
  from `.env` and git-ignored. The same fill-empty rule now applies to `docker/stream/env.py` (tests in
  `docker/governance/tests`).
- `make smoke-gov`, 15 checks: GMS health, frontend health, GMS without host port binding, frontend
  bound to `127.0.0.1:9002`, login with the generated password (200), default `datahub/datahub`
  rejected (400 `Invalid Credentials`), GraphQL as admin, GraphQL `appVersion = v1.7.0.1`, probe
  dataset write/read/delete through GMS, DataHub topics present, broker auto-create off, `datahub_ro`
  without row access, no generated secret in GMS/frontend/system-update logs.
- Runs: green after the first start, after `make restart-gov`, after `make down` + `make up-gov`
  (cold start 166 s, system-update rerun is idempotent) and after a second `make up-gov` on the
  running stack.
- Memory (steady state, core + Kafka + DataHub): GMS 1.61 GiB (limit raised 1.75 → 2 GiB after this
  measurement), OpenSearch 1.17 GiB, frontend 0.57 GiB, Kafka 0.57 GiB, MinIO 0.17 GiB, PostgreSQL
  0.06 GiB; host available 4.1 GiB.
- `datahub-ingest` image (`make build-gov`): `pip install acryl-datahub[datahub-rest,postgres,kafka,
  iceberg]==1.7.0.19` fails with `ResolutionImpossible` (pip backtracks out on `postgres` + `iceberg`),
  although a solution exists; `make gov-lock` compiles `datahub/requirements.lock` with uv 0.9.5
  (exact versions + hashes, reproducible: same SHA-256 twice) and the image installs it with
  `--require-hashes --no-deps`. CLI reports `1.7.0.19`; image 1.15 GB.

## P9-04 — PostgreSQL and Kafka ingestion

- `make gov-ingest SOURCE=postgres|kafka` runs `datahub/recipes/<source>.yml` in `datahub-ingest`
  (secrets from the environment only). Warnings fail the run, except for Kafka: the JSON topics have no
  registry subject, reported as one warning per topic (5).
- `make gov-inventory` (`datahub/tools/inventory.py`, GraphQL against the internal GMS) prints counts
  per platform and a SHA-256 of the URN set, read only after two reads 5 s apart agree (the search
  index is updated asynchronously; an immediate read once showed 2 of 5 topics).

| Run | Result | Inventory |
|---|---|---|
| postgres #1 | 14 tables (`walmart` 5, `warehouse` 9), 75 events, 0 warnings | `postgres: 14`, `a4dc3929…` |
| postgres #2 | same | unchanged `a4dc3929…` |
| kafka #1…#3 | 5 topics: `amazon.order-notifications(.dlq)`, `tiktok_shop.order-webhooks(.dlq)`, `platform_cdc.walmart.orders`; 27 events | `kafka: 5, postgres: 14`, unchanged `947857af…` |

- Read-only evidence: `walmart.customers` has its five columns with native types and **no
  `datasetProfile`**; `walmart.orders.total_amount` is `NUMERIC(18, 2)`; `datahub_ro` cannot `SELECT`
  the table. The Kafka source creates no consumer group (only DataHub's own, Flink's and `cdc-signals`
  exist) and internal topics are filtered out.
- Sources unchanged: `make cdc-batch-guard` ok; no Spark job ran, ledgers untouched.

## P9-05 — Iceberg ingestion

- `make up-gov` now also starts Lakekeeper (+ `init-lakehouse`), like `make up-query`; no Spark.
- `make gov-ingest SOURCE=iceberg`: Lakekeeper REST catalog, MinIO `trino_local` (read-only
  `lakehouse`), namespaces `bronze|silver|gold`; report `tables_scanned: 41`, `entities_profiled: 0`,
  filtered `benchmark.*` and the three `bronze._smoke_*` fixtures; 345 events, 0 warnings.
- Counts match the catalogue of P9-01: bronze 13 (16 − 3 smoke), silver 17, gold 11.
- Three runs → inventory `iceberg: 41, kafka: 5, postgres: 14`, URN hash `af486736…` unchanged.
- Stable URNs for lineage: `urn:li:dataset:(urn:li:dataPlatform:iceberg,<namespace>.<table>,PROD)`
  (no platform instance), PostgreSQL `…:postgres,sources.<schema>.<table>,PROD`, Kafka
  `…:kafka,<topic>,PROD`.
- Column types follow the contract: `silver.orders.total_amount decimal(18, 2)`, `order_ts`/`updated_ts
  timestamptz`; `gold.fact_orders` `gross/discount/net_amount decimal(18, 2)`; CDC Bronze `_kafka_ts`,
  `_ingest_ts timestamptz`.
- Ledgers unchanged (`51d0f9d6`, `be2e5d29`, `b2d6892e`, `cfa1cdf5`).

## P9-06 — Dataset lineage from ledgers

- `datahub/lineage/model.py` (pure, unit-tested in `datahub/tests/test_lineage_model.py`, 6 tests incl.
  parity of its constants with `spark/lib`) builds the edges:
  - Bronze from completion records (`/runtime/bronze/*.json`, completed only): `jsonl` → `s3 raw/<channel>`,
    `jdbc` → `postgres sources.walmart.<entity>`, `stream` → `s3 raw/stream/<channel>`;
  - Silver from the ledger slices (`input_tables`, or the `silver_input` rule for legacy slices) and
    per-entity `accounting.input > 0`: Bronze → `_contrib_<entity>` + `quarantine_<entity>`,
    `_contrib_<entity>` → `<entity>`;
  - Gold/DP3 from the code contract per table (Silver entities / Gold tables read), checked against
    the ledgers' table sets; `gold.dim_channel` is static (channel registry), no upstream;
  - warehouse from the export ledger runs (`lh.gold.<t>` → `sources.warehouse.<t>`);
  - stream: topic → `s3 raw/stream/<channel>` and topic → `.dlq`; CDC: `sources.walmart.orders` →
    `platform_cdc.walmart.orders` → `bronze.walmart_orders_cdc` → `silver.walmart_orders_cdc_current`
    + `silver.quarantine_walmart_orders_cdc`.
- `make gov-lineage` (ledgers mounted read-only) upserts one `upstreamLineage` per downstream with
  sorted upstreams and a fixed audit time; the seven S3 prefixes are declared as datasets (subtype
  `Prefix`). `DRY_RUN=1` prints the edges.
- Result: 54 downstreams, 108 edges, lineage hash `35224b8f…`, identical on two emissions; the
  `upstreamLineage` aspect of `gold.fact_orders` stays at version 1 after the second run. Inventory:
  `iceberg 41, kafka 5, postgres 14, s3 7`.
- Downstream lineage (GraphQL `scrollAcrossLineage`): `sources.walmart.orders` reaches
  `sources.warehouse.fact_orders` (5 hops), `gold.lbl_order_return` (5) and
  `silver.walmart_orders_cdc_current` (3); `s3 raw/amazon` reaches `sources.warehouse.fact_returns` (5);
  `tiktok_shop.order-webhooks` reaches `bronze.tiktok_shop_order_webhooks_raw` (2) and its DLQ (1).
- `bronze.walmart_orders_export_raw` (Walmart JSONL export) has no downstream: Silver reads Walmart
  through JDBC (ADR-0008), which the lineage shows as it is.

## P9-07 — Airflow job lineage and scheduled ingestion

- Decision (ADR-0018 §3): no DataHub Airflow plugin (incompatible with the constraints). The Airflow
  image gets the CLI in `/opt/datahub-venv` from `datahub/requirements.lock` (`pip check` in the Airflow
  environment: no broken requirements; Airflow 3.3.2 unchanged); `datahub/` is bind-mounted read-only
  like the DAGs, so only a lock change needs `make build-orch`.
- `airflow/plugins/omnichannel/datahub_export.py` turns the DagBag into JSON (dag id, owner, tags,
  timetable, tasks, dependencies, Spark script + `--channel/--source/--mode/--verify`);
  `datahub/lineage/jobs.py` maps each Spark task to its datasets from the same edges as P9-06 (verify
  tasks only read); `datahub/lineage/emit_jobs.py` upserts DataFlow/DataJob info, inputs/outputs,
  upstream jobs and ownership. `make gov-jobs` runs it from Make.
- Result: 12 DataFlows, 39 DataJobs, 24 with dataset inputs/outputs (all Bronze/Silver/Gold/DP3/
  warehouse/stream/CDC tasks); hash `cde1c644…` on two emissions; `dataJobInputOutput` of
  `dp4_warehouse.warehouse_export` stays at version 1 (8 inputs, 8 outputs).
- DAG `datahub_ingest` (`@daily`, paused at creation, not in `spark_driver`): `ingest_postgres` →
  `ingest_kafka` → `ingest_iceberg` → `emit_dataset_lineage` → `emit_job_lineage`.
  `make up-gov-orch` starts Airflow next to DataHub without Spark. Real runs: unpausing created the
  scheduled run `scheduled__2026-11-20` and the manual `p9_manual_1`; both **success** (the first
  attempt of `emit_job_lineage` failed: Airflow 3 task processes have no metadata-DB URL, so the
  DagBag export now runs with `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite://` — it only parses
  files). The task logs carry the same hashes as Make (`35224b8f…`, `cde1c644…`); the inventory hash
  `6a50a9db…` did not change; the DAG was paused again afterwards.
- `make test-dags`: 14 passed (new: DAG contract for `datahub_ingest`, export → dataset mapping for
  every Spark task). Memory with Airflow: Airflow 1.5 GiB, host available 3.1 GiB.

## P9-08 — Metadata as code

- `datahub/metadata/metadata.yml`: owner `data-platform`; tags `PII`, `raw`, `quarantine` and tier tags
  `bronze`/`silver`/`gold`; domains Sales, Returns, Features, Streaming, CDC; glossary node
  `Omnichannel` with 11 terms; ordered regex rules for tags/terms/domain/description; column
  descriptions (PII columns, hashes, `_raw`, `net_amount`, label columns). `datahub/metadata/apply.py`
  plans (pure) and upserts; `make gov-metadata` (`DRY_RUN=1`, `VERIFY=1`).
- Unit tests `datahub/tests/test_metadata.py` over the 67 catalogued datasets (fixture from
  `gov-inventory`): the `PII` tag is **exactly** the 20 raw-PII objects; every Gold/warehouse table has
  domain, description and tier `gold`; every dataset has a domain and description; Iceberg tables
  carry their tier; plans are deterministic; undefined tags/terms/domains are rejected.
- Correction found while writing the rules: P9-01 marked Silver quarantine as PII; it keeps no raw
  payload (data-model §Silver), and Bronze `walmart_orders_raw` and the CDC chain carry no
  name/e-mail. The P9-01 table and ADR-0018 §5 are corrected.
- Applied: 67 datasets, 20 `PII`, 0 without domain, plan hash `ea76250e…`; `VERIFY=1` → no drift.
- Two defects found and fixed by the rerun check:
  - the Iceberg recipe re-emitted `ownership` = `root` (Spark's table property) and erased the owner
    → `user_ownership_property: null`;
  - `ownership`/`domains` got a new version on every apply because GMS stores an enriched form
    (`ownerTypes`, `lastModified`, `domainAssociations`) → `datahub/tools/aspects.py` sends that form
    (also for DataFlow/DataJob owners).
- After the fixes, three rounds of `gov-metadata` + `gov-jobs` leave every aspect version unchanged
  (`gold.fact_orders`: globalTags 1, glossaryTerms 1, editableDatasetProperties 1, upstreamLineage 1,
  ownership 12, domains 6; `warehouse_export` job ownership 4), and a full re-ingestion
  (postgres, kafka, iceberg) + lineage followed by `VERIFY=1` reports no drift.

## P9-09 — G1 assertions

- `datahub/assertions/lineage_g1.py` (`make gov-assert`, then `metadata.apply --verify`): reads every
  catalogued dataset and its live `upstreamLineage` from GMS (404 = no aspect) and evaluates (pure
  `evaluate()`, 4 unit tests incl. edge removal, broken CDC/warehouse edges, cycles):

| Check | Subjects | Result |
|---|---|---|
| `gold_has_silver_upstream` (all Gold except static `dim_channel`) | 10 | ok |
| `silver_has_bronze_upstream` | 17 | ok |
| `bronze_has_source_upstream` (MinIO / PostgreSQL / Kafka) | 13 | ok |
| `warehouse_has_gold_upstream` (same table name) | 8 | ok |
| `stream_bronze_reaches_topic` | 2 | ok |
| `cdc_silver_reaches_source` (CDC topic and `walmart.orders`) | 2 | ok |
| `ledger_edges_present` (edges built from the ledgers) | 108 | ok |
| `ledger_downstreams_catalogued` | 54 | ok |

- Two consecutive runs: `g1_finished ok: true` (8 checks, 67 datasets) and metadata drift none.
- Live negative test (`make gov-assert-negative`, probe `iceberg gold.g1_probe`, real entities never
  touched): with edge → pass; edge removed → fail with exactly `gold_has_silver_upstream:
  gold.g1_probe`; restored → pass; probe hard-deleted → inventory hash back to `6a50a9db…`.
- Idempotency of the whole flow (P9-04…P9-08): every recipe, lineage, jobs and metadata run twice or
  more with unchanged URN hashes and aspect versions. Sources unchanged: ledgers `51d0f9d6`,
  `be2e5d29`, `b2d6892e`, `cfa1cdf5`; `make cdc-batch-guard` ok.

### UI evidence (P9-09)

Screenshots taken headless (Playwright 1.49.1 container on the compose network, generated login read
from the git-ignored file) after `make restart-gov`:

| Image | Shows |
|---|---|
| [warehouse `fact_orders` lineage](images/phase9/lineage-warehouse-fact-orders.png) | Gold tables → `dp4_warehouse.warehouse_export` → `sources.warehouse.fact_orders`; owner, domain Sales, tag `gold`, terms |
| [stream Bronze lineage](images/phase9/lineage-stream-tiktok.png) | `raw/stream/tiktok_shop` → `dp1_stream_bronze` job → `bronze.tiktok_shop_order_webhooks_raw`; tags `PII`, `bronze`, `raw`; domain Streaming |
| [CDC Silver lineage](images/phase9/lineage-cdc-silver.png) | `bronze.walmart_orders_cdc` → `dp1_cdc.cdc_silver` → `silver.walmart_orders_cdc_current`; domain CDC, terms Change event/Order |
| [Gold `fact_orders`](images/phase9/dataset-gold-fact-orders.png) | schema (24 columns), description, owner, domain, tag, glossary terms |

Two defects found through the UI and fixed:
- Airflow task order was emitted as `inputDatajobs`, which DataHub drew as lineage (the Amazon stream
  prefix appeared upstream of TikTok Bronze). Task order is now the `upstream_tasks` property; the jobs
  hash is unchanged (the records are the same, only the emitted aspect changed).
- The UI kept the removed edge after the fix while GraphQL `lineage`, `scrollAcrossLineage` and
  `searchAcrossLineage` were already correct: GMS caches lineage results; `make restart-gov` cleared it
  (runbook).

Stream and CDC chains now follow evidence (completed stream Bronze slice per channel, `dp1_cdc`
checkpoint) so a fresh environment does not claim them; locally the lineage hash stays `35224b8f…`
and G1 requires both chains (`--optional-chains` for CI only, unit-tested).
