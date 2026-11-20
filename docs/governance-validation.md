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
| Iceberg `bronze` (16) | `<channel>_orders_raw` ×5 + `walmart_orders_export_raw`, `walmart_{customers,order_items,products,returns}_raw`, `amazon_order_notifications_raw`, `tiktok_shop_order_webhooks_raw`, `walmart_orders_cdc`; 3 `_smoke_*` fixtures (exclude) | **raw payloads (`_raw`, `before`/`after`) with names/emails** |
| Iceberg `silver` (17) | `customers`, `orders`, `order_items`, `products`, `returns`, 5 `quarantine_*`, 5 `_contrib_*`, `walmart_orders_cdc_current`, `quarantine_walmart_orders_cdc` | hashes only (`customers.name_hash`, `email_hash`); quarantine keeps `_raw` → **PII** |
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
