# Phase 9 handoff: data governance with DataHub

Phase 8 is complete locally (`v0.8-cdc`). This page lists what Phase 9 can rely on and the
constraints it inherits. It does not implement Phase 9 ([plan §Phase 9](plan.md)).

## 1. What exists to catalogue

| Area | Assets | Where the lineage is recorded today |
|---|---|---|
| Sources | MinIO `raw` (JSONL per channel, `stream/` Flink output, `manifests/`), PostgreSQL `sources.walmart` | generator manifests and ground truth (ADR-0005, 0016, 0017) |
| Bronze | `lh.bronze.<channel>_orders_raw`, `walmart_*_raw` (JDBC), `amazon_order_notifications_raw`, `tiktok_shop_order_webhooks_raw` (stream), `walmart_orders_cdc` (CDC) | completion records in `/runtime/bronze` (ADR-0007) |
| Silver | entity tables + quarantine (ADR-0008), `walmart_orders_cdc_current` + CDC quarantine (ADR-0017) | Silver ledger with Bronze pins |
| Gold / DP3 | star schema, features, labels (ADR-0009, 0010) | Gold and DP3 ledgers with Silver/Gold publication pins |
| Warehouse | PostgreSQL `warehouse` (ADR-0011) | export ledger with Gold snapshot pins |
| Streams | Kafka topics `amazon.order-notifications`, `tiktok_shop.order-webhooks`, DLQs, `platform_cdc.walmart.orders` | ADR-0016/0017 |
| Orchestration | Airflow DAGs `dp1_*`…`dp4_*`, `batch_daily`, `dp1_stream_bronze`, `dp1_cdc`, `query_checks`; layer assets `lh.bronze` … `sources.warehouse` | ADR-0013 |
| Query | Trino 484 read-only (`iceberg`, `postgresql` catalogs) | ADR-0015 |

The ledgers already pin exact upstream snapshots, so a lineage assertion (plan G1) can be checked
against them, not only against what DataHub infers.

## 2. Contracts Phase 9 must keep

- **PII:** Bronze `_raw` payloads, stream payloads and `walmart.customers` contain synthetic but
  raw names/emails. Metadata ingestion may read schemas and statistics, never sample rows from those
  objects; tag them `PII`. Trino keeps `iceberg.bronze` hidden — ingest Iceberg through Lakekeeper's
  REST catalog with read-only credentials instead of widening Trino.
- **Read-only:** DataHub recipes must use read-only roles (`trino_ro`, a read-only MinIO user,
  `spark_reader`-like PostgreSQL role); no recipe may write to sources, Iceberg or Kafka (no topic
  auto-creation; DataHub's own topics, if any, are created explicitly).
- **Ports:** DataHub frontend 9002, GMS away from 8080 (AGENTS §5). Kafka Connect has no host port;
  Trino is on 8083.
- **Resources:** DataHub is heavy; run it only with `core` (plus the one source being ingested), never
  together with Spark + Flink + Airflow. Every new service needs `mem_limit`, a healthcheck and the
  `governance` profile; `make up-gov` is still a placeholder (`exit 2`).
- **Batch datasets and CDC scenarios stay untouched**; `make cdc-batch-guard` and the ledger hashes
  in `docs/cdc-validation.md` are the baseline.

## 3. Resources measured so far

core ≈ 0.4–0.7 GiB; Spark workers ≈ 1 GiB each under load; Airflow ≈ 1.2–2.1 GiB; Trino ≈ 1.75 GiB;
Kafka ≈ 0.5 GiB; Kafka Connect ≈ 0.5 GiB; Flink ≈ 1.4 GiB. The host has ~11.7 GiB for Docker.

## 4. Suggested verification

- G1 assertion script: every Gold table has a Silver upstream, every Silver table a Bronze upstream,
  and the warehouse has Gold upstreams — cross-checked with the ledgers' pins.
- Ingestion is idempotent (a second run creates no new entities) and leaves every source unchanged.
