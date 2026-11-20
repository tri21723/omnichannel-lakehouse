"""Dataset-level lineage built from the pipelines' ledgers and contracts (ADR-0018 §4).

Pure functions only (no DataHub SDK, no Spark), so the edges can be unit-tested. Edges observed in
run evidence come from the ledgers (Bronze completion records, Silver slices/accounting, warehouse
export runs); per-table dependencies inside a job come from its code contract (constants below, kept
in sync with `spark/lib` by `datahub/tests/test_lineage_model.py`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

ENV = "PROD"
ENTITIES = ("orders", "order_items", "customers", "products", "returns")
CHANNELS = ("walmart", "amazon", "tiktok_shop", "shopee", "lazada")
STREAM_TABLES = {
    "amazon": "amazon_order_notifications_raw",
    "tiktok_shop": "tiktok_shop_order_webhooks_raw",
}
STREAM_TOPICS = {
    "amazon": "amazon.order-notifications",
    "tiktok_shop": "tiktok_shop.order-webhooks",
}
CDC_SOURCE = "sources.walmart.orders"
CDC_TOPIC = "platform_cdc.walmart.orders"
CDC_BRONZE = "lh.bronze.walmart_orders_cdc"
CDC_SILVER = "lh.silver.walmart_orders_cdc_current"
CDC_QUARANTINE = "lh.silver.quarantine_walmart_orders_cdc"

# Gold tables and the Silver entities each one reads (spark/lib/gold_pipeline.py).
# dim_channel is built from the static channel registry (gold_entities.CHANNELS), not from Silver.
GOLD_UPSTREAMS: dict[str, tuple[str, ...]] = {
    "dim_channel": (),
    "dim_date": ("orders", "customers", "returns"),
    "dim_customer": ("customers",),
    "dim_seller": ("products", "order_items"),
    "dim_product": ("products",),
    "fact_order_items": ("order_items", "orders"),
    "fact_orders": ("orders", "order_items"),
    "fact_returns": ("returns", "order_items", "orders"),
}
STATIC_GOLD = frozenset(name for name, upstream in GOLD_UPSTREAMS.items() if not upstream)
# DP3 tables and the Gold tables each one reads (spark/lib/dp3_features.py, dp3_labels.py).
DP3_UPSTREAMS: dict[str, tuple[str, ...]] = {
    "feat_customer_daily": ("fact_orders", "fact_returns", "dim_customer", "dim_channel"),
    "feat_product_daily": (
        "fact_orders",
        "fact_order_items",
        "fact_returns",
        "dim_product",
        "dim_channel",
    ),
    "lbl_order_return": ("fact_orders", "fact_returns"),
}

Lineage = dict[str, set[str]]


def dataset_urn(platform: str, name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{ENV})"


def iceberg(table: str) -> str:
    """`lh.<namespace>.<table>` (Spark catalog name) → the URN the Iceberg recipe ingests."""
    catalog, _, name = table.partition(".")
    if catalog != "lh" or name.count(".") != 1:
        raise ValueError(f"Unexpected Iceberg table name: {table}")
    return dataset_urn("iceberg", name)


def postgres(name: str) -> str:
    return dataset_urn("postgres", name)


def kafka(topic: str) -> str:
    return dataset_urn("kafka", topic)


def s3(prefix: str) -> str:
    return dataset_urn("s3", prefix)


def _add(edges: Lineage, downstream: str, upstreams: Iterable[str]) -> None:
    edges.setdefault(downstream, set()).update(upstreams)


def bronze_source(selection: Mapping[str, Any], table: str) -> str:
    """Upstream of one Bronze table written by a completed DP1 batch."""
    source, channel = selection.get("source"), selection.get("channel")
    if channel not in CHANNELS:
        raise ValueError(f"Unknown channel in Bronze selection: {channel}")
    if source == "jsonl":
        return s3(f"raw/{channel}")
    if source == "stream":
        return s3(f"raw/stream/{channel}")
    if source == "jdbc":
        prefix, suffix = "lh.bronze.walmart_", "_raw"
        entity = table.removeprefix(prefix).removesuffix(suffix)
        if channel != "walmart" or entity not in ENTITIES:
            raise ValueError(f"Unexpected JDBC Bronze table: {table}")
        return postgres(f"sources.walmart.{entity}")
    raise ValueError(f"Unknown Bronze source: {source}")


def bronze_edges(records: Iterable[Mapping[str, Any]]) -> Lineage:
    edges: Lineage = {}
    for record in records:
        if record.get("status") != "completed" or "selection" not in record:
            continue  # failed or legacy attempts carry no published table
        for table in record.get("tables", {}):
            _add(edges, iceberg(table), [bronze_source(record["selection"], table)])
    return edges


def silver_inputs(selection: Mapping[str, Any]) -> set[str]:
    """Bronze tables a Silver slice reads (mirrors spark/lib/silver_input.py)."""
    if selection.get("source") == "jdbc":
        return {f"lh.bronze.walmart_{name}_raw" for name in ENTITIES}
    return {f"lh.bronze.{selection['channel']}_orders_raw"}


def silver_edges(ledger: Mapping[str, Any]) -> Lineage:
    """Bronze → `_contrib_<entity>` / `quarantine_<entity>`; `_contrib_<entity>` → `<entity>`."""
    edges: Lineage = {}
    for slice_id, item in ledger.get("slices", {}).items():
        inputs = set(item.get("input_tables") or silver_inputs(item["selection"]))
        accounting = ledger.get("accounting", {}).get(slice_id, {})
        for entity in ENTITIES:
            if accounting.get(entity, {}).get("input", 0) <= 0:
                continue
            if item["selection"].get("source") == "jdbc":
                sources = {f"lh.bronze.walmart_{entity}_raw"} & inputs
            else:
                sources = inputs
            upstream = [iceberg(table) for table in sorted(sources)]
            for prefix in ("_contrib_", "quarantine_"):
                _add(edges, iceberg(f"lh.silver.{prefix}{entity}"), upstream)
    for entity in ENTITIES:
        contrib = iceberg(f"lh.silver._contrib_{entity}")
        if contrib in edges:
            _add(edges, iceberg(f"lh.silver.{entity}"), [contrib])
    return edges


def gold_edges(ledger: Mapping[str, Any]) -> Lineage:
    edges: Lineage = {}
    for table in ledger.get("tables", {}):
        name = table.removeprefix("lh.gold.")
        if name not in GOLD_UPSTREAMS:
            raise ValueError(f"Gold table without a lineage contract: {table}")
        _add(edges, iceberg(table), [iceberg(f"lh.silver.{e}") for e in GOLD_UPSTREAMS[name]])
    return edges


def dp3_edges(ledger: Mapping[str, Any]) -> Lineage:
    edges: Lineage = {}
    for table in ledger.get("tables", {}):
        name = table.removeprefix("lh.gold.")
        if name not in DP3_UPSTREAMS:
            raise ValueError(f"DP3 table without a lineage contract: {table}")
        _add(edges, iceberg(table), [iceberg(f"lh.gold.{t}") for t in DP3_UPSTREAMS[name]])
    return edges


def warehouse_edges(ledger: Mapping[str, Any]) -> Lineage:
    edges: Lineage = {}
    for run in ledger.get("runs", {}).values():
        for table in run.get("tables", {}):
            name = table.removeprefix("warehouse.")
            _add(edges, postgres(f"sources.warehouse.{name}"), [iceberg(f"lh.gold.{name}")])
    return edges


def stream_edges() -> Lineage:
    """Kafka topic → Flink → `raw/stream/<channel>` (+ DLQ topic); Bronze edges come from DP1."""
    edges: Lineage = {}
    for channel, topic in STREAM_TOPICS.items():
        _add(edges, s3(f"raw/stream/{channel}"), [kafka(topic)])
        _add(edges, kafka(f"{topic}.dlq"), [kafka(topic)])
    return edges


def cdc_edges() -> Lineage:
    edges: Lineage = {}
    _add(edges, kafka(CDC_TOPIC), [postgres(CDC_SOURCE)])
    _add(edges, iceberg(CDC_BRONZE), [kafka(CDC_TOPIC)])
    _add(edges, iceberg(CDC_SILVER), [iceberg(CDC_BRONZE)])
    _add(edges, iceberg(CDC_QUARANTINE), [iceberg(CDC_BRONZE)])
    return edges


def merge(*parts: Lineage) -> Lineage:
    edges: Lineage = {}
    for part in parts:
        for downstream, upstreams in part.items():
            _add(edges, downstream, upstreams)
    return edges


def build(
    bronze_records: Iterable[Mapping[str, Any]],
    silver: Mapping[str, Any],
    gold: Mapping[str, Any],
    dp3: Mapping[str, Any],
    warehouse: Mapping[str, Any],
) -> Lineage:
    return merge(
        bronze_edges(bronze_records),
        silver_edges(silver),
        gold_edges(gold),
        dp3_edges(dp3),
        warehouse_edges(warehouse),
        stream_edges(),
        cdc_edges(),
    )


def external_datasets(edges: Lineage) -> set[str]:
    """S3 prefixes have no recipe; the emitter declares them so they are real entities."""
    every = set(edges) | {u for ups in edges.values() for u in ups}
    return {urn for urn in every if ":dataPlatform:s3," in urn}
