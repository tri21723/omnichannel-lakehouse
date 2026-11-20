import pytest
from lineage import model

from spark.lib import bronze_input, cdc, gold_entities, silver_input, warehouse_input
from spark.lib.dp3_input import GOLD_TABLES

ice = model.iceberg


def test_constants_match_the_spark_contracts():
    assert model.ENTITIES == silver_input.ENTITIES
    assert model.CHANNELS == bronze_input.CHANNELS
    assert model.STREAM_TABLES == bronze_input.STREAM_TABLES
    assert set(model.GOLD_UPSTREAMS) == set(GOLD_TABLES) == set(warehouse_input.EXPORT_TABLES)
    assert set(model.GOLD_UPSTREAMS) == set(gold_entities.GOLD_KEYS)
    assert model.STATIC_GOLD == {"dim_channel"}
    assert (model.CDC_TOPIC, model.CDC_BRONZE) == (cdc.TOPIC, cdc.BRONZE_TABLE)
    assert (model.CDC_SILVER, model.CDC_QUARANTINE) == (cdc.SILVER_TABLE, cdc.QUARANTINE_TABLE)


def test_urns_match_the_ingestion_recipes():
    assert (
        ice("lh.silver.orders") == "urn:li:dataset:(urn:li:dataPlatform:iceberg,silver.orders,PROD)"
    )
    assert model.postgres("sources.warehouse.fact_orders").endswith(
        ":postgres,sources.warehouse.fact_orders,PROD)"
    )
    with pytest.raises(ValueError):
        ice("silver.orders")


def record(source, channel, tables, status="completed"):
    return {
        "status": status,
        "selection": {"source": source, "channel": channel},
        "tables": {t: {} for t in tables},
    }


def test_bronze_edges_follow_completion_records():
    edges = model.bronze_edges(
        [
            record("jsonl", "amazon", ["lh.bronze.amazon_orders_raw"]),
            record(
                "jdbc", "walmart", ["lh.bronze.walmart_orders_raw", "lh.bronze.walmart_returns_raw"]
            ),
            record("stream", "tiktok_shop", ["lh.bronze.tiktok_shop_order_webhooks_raw"]),
            record("jsonl", "lazada", ["lh.bronze.lazada_orders_raw"], status="failed"),
            {"status": "completed", "batch_id": "legacy"},
        ]
    )
    assert edges == {
        ice("lh.bronze.amazon_orders_raw"): {model.s3("raw/amazon")},
        ice("lh.bronze.walmart_orders_raw"): {model.postgres("sources.walmart.orders")},
        ice("lh.bronze.walmart_returns_raw"): {model.postgres("sources.walmart.returns")},
        ice("lh.bronze.tiktok_shop_order_webhooks_raw"): {model.s3("raw/stream/tiktok_shop")},
    }
    with pytest.raises(ValueError):
        model.bronze_edges([record("jdbc", "walmart", ["lh.bronze.walmart_unknown_raw"])])


def silver_ledger():
    accounting = {e: {"input": 3} for e in model.ENTITIES}
    return {
        "slices": {
            "a": {"selection": {"source": "jsonl", "channel": "amazon"}},  # legacy: no input_tables
            "b": {
                "selection": {"source": "jdbc", "channel": "walmart"},
                "input_tables": {f"lh.bronze.walmart_{e}_raw": {} for e in model.ENTITIES},
            },
        },
        "accounting": {"a": accounting, "b": {**accounting, "returns": {"input": 0}}},
    }


def test_silver_edges_use_slices_and_accounting():
    edges = model.silver_edges(silver_ledger())
    assert edges[ice("lh.silver._contrib_orders")] == {
        ice("lh.bronze.amazon_orders_raw"),
        ice("lh.bronze.walmart_orders_raw"),
    }
    assert edges[ice("lh.silver.quarantine_customers")] == {
        ice("lh.bronze.amazon_orders_raw"),
        ice("lh.bronze.walmart_customers_raw"),
    }
    # The JDBC slice had no returns input, so only the JSONL channel feeds returns.
    assert edges[ice("lh.silver._contrib_returns")] == {ice("lh.bronze.amazon_orders_raw")}
    assert edges[ice("lh.silver.orders")] == {ice("lh.silver._contrib_orders")}


def test_gold_dp3_and_warehouse_edges():
    gold = model.gold_edges({"tables": {f"lh.gold.{t}": {} for t in model.GOLD_UPSTREAMS}})
    assert gold[ice("lh.gold.fact_returns")] == {
        ice("lh.silver.returns"),
        ice("lh.silver.order_items"),
        ice("lh.silver.orders"),
    }
    assert gold[ice("lh.gold.dim_channel")] == set()
    dp3 = model.dp3_edges({"tables": {f"lh.gold.{t}": {} for t in model.DP3_UPSTREAMS}})
    assert ice("lh.gold.dim_customer") in dp3[ice("lh.gold.feat_customer_daily")]
    warehouse = model.warehouse_edges({"runs": {"r": {"tables": {"warehouse.fact_orders": 1}}}})
    assert warehouse == {
        model.postgres("sources.warehouse.fact_orders"): {ice("lh.gold.fact_orders")}
    }
    with pytest.raises(ValueError):
        model.gold_edges({"tables": {"lh.gold.dim_unknown": {}}})


def test_stream_and_cdc_chains_end_in_bronze_and_silver():
    edges = model.merge(
        model.stream_edges(),
        model.cdc_edges(),
        model.bronze_edges(
            [record("stream", "amazon", ["lh.bronze.amazon_order_notifications_raw"])]
        ),
    )
    topic = model.kafka("amazon.order-notifications")
    assert edges[model.s3("raw/stream/amazon")] == {topic}
    assert edges[model.kafka("amazon.order-notifications.dlq")] == {topic}
    assert edges[ice("lh.bronze.amazon_order_notifications_raw")] == {model.s3("raw/stream/amazon")}
    assert edges[model.kafka(cdc.TOPIC)] == {model.postgres("sources.walmart.orders")}
    assert edges[ice(cdc.SILVER_TABLE)] == {ice(cdc.BRONZE_TABLE)}
    assert model.external_datasets(edges) == {
        model.s3("raw/stream/amazon"),
        model.s3("raw/stream/tiktok_shop"),
    }
