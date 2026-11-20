from pathlib import Path

import pytest
from metadata.apply import key, load, plan, term_urn

KEYS = [
    line
    for line in (Path(__file__).parent / "fixtures/datasets.txt").read_text().splitlines()
    if line and not line.startswith("#")
]
# Objects holding raw synthetic names/e-mails (docs/data-model.md, generators, ADR-0016).
PII = {
    "postgres:sources.walmart.customers",
    "iceberg:bronze.walmart_customers_raw",
    "iceberg:bronze.walmart_orders_export_raw",
    *(f"iceberg:bronze.{c}_orders_raw" for c in ("amazon", "tiktok_shop", "shopee", "lazada")),
    *(f"s3:raw/{c}" for c in ("walmart", "amazon", "tiktok_shop", "shopee", "lazada")),
    "iceberg:bronze.amazon_order_notifications_raw",
    "iceberg:bronze.tiktok_shop_order_webhooks_raw",
    "s3:raw/stream/amazon",
    "s3:raw/stream/tiktok_shop",
    "kafka:amazon.order-notifications",
    "kafka:amazon.order-notifications.dlq",
    "kafka:tiktok_shop.order-webhooks",
    "kafka:tiktok_shop.order-webhooks.dlq",
}


@pytest.fixture(scope="module")
def planned():
    return plan(load(), KEYS)


def test_fixture_covers_the_catalogue():
    assert len(KEYS) == 67 and len(set(KEYS)) == 67
    assert key("urn:li:dataset:(urn:li:dataPlatform:iceberg,gold.fact_orders,PROD)") == (
        "iceberg:gold.fact_orders"
    )


def test_pii_tag_is_exactly_the_raw_pii_objects(planned):
    assert {k for k, v in planned.items() if "PII" in v["tags"]} == PII
    assert "PII" not in planned["iceberg:bronze.walmart_orders_raw"]["tags"]  # JDBC orders
    assert "PII" not in planned["iceberg:silver.quarantine_walmart_orders_cdc"]["tags"]


def test_every_gold_table_has_domain_description_and_tier(planned):
    gold = [k for k in KEYS if k.startswith("iceberg:gold.")]
    warehouse = [
        k for k in KEYS if k.startswith("postgres:sources.warehouse.") and "_state" not in k
    ]
    assert len(gold) == 11 and len(warehouse) == 8
    for name in gold + warehouse:
        item = planned[name]
        assert item["domain"] and item["description"] and "gold" in item["tags"], name


def test_every_dataset_has_a_domain_description_and_known_tier(planned):
    for name, item in planned.items():
        assert item["domain"] and item["description"], name
        if name.startswith("iceberg:"):
            tier = name.split(":")[1].split(".")[0]
            assert tier in item["tags"], name
    assert planned["iceberg:silver.quarantine_orders"]["tags"] == ["quarantine", "silver"]
    assert planned["kafka:platform_cdc.walmart.orders"]["domain"] == "cdc"
    assert planned["iceberg:bronze.amazon_order_notifications_raw"]["domain"] == "streaming"


def test_plan_is_deterministic_and_rejects_unknown_references():
    metadata = load()
    assert plan(metadata, KEYS) == plan(metadata, list(reversed(KEYS)))
    assert term_urn("Net amount") == "urn:li:glossaryTerm:omnichannel.net_amount"
    broken = {**metadata, "rules": [{"match": ".*", "tags": ["missing"]}]}
    with pytest.raises(ValueError):
        plan(broken, KEYS)
