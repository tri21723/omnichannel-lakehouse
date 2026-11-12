"""Nested Walmart export maps to relational keys without floating point loss."""

from decimal import Decimal

import pytest

from generators.batch.dataset import generate, verify_local
from generators.common.config import GeneratorConfig
from generators.load.postgres import TABLES, entity_rows, load


def test_export_entities_preserve_foreign_keys_and_exact_amounts(tmp_path):
    root = generate(
        GeneratorConfig(
            channels=["walmart"], days=1, orders_per_day_per_channel=10, dup_rate=0.5, return_rate=1
        ),
        tmp_path,
    )
    manifest = verify_local(root)
    tables = {
        name: [dict(zip(columns, row, strict=True)) for row in entity_rows(root, manifest, name)]
        for name, (_, columns) in TABLES.items()
    }
    orders = {row["order_id"]: row for row in tables["orders"]}
    assert len(tables["orders"]) == 15 and len(orders) == 10
    customers = {row["customer_id"] for row in tables["customers"]}
    products = {row["product_id"] for row in tables["products"]}
    assert all(row["customer_id"] in customers for row in orders.values())
    for item in tables["order_items"]:
        assert item["product_id"] in products
        assert Decimal(item["unit_price"]) * item["qty"] == Decimal(
            orders[item["order_id"]]["total_amount"]
        )
    for returned in tables["returns"]:
        assert returned["order_id"] in orders


def test_load_requires_walmart_before_connecting(tmp_path):
    root = generate(
        GeneratorConfig(channels=["amazon"], days=1, orders_per_day_per_channel=1), tmp_path
    )
    with pytest.raises(ValueError, match="Walmart"):
        load(root, None)
