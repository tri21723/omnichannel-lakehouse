"""Real Spark negative cases; fixtures remain in memory and never publish tables."""

import json
import logging
import os
from decimal import Decimal

from spark.lib.bronze import event
from spark.lib.session import get_spark
from spark.lib.silver_transform import walmart_candidates


def run():
    spark = get_spark()
    try:
        timestamp = "2026-01-01T00:00:00Z"
        order = dict(
            order_id="o1",
            customer_id="c1",
            order_ts=timestamp,
            updated_ts=timestamp,
            status="paid",
            currency="USD",
            total_amount="10.25",
            shipping_country="VN",
        )
        customer = dict(
            customer_id="c1",
            full_name="Fixture User",
            email="fixture@example.invalid",
            country="VN",
            updated_ts=timestamp,
        )
        item = dict(
            order_id="o1",
            line_no=1,
            product_id="p",
            seller_id="s",
            qty=1,
            unit_price="10.25",
            discount="0",
        )
        payloads = {
            "orders": [
                order,
                {**order, "order_id": "o2", "total_amount": "1.001"},
                {**order, "order_id": "o3", "total_amount": "10.26"},
            ],
            "customers": [customer, {**customer, "customer_id": "c2", "email": None}],
            "order_items": [
                item,
                {**item, "order_id": "o2", "qty": True},
                {**item, "order_id": "o3"},
            ],
        }
        frames = {
            f"lh.bronze.walmart_{name}_raw": spark.createDataFrame(
                [(json.dumps(row),) for row in rows], "_raw string"
            )
            for name, rows in payloads.items()
        }
        state = {
            "selection": {"run_id": "f" * 20, "channel": "walmart"},
            "batch_id": "fixture",
            "tables": {table: {"snapshot": None} for table in frames},
        }
        result = walmart_candidates(
            spark,
            frames,
            state,
            "fixture",
            bytes.fromhex(os.environ["SILVER_PII_KEY"]),
            os.environ["SILVER_PII_KEY_VERSION"],
        )
        for name, expected in {
            "orders": (1, 2),
            "customers": (1, 1),
            "order_items": (1, 2),
        }.items():
            counts = result[name]["valid"].count(), result[name]["rejected"].count()
            assert counts == expected
            assert sum(counts) == result[name]["input_rows"]
            assert "_raw" not in result[name]["rejected"].columns
        typed = result["orders"]["valid"].first()
        assert typed["total_amount"] == Decimal("10.25")
        assert str(typed["order_ts"]) == "2026-01-01 00:00:00"
        assert result["order_items"]["valid"].first()["updated_ts"] == typed["updated_ts"]
        assert result["customers"]["valid"].first()["first_order_ts"] == typed["order_ts"]
        assert len(result["customers"]["valid"].first()["email_hash"]) == 64
        event("silver_normalization_cases_passed", entities=3, rejected=5)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
