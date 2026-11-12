"""Reproducible synthetic orders; no wall clock or process-randomized hash()."""

import hashlib
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

from faker import Faker

from generators.common.config import GeneratorConfig


def stable_seed(*parts: object) -> int:
    return int.from_bytes(hashlib.sha256(repr(parts).encode()).digest()[:8], "big")


def daily_orders(config: GeneratorConfig, channel: str, day: int) -> Iterator[dict]:
    """Yield canonical unique orders. Skew and returns have exact rounded counts."""
    count = config.orders_per_day_per_channel
    rng = random.Random(stable_seed(config.seed, channel, day))
    hot = set(rng.sample(range(count), round(count * config.skew_top_seller_share)))
    returned = set(rng.sample(range(count), round(count * config.return_rate)))
    start = datetime.combine(config.start_date, datetime.min.time(), UTC)
    day_start = start + timedelta(days=day)
    fake = Faker("en_US")
    for index in range(count):
        oid = f"{config.run_id}-{channel}-{day:03d}-{index:07d}"
        cid = f"{config.run_id}-{channel}-c{index % max(1, count // 5):07d}"
        product = 0 if index in hot else rng.randint(1, 19)
        pid = f"{config.run_id}-{channel}-p{product:02d}"
        seller = f"{channel}-s{product:02d}"
        fake.seed_instance(stable_seed(config.seed, cid))
        customer = {
            "customer_id": cid,
            "full_name": fake.name(),
            "email": f"{cid}@example.invalid",
            "country": "VN",
            "updated_ts": start.isoformat(),
        }
        ts = day_start + timedelta(seconds=rng.randrange(86400))
        qty = rng.randint(1, 4)
        unit_price = Decimal(rng.randint(100, 20000)) / 100
        currency = "USD"
        if channel == "lazada":
            currency = "VND"
            unit_price *= 25000  # Synthetic price scale, not an FX conversion rate.
        amount = unit_price * qty
        item = {
            "line_no": 1,
            "product_id": pid,
            "seller_id": seller,
            "qty": qty,
            "unit_price": f"{unit_price:.2f}",
            "discount": "0.00",
            "product": {
                "product_id": pid,
                "seller_id": seller,
                "title": f"Synthetic product {product}",
                "category": ["electronics", "home", "fashion"][product % 3],
                "brand": "DemoBrand",
                "updated_ts": start.isoformat(),
            },
        }
        returns = []
        if index in returned:
            returns.append(
                {
                    "return_id": f"r-{oid}",
                    "return_line_no": 1,
                    "order_id": oid,
                    "line_no": 1,
                    "return_ts": (ts + timedelta(days=rng.randint(1, 14))).isoformat(),
                    "reason": "changed_mind",
                    "refund_amount": f"{amount:.2f}",
                }
            )
        yield {
            "order_id": oid,
            "customer_id": cid,
            "order_ts": ts.isoformat(),
            "updated_ts": (ts + timedelta(minutes=5)).isoformat(),
            "status": "paid",
            "currency": currency,
            "total_amount": f"{amount:.2f}",
            "shipping_country": "VN",
            "customer": customer,
            "items": [item],
            "returns": returns,
        }


def source_order(order: dict, channel: str, day: int, config: GeneratorConfig) -> dict:
    """Map canonical records to deliberate channel differences."""
    if channel == "amazon":
        if day < config.schema_drift_at_day:
            order["buyer_id"] = order.pop("customer_id")
        else:
            order["fulfillment_type"] = "FBA"
    elif channel == "shopee":
        for field in ("order_ts", "updated_ts"):
            order[field] = (
                datetime.fromisoformat(order[field])
                .astimezone(timezone(timedelta(hours=7)))
                .strftime("%Y/%m/%d %H:%M:%S%z")
            )
    elif channel == "lazada":
        for item in order["items"]:
            item["product"].pop("brand")
    return order
