"""Behavioral checks for deterministic dirty data and reconciliation."""

import json
from collections import Counter
from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from generators.batch.dataset import generate, verify_local
from generators.common.config import GeneratorConfig


def records(root, channel):
    return [
        json.loads(line)
        for path in sorted((root / channel).rglob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]


def test_replay_is_byte_identical_and_seed_changes_identity(tmp_path):
    config = GeneratorConfig(days=1, orders_per_day_per_channel=10, rows_per_file=4)
    first = generate(config, tmp_path / "first")
    second = generate(config, tmp_path / "second")
    a = {p.relative_to(first): p.read_bytes() for p in first.rglob("*") if p.is_file()}
    b = {p.relative_to(second): p.read_bytes() for p in second.rglob("*") if p.is_file()}
    assert a == b
    assert generate(config, tmp_path / "first") == first
    assert GeneratorConfig(seed=43).run_id != GeneratorConfig(seed=42).run_id


def test_dup_skew_returns_schema_and_amounts(tmp_path):
    config = GeneratorConfig(
        days=2,
        orders_per_day_per_channel=100,
        dup_rate=0.1,
        schema_drift_at_day=1,
        rows_per_file=37,
    )
    root = generate(config, tmp_path)
    manifest = verify_local(root)
    for channel in config.channels:
        rows = records(root, channel)
        unique = {row["order_id"]: row for row in rows}
        assert len(rows) == 220
        assert len(unique) == 200
        assert sum(bool(row["returns"]) for row in unique.values()) == 20
        sellers = Counter(row["items"][0]["seller_id"] for row in unique.values())
        assert sellers[f"{channel}-s00"] == 100
        assert manifest["channels"][channel]["hot_seller_orders"] == 100
        assert all(f["rows"] <= 37 for f in manifest["channels"][channel]["files"])
        for row in unique.values():
            total = sum(
                Decimal(i["unit_price"]) * i["qty"] - Decimal(i["discount"]) for i in row["items"]
            )
            assert total == Decimal(row["total_amount"])
    amazon = records(root, "amazon")
    assert sum("buyer_id" in r for r in amazon) == 110
    assert sum("fulfillment_type" in r for r in amazon) == 110
    assert all(
        datetime.strptime(r["order_ts"], "%Y/%m/%d %H:%M:%S%z").utcoffset().total_seconds() == 25200
        for r in records(root, "shopee")
    )
    assert all(
        r["currency"] == "VND" and "brand" not in r["items"][0]["product"]
        for r in records(root, "lazada")
    )


def test_modified_or_extra_file_rejected(tmp_path):
    root = generate(GeneratorConfig(days=1, orders_per_day_per_channel=2), tmp_path)
    extra = root / "extra.jsonl"
    extra.write_text("{}\n")
    with pytest.raises(ValueError, match="inventory"):
        verify_local(root)
    extra.unlink()
    next(root.rglob("*.jsonl")).write_text("{}\n")
    with pytest.raises(ValueError, match="reconciliation"):
        verify_local(root)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"days": 0},
        {"dup_rate": -1},
        {"channels": []},
        {"channels": ["amazon", "amazon"]},
        {"unknown": 1},
    ],
)
def test_bad_config_rejected(kwargs):
    with pytest.raises(ValidationError):
        GeneratorConfig(**kwargs)


def test_default_config_round_trip_preserves_identity():
    config = GeneratorConfig()
    assert GeneratorConfig.model_validate(config.model_dump(mode="json")).run_id == config.run_id
    assert GeneratorConfig(channels=list(reversed(config.channels))).run_id == config.run_id
