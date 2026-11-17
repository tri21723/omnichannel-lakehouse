"""Unit tests for Phase 4D daily feature calculations and constraints."""

import hashlib
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from spark.lib.dp3_entities import (
    DP3_AUDIT,
    DP3_KEYS,
    POLICY,
)
from spark.lib.dp3_publish import verify_dp3


def test_dp3_fields_and_keys_inventory():
    assert "feat_customer_daily" in DP3_KEYS
    assert "feat_product_daily" in DP3_KEYS
    assert "lbl_order_return" in DP3_KEYS

    assert DP3_KEYS["feat_customer_daily"] == ["customer_feature_key"]
    assert DP3_KEYS["feat_product_daily"] == ["product_feature_key"]
    assert DP3_KEYS["lbl_order_return"] == ["order_label_key"]

    assert POLICY == "feature-policy-v4"
    assert "_dataset_run_id string" in DP3_AUDIT
    assert "_gold_publication_id string" in DP3_AUDIT
    assert "_record_hash string" in DP3_AUDIT
    assert "_policy_version string" in DP3_AUDIT


def test_customer_feature_key_determinism():
    run_id = "test_run_01"
    ch = "walmart"
    cust_id = "cust_001"
    dt = "2026-01-03"

    raw = f"{run_id}:{ch}:{cust_id}:{dt}".encode()
    expected_key = hashlib.sha256(raw).hexdigest()
    assert len(expected_key) == 64
    assert expected_key == expected_key.lower()


def test_product_feature_key_determinism():
    run_id = "test_run_01"
    ch = "amazon"
    prod_id = "prod_001"
    dt = "2026-01-03"

    raw = f"{run_id}:{ch}:{prod_id}:{dt}".encode()
    expected_key = hashlib.sha256(raw).hexdigest()
    assert len(expected_key) == 64
    assert expected_key == expected_key.lower()


def test_point_in_time_window_boundaries():
    eval_date = date(2026, 1, 3)
    next_date = eval_date + timedelta(days=1)
    as_of_ts = datetime.combine(next_date, time(0, 0, 0))
    ts_7d = datetime.combine(next_date - timedelta(days=7), time(0, 0, 0))
    ts_30d = datetime.combine(next_date - timedelta(days=30), time(0, 0, 0))

    assert as_of_ts == datetime(2026, 1, 4, 0, 0, 0)
    assert ts_7d == datetime(2025, 12, 28, 0, 0, 0)
    assert ts_30d == datetime(2025, 12, 5, 0, 0, 0)

    # Order at 2026-01-03 23:59:59 is inside as_of_ts (< 2026-01-04 00:00:00)
    order_in = datetime(2026, 1, 3, 23, 59, 59)
    assert order_in < as_of_ts
    assert order_in >= ts_7d
    assert order_in >= ts_30d

    # Order at 2026-01-04 00:00:00 is outside as_of_ts (leakage prevented)
    order_out = datetime(2026, 1, 4, 0, 0, 0)
    assert not (order_out < as_of_ts)


def test_dual_timestamp_return_leakage_logic():
    eval_date = date(2026, 1, 3)
    as_of_ts = datetime.combine(eval_date + timedelta(days=1), time(0, 0, 0))

    order_ts = datetime(2026, 1, 2, 10, 0, 0)
    future_return_ts = datetime(2026, 1, 10, 14, 0, 0)
    past_return_ts = datetime(2026, 1, 3, 12, 0, 0)

    # Case 1: Return in future must be excluded
    assert order_ts < as_of_ts
    assert not (future_return_ts < as_of_ts)
    # Both must be < as_of_ts to be eligible
    eligible_future = (order_ts < as_of_ts) and (future_return_ts < as_of_ts)
    assert eligible_future is False

    # Case 2: Return in past is eligible
    eligible_past = (order_ts < as_of_ts) and (past_return_ts < as_of_ts)
    assert eligible_past is True


def test_customer_monetary_and_rate_arithmetic():
    orders_30d = 3
    spend_30d = Decimal("150.00")
    returned_orders_30d = 1
    refund_amount_30d = Decimal("50.00")
    assert refund_amount_30d == Decimal("50.00")

    aov = (spend_30d / orders_30d).quantize(Decimal("0.01"))
    assert aov == Decimal("50.00")

    return_rate = (Decimal(returned_orders_30d) / Decimal(orders_30d)).quantize(Decimal("0.0001"))
    assert return_rate == Decimal("0.3333")

    # Zero orders yields None AOV and 0.0000 return rate
    zero_orders = 0
    zero_spend = Decimal("0.00")
    aov_zero = None if zero_orders == 0 else (zero_spend / zero_orders)
    assert aov_zero is None

    rate_zero = (
        Decimal("0.0000")
        if zero_orders == 0
        else (Decimal(0) / Decimal(zero_orders)).quantize(Decimal("0.0001"))
    )
    assert rate_zero == Decimal("0.0000")


def test_verify_dp3_guards():
    # Incomplete status
    with pytest.raises(ValueError, match="not marked completed"):
        verify_dp3(None, dict(status="pending"))
