import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from spark.lib.silver_normalize import normalize, pii_hash

KEY = b"synthetic-unit-test-key-only-00000"
ORDER = dict(
    order_id="o",
    customer_id="c",
    order_ts="2026-01-01T07:00:00+07:00",
    updated_ts="2026-01-01T00:05:00Z",
    status="paid",
    currency="USD",
    shipping_country="VN",
    total_amount="10.25",
)


def test_strict_types_decimal_and_timezone():
    row, errors = normalize(json.dumps(ORDER), "orders", "walmart", KEY, "test-v1")
    assert not errors
    assert row["total_amount"] == Decimal("10.25")
    assert row["order_ts"] == datetime(2026, 1, 1, tzinfo=UTC)
    numeric = json.dumps(ORDER).replace('"10.25"', "10.25")
    assert normalize(numeric, "orders", "walmart", KEY, "test-v1")[0] == row


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("total_amount", "1.001", "decimal_scale"),
        ("total_amount", "10000000000000000", "decimal_overflow"),
        ("total_amount", "-1", "negative_amount"),
        ("total_amount", "NaN", "invalid_decimal"),
        ("order_id", True, "invalid_string"),
        ("order_id", None, "null_required_field"),
        ("order_ts", "2026-02-30T00:00:00Z", "invalid_timestamp"),
        ("order_ts", "2026-01-01T00:00:00", "invalid_timestamp"),
        ("currency", "usd", "invalid_currency"),
        ("status", "unknown", "invalid_enum"),
    ],
)
def test_invalid_values_do_not_escape_as_values(field, value, reason):
    payload = {**ORDER, field: value}
    row, errors = normalize(json.dumps(payload), "orders", "walmart", KEY, "test-v1")
    assert not row
    assert f"{field}:{reason}" in errors


@pytest.mark.parametrize(
    "raw,reason",
    [
        ("broken", "invalid_json"),
        ("42", "expected_object"),
        ("NaN", "invalid_json"),
        ("{}", "missing_required_field"),
    ],
)
def test_raw_structure(raw, reason):
    row, errors = normalize(raw, "orders", "walmart", KEY, "test-v1")
    assert not row and any(reason in error for error in errors)


def test_customer_pii_is_hashed_and_domain_separated():
    raw = dict(
        customer_id="c",
        full_name=" Fixture  User ",
        email="FIXTURE@example.invalid ",
        country="VN",
        updated_ts="2026-01-01T00:00:00Z",
    )
    row, errors = normalize(json.dumps(raw), "customers", "walmart", KEY, "test-v1")
    assert not errors
    assert "full_name" not in row and "email" not in row
    assert pii_hash("Fixture User", "name", "walmart", KEY) == row["name_hash"]
    assert pii_hash("fixture@example.invalid", "email", "walmart", KEY) == row["email_hash"]
    assert pii_hash("Fixture User", "name", "amazon", KEY) != row["name_hash"]
    with pytest.raises(ValueError):
        normalize(json.dumps(raw), "customers", "walmart", b"short", "test-v1")


def test_boolean_quantity_is_not_integer():
    raw = dict(
        order_id="o",
        product_id="p",
        seller_id="s",
        line_no=1,
        qty=True,
        unit_price="10.25",
        discount="0",
    )
    assert (
        "qty:invalid_positive_integer"
        in normalize(json.dumps(raw), "order_items", "walmart", KEY, "test-v1")[1]
    )


def test_discount_cannot_exceed_whole_line_amount():
    raw = dict(
        order_id="o",
        product_id="p",
        seller_id="s",
        line_no=1,
        qty=2,
        unit_price="10.25",
        discount="20.51",
    )
    assert (
        "discount:exceeds_line_amount"
        in normalize(json.dumps(raw), "order_items", "walmart", KEY, "test-v1")[1]
    )
