import json
from copy import deepcopy

import pytest

from spark.lib.silver_expand import expand
from spark.lib.silver_normalize import normalize
from spark.lib.silver_publish import revision

KEY = b"synthetic-unit-test-key-only-00000"


def test_products_optional_brand_and_future_returns():
    product = dict(
        product_id="p",
        seller_id="s",
        title="Demo",
        category="Demo",
        updated_ts="2026-01-01T00:00:00Z",
    )
    result, errors = normalize(json.dumps(product), "products", "lazada", KEY, "v1")
    assert not errors and result["brand"] is None
    returned = dict(
        return_id="r",
        return_line_no=1,
        order_id="o",
        line_no=1,
        reason="fixture",
        return_ts="2026-01-15T00:00:00Z",
        refund_amount="12.25",
    )
    result, errors = normalize(json.dumps(returned), "returns", "walmart", KEY, "v1")
    assert not errors and result["return_ts"].day == 15


def test_mapping_missing_empty_arrays_and_alias_conflict():
    order = dict(order_id="o", buyer_id="before", customer_id="after", items=[], returns=[])
    records = expand(json.dumps(order), "amazon")
    parent = json.loads(records[0]["_raw"])
    assert parent["_mapping_error"]
    assert parent["_array_errors"] == ["items:empty_required_array"]
    assert [r["entity"] for r in records] == ["orders", "customers"]
    absent = deepcopy(order)
    absent.pop("returns")
    assert (
        "returns:invalid_or_missing_array"
        in json.loads(expand(json.dumps(absent), "amazon")[0]["_raw"])["_array_errors"]
    )


def test_shopee_timezone_boundary_and_bad_date():
    from spark.lib.silver_normalize import Fields

    fields = Fields({"ts": "2026/01/01 00:30:00+0700"}, "shopee")
    assert fields.timestamp("ts").isoformat() == "2025-12-31T17:30:00+00:00"
    fields = Fields({"ts": "2026/02/30 00:30:00+0700"}, "shopee")
    assert fields.timestamp("ts") is None


def test_revision_cannot_silently_restore_old_pin_or_mix_key_versions():
    old = dict(input_id="new", revision=2, key_version="v1", superseded=["old"])
    assert revision(old, "new", "v1") == 2
    with pytest.raises(ValueError):
        revision(old, "old", "v1")
    assert revision(old, "old", "v1", correction=True) == 3
    with pytest.raises(ValueError):
        revision(old, "new", "v2")


def test_invalid_unicode_customer_rejects_without_raw_value():
    record = dict(
        customer_id="c",
        full_name="\ud800",
        email="fixture@example.invalid",
        country="VN",
        updated_ts="2026-01-01T00:00:00Z",
    )
    row, errors = normalize(json.dumps(record), "customers", "walmart", KEY, "v1")
    assert row == {} and "full_name:invalid_unicode" in errors


@pytest.mark.parametrize(
    "old,new,accepted",
    [(1, 3, True), (3, 1, False), (2, 3, True), (1, 4, False), (3, None, False), (None, 3, True)],
)
def test_unseen_stale_snapshot_ancestry(old, new, accepted, tmp_path):
    from types import SimpleNamespace

    from spark.lib.silver_publish import check_source_order

    graph = [
        dict(snapshot_id=1, parent_id=None),
        dict(snapshot_id=2, parent_id=1),
        dict(snapshot_id=3, parent_id=2),
    ]
    spark = SimpleNamespace(sql=lambda query: SimpleNamespace(collect=lambda: graph))
    previous = dict(input_tables={"lh.bronze.walmart_orders_raw": dict(snapshot=old)})
    state = dict(tables={"lh.bronze.walmart_orders_raw": dict(snapshot=new)})
    if accepted:
        check_source_order(spark, previous, state, tmp_path)
    else:
        with pytest.raises(ValueError):
            check_source_order(spark, previous, state, tmp_path)


def test_child_parent_ids_are_validated_before_injection():
    payload = dict(
        order_id="o",
        customer_id="c",
        customer=dict(customer_id="other"),
        items=[dict(order_id="wrong", product_id="p", product=dict(product_id="other-p"))],
        returns=[dict(order_id="wrong")],
    )
    rows = expand(json.dumps(payload), "tiktok_shop")
    order = json.loads(rows[0]["_raw"])
    item = json.loads(next(r["_raw"] for r in rows if r["entity"] == "order_items"))
    returned = json.loads(next(r["_raw"] for r in rows if r["entity"] == "returns"))
    assert order["_customer_error"] and item["_product_error"]
    assert item["_parent_error"] and returned["_parent_error"]


def test_unknown_product_fields_do_not_activate_order_mapping_rules():
    raw = dict(
        product_id="p",
        seller_id="s",
        title="fixture",
        category="fixture",
        updated_ts="2026-01-01T00:00:00Z",
        _parent_error=True,
        _customer_error=True,
    )
    row, errors = normalize(json.dumps(raw), "products", "lazada", KEY, "v1")
    assert not errors and row["product_id"] == "p"


@pytest.mark.parametrize("brand", [42, "\ud800"])
def test_optional_product_strings_reject_bad_type_or_unicode(brand):
    raw = dict(
        product_id="p",
        seller_id="s",
        title="fixture",
        category="fixture",
        updated_ts="2026-01-01T00:00:00Z",
        brand=brand,
    )
    row, errors = normalize(json.dumps(raw), "products", "lazada", KEY, "v1")
    assert not row and any(error.startswith("brand:") for error in errors)


@pytest.mark.parametrize("key,version", [(None, "v1"), ("00", "v1"), ("zz", "v1"), ("00" * 32, "")])
def test_missing_or_invalid_pii_settings_fail_before_spark(monkeypatch, key, version):
    from spark.jobs.dp2_silver.main import key_settings

    monkeypatch.delenv("SILVER_PII_KEY", raising=False)
    if key is not None:
        monkeypatch.setenv("SILVER_PII_KEY", key)
    monkeypatch.setenv("SILVER_PII_KEY_VERSION", version)
    with pytest.raises((KeyError, ValueError)):
        key_settings()


def test_incomplete_ledger_rejected_before_table_reads():
    from spark.lib.silver_publish import verify

    with pytest.raises(ValueError, match="Incomplete Silver table inventory"):
        verify(None, dict(status="completed", tables={}))
