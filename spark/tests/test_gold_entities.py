import datetime

from spark.lib.gold_entities import (
    CHANNELS,
    GOLD_FIELDS,
    GOLD_KEYS,
    channel_key,
    customer_key,
    date_key_int,
    order_item_key,
    order_key,
    product_key,
    return_key,
    seller_key,
)


def test_surrogate_key_determinism_and_format():
    run_id = "test_run_12345"
    ch = "walmart"
    cust_id = "cust_001"

    key1 = customer_key(run_id, ch, cust_id)
    key2 = customer_key(run_id, ch, cust_id)
    assert key1 == key2
    assert len(key1) == 64
    assert key1 == key1.lower()


def test_cross_channel_isolation():
    run_id = "test_run_12345"
    order_id = "ORD_COMMON"

    key_walmart = order_key(run_id, "walmart", order_id)
    key_amazon = order_key(run_id, "amazon", order_id)
    assert key_walmart != key_amazon

    item_walmart = order_item_key(run_id, "walmart", order_id, 1)
    item_amazon = order_item_key(run_id, "amazon", order_id, 1)
    assert item_walmart != item_amazon


def test_cross_run_isolation():
    order_id = "ORD_123"
    ch = "shopee"

    key_run1 = order_key("run_1", ch, order_id)
    key_run2 = order_key("run_2", ch, order_id)
    assert key_run1 != key_run2


def test_channel_key_and_seller_product_return_keys():
    run_id = "run_abc"
    ch = "tiktok_shop"

    ck = channel_key(ch)
    assert len(ck) == 64

    pk = product_key(run_id, ch, "prod_1")
    sk = seller_key(run_id, ch, "seller_1")
    rk = return_key(run_id, ch, "ret_1", 1)

    assert len(pk) == 64
    assert len(sk) == 64
    assert len(rk) == 64


def test_date_key_formatting():
    dt = datetime.date(2026, 1, 15)
    assert date_key_int(dt) == 20260115

    dt2 = datetime.datetime(2026, 12, 31, 23, 59, 59)
    assert date_key_int(dt2) == 20261231


def test_table_definitions_completeness():
    expected_tables = {
        "dim_date",
        "dim_channel",
        "dim_customer",
        "dim_product",
        "dim_seller",
        "fact_orders",
        "fact_order_items",
        "fact_returns",
    }
    assert set(GOLD_FIELDS.keys()) == expected_tables
    assert set(GOLD_KEYS.keys()) == expected_tables
    assert set(CHANNELS.keys()) == {"walmart", "amazon", "tiktok_shop", "shopee", "lazada"}
