import json

import pytest

from spark.lib.cdc import contiguous, merge_sql, parse_envelope

KEY = json.dumps({"order_id": "x-walmart-o1"})
ROW = {"order_id": "x-walmart-o1", "status": "paid", "total_amount": "12.30"}


def envelope(op, before=None, after=None, lsn=123, **extra):
    return json.dumps(
        {
            "op": op,
            "ts_ms": 1700,
            "before": before,
            "after": after,
            "source": {"lsn": lsn, "txId": 9},
            **extra,
        }
    )


@pytest.mark.parametrize(
    ("op", "before", "after"),
    [
        ("c", None, ROW),
        ("u", ROW, {**ROW, "status": "shipped"}),
        ("r", None, ROW),
        ("d", ROW, None),
    ],
)
def test_valid_operations_keep_images_as_json(op, before, after):
    parsed = parse_envelope(KEY, envelope(op, before, after))
    assert parsed["_parse_status"] == "valid" and parsed["_error_reason"] is None
    assert (parsed["op"], parsed["lsn"], parsed["tx_id"], parsed["ts_ms"]) == (op, 123, 9, 1700)
    assert parsed["order_id"] == "x-walmart-o1"
    if after:
        assert json.loads(parsed["after"])["total_amount"] == after["total_amount"]  # string, exact
    assert (parsed["before"] is None) == (before is None)


def test_tombstone_is_kept_not_applied():
    parsed = parse_envelope(KEY, None)
    assert parsed["_parse_status"] == "tombstone" and parsed["op"] is None


@pytest.mark.parametrize(
    ("key", "value", "reason"),
    [
        (KEY, "{not json", "invalid_json"),
        (KEY, json.dumps({"op": "x"}), "bad_op"),
        (KEY, envelope("c", None, ROW, lsn=None), "missing_lsn"),
        (KEY, envelope("u", ROW, {**ROW, "order_id": "other"}), "key_image_mismatch"),
        (KEY, envelope("d", None, None), "key_image_mismatch"),
        ("{bad key", envelope("c", None, ROW), "invalid_key"),
    ],
)
def test_problems_are_classified_never_raised(key, value, reason):
    parsed = parse_envelope(key, value)
    assert parsed["_parse_status"] == "invalid"
    assert parsed["_error_reason"] == reason


def test_merge_is_keyed_on_kafka_coordinates_and_only_inserts():
    sql = merge_sql("v")
    for column in ("_kafka_topic", "_kafka_partition", "_kafka_offset"):
        assert f"t.{column} = s.{column}" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql and "UPDATE" not in sql and "DELETE" not in sql


def test_contiguous_offsets_detect_gaps_and_missing_start():
    assert contiguous({0: (0, 9, 10), 1: (0, 4, 5)}, {0: 0, 1: 0}) == []
    assert contiguous({0: (0, 9, 9)}, {0: 0}) == [0]  # one offset missing
    assert contiguous({0: (3, 9, 7)}, {0: 0}) == [0]  # earliest records never ingested
    assert contiguous({0: (3, 9, 7)}, {0: 3}) == []  # retention removed 0..2


def test_bronze_schema_carries_every_parsed_field_and_kafka_coordinates():
    from spark.lib.cdc import BRONZE_SCHEMA, PARSED_SCHEMA

    bronze = [part.split()[0] for part in BRONZE_SCHEMA.split(", ")]
    parsed = [part.split()[0] for part in PARSED_SCHEMA.split(", ")]
    assert set(parsed) <= set(bronze)
    assert bronze[:3] == ["_kafka_topic", "_kafka_partition", "_kafka_offset"]
    assert {"_raw", "_key", "_ingest_ts", "_batch_id", "_channel", "_source_type"} <= set(bronze)


# --- Silver current state -----------------------------------------------------------------
def image(**overrides):
    row = {
        "order_id": "x-walmart-o1",
        "customer_id": "x-walmart-c1",
        "order_ts": "2026-04-01T00:00:01Z",
        "updated_ts": "2026-04-01T07:00:02+07:00",
        "status": "paid",
        "currency": "USD",
        "total_amount": "12.30",
        "shipping_country": "VN",
    }
    return json.dumps({**row, **overrides})


def test_typed_row_uses_before_for_deletes_and_normalizes_to_utc():
    from spark.lib.cdc import typed_row

    upsert = typed_row("u", None, image())
    assert upsert["_error_reason"] is None
    assert upsert["total_amount"] == "12.30"
    assert upsert["updated_ts"] == "2026-04-01 00:00:02.000000"  # +07:00 converted to UTC
    deleted = typed_row("d", image(status="cancelled"), None)
    assert deleted["status"] == "cancelled" and deleted["_error_reason"] is None


@pytest.mark.parametrize(
    ("op", "before", "after", "reason"),
    [
        ("u", None, None, "missing_row_image"),
        ("d", None, image(), "missing_row_image"),
        ("c", None, image(total_amount=12.3), "bad_amount"),
        ("c", None, image(total_amount="12.345"), "bad_amount"),
        ("c", None, image(total_amount="-1.00"), "bad_amount"),
        ("c", None, image(order_ts="2026-04-01T00:00:01"), "bad_order_ts"),
        ("c", None, image(status=None), "bad_status"),
    ],
)
def test_untypable_rows_are_quarantined_with_a_reason(op, before, after, reason):
    from spark.lib.cdc import typed_row

    assert typed_row(op, before, after)["_error_reason"] == reason


def test_only_strictly_newer_events_replace_state_in_any_arrival_order():
    import itertools

    from spark.lib.cdc import newer, silver_merge_sql

    events = [  # (lsn, offset, status); a duplicate delivery of lsn 30 at a later offset
        (10, 1, "paid"),
        (20, 2, "shipped"),
        (30, 3, "delivered"),
        (30, 7, "delivered"),
    ]
    for arrival in itertools.permutations(events):
        state = None
        for lsn, offset, status in arrival:
            if newer((lsn, offset), state and state[:2]):
                state = (lsn, offset, status)
        assert state == (30, 7, "delivered")
    sql = silver_merge_sql("v")
    assert "s._cdc_lsn > t._cdc_lsn" in sql and "s._kafka_offset > t._kafka_offset" in sql
    assert "ON t.order_id = s.order_id" in sql and "DELETE" not in sql  # soft delete only


def test_scenario_checksum_matches_the_generator_algorithm():
    from datetime import UTC, datetime

    from generators.cdc.scenario import checksum
    from spark.lib.cdc import scenario_checksum

    rows = [
        {
            "order_id": "b",
            "status": "paid",
            "total_amount": "1.00",
            "updated_ts": "2026-04-01T00:00:01+00:00",
        },
        {
            "order_id": "a",
            "status": "shipped",
            "total_amount": "20.50",
            "updated_ts": datetime(2026, 4, 1, 0, 0, 2, 5, tzinfo=UTC),
        },
    ]
    spark_style = [
        dict(r, updated_ts=datetime(2026, 4, 1, 0, 0, 1)) if r["order_id"] == "b" else r
        for r in rows
    ]
    assert scenario_checksum(rows) == checksum(rows) == scenario_checksum(spark_style)
