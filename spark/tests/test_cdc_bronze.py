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
