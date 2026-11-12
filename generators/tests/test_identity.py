"""Contract checks for identities that cross source boundaries."""

import json

import pytest

from generators.common.identity import entity_key


def test_same_source_id_in_different_channels_is_distinct():
    assert entity_key("amazon", "123") != entity_key("walmart", "123")


def test_opaque_id_round_trip():
    source_id = 'sku:01/"special"/đơn'
    assert json.loads(entity_key("amazon", source_id)) == ["amazon", source_id]
    assert entity_key("amazon", "001") != entity_key("amazon", "1")


@pytest.mark.parametrize("source_id", ["", "  ", None, 123])
def test_invalid_id_rejected(source_id):
    with pytest.raises(ValueError):
        entity_key("amazon", source_id)


def test_unknown_channel_rejected():
    with pytest.raises(ValueError):
        entity_key("unknown", "123")
