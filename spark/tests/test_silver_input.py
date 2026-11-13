import json
from copy import deepcopy

import pytest

from spark.lib.bronze import input_digest
from spark.lib.bronze_input import identity
from spark.lib.silver_input import ENTITIES, load_pin, pin_completion, validate_completion


def fixture():
    from argparse import Namespace

    selection = dict(run_id="a" * 20, run_date="2026-01-01", channel="walmart", source="jdbc")
    state = dict(
        status="completed",
        contract="bronze-v1",
        selection=selection,
        batch_id=identity(**selection),
        manifest_hash="b" * 64,
        input_hash=input_digest("b" * 64, Namespace(**selection)),
        tables={
            f"lh.bronze.walmart_{name}_raw": {
                "snapshot": 123,
                "content": {"rows": "1", "h1": "2", "h16": "3"},
            }
            for name in ENTITIES
        },
    )
    return selection, state


def test_pin_survives_completion_replacement(tmp_path):
    selection, state = fixture()
    completion = tmp_path / "completion.json"
    completion.write_text(json.dumps(state))
    first = pin_completion(completion, selection, tmp_path / "pins")
    assert pin_completion(completion, selection, tmp_path / "pins") == first
    state["tables"]["lh.bronze.walmart_orders_raw"]["snapshot"] = 456
    completion.write_text(json.dumps(state))
    second = pin_completion(completion, selection, tmp_path / "pins")
    assert first != second
    assert load_pin(first)["tables"]["lh.bronze.walmart_orders_raw"]["snapshot"] == 123
    first.write_text("{}")
    with pytest.raises(ValueError):
        load_pin(first)


@pytest.mark.parametrize(
    "change", ["failed", "legacy", "table", "snapshot", "batch", "selection", "null"]
)
def test_reject_untrusted_completion(change):
    selection, state = fixture()
    state = deepcopy(state)
    if change == "failed":
        state["status"] = "failed"
    elif change == "legacy":
        state.pop("contract")
    elif change == "table":
        state["tables"].pop("lh.bronze.walmart_returns_raw")
    elif change == "snapshot":
        state["tables"]["lh.bronze.walmart_orders_raw"]["snapshot"] = "123; DROP"
    elif change == "batch":
        state["batch_id"] = "wrong"
    elif change == "selection":
        state["selection"]["run_date"] = "2026-01-02"
    else:
        state["tables"]["lh.bronze.walmart_orders_raw"]["snapshot"] = None
    with pytest.raises(ValueError):
        validate_completion(state, selection)


def test_walmart_export_is_not_authoritative():
    selection, state = fixture()
    selection["source"] = "jsonl"
    with pytest.raises(ValueError, match="authoritative"):
        validate_completion(state, selection)
