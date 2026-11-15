import json
from copy import deepcopy

import pytest

from spark.lib.gold_input import (
    SILVER_ENTITIES,
    compute_publication_id,
    load_publication,
    validate_run_membership,
)


def sample_ledger():
    tables = {
        f"lh.silver.{prefix}{entity}": {
            "snapshot": 1001,
            "content": {"rows": "10", "h1": "100", "h2": "200"},
        }
        for entity in SILVER_ENTITIES
        for prefix in ("", "_contrib_", "quarantine_")
    }
    slices = {
        "batch_001": {
            "selection": {
                "run_id": "test_run_1",
                "run_date": "2026-01-01",
                "channel": "walmart",
            }
        }
    }
    return dict(status="completed", tables=tables, slices=slices)


def test_compute_publication_id_determinism():
    ledger = sample_ledger()
    id1 = compute_publication_id(ledger)
    id2 = compute_publication_id(deepcopy(ledger))
    assert id1 == id2
    assert len(id1) == 64


def test_load_publication_success(tmp_path):
    ledger = sample_ledger()
    pub_id = compute_publication_id(ledger)

    ledger_file = tmp_path / "ledger.json"
    archive_file = tmp_path / f"publication-{pub_id}.json"

    ledger_file.write_text(json.dumps(ledger))
    archive_file.write_text(json.dumps(ledger))

    loaded, returned_id = load_publication(ledger_file)
    assert loaded["status"] == "completed"
    assert returned_id == pub_id

    # Test explicit matching publication_id
    loaded2, returned_id2 = load_publication(ledger_file, publication_id=pub_id)
    assert returned_id2 == pub_id


def test_load_publication_failures(tmp_path):
    ledger = sample_ledger()
    ledger_file = tmp_path / "ledger.json"

    # Missing file
    with pytest.raises(FileNotFoundError):
        load_publication(tmp_path / "nonexistent.json")

    # Incomplete status
    bad_ledger = deepcopy(ledger)
    bad_ledger["status"] = "running"
    ledger_file.write_text(json.dumps(bad_ledger))
    with pytest.raises(ValueError, match="not completed"):
        load_publication(ledger_file)

    # Wrong publication ID
    ledger_file.write_text(json.dumps(ledger))
    with pytest.raises(ValueError, match="does not match"):
        load_publication(ledger_file, publication_id="wrong" * 16)

    # Missing table in inventory
    bad_inv = deepcopy(ledger)
    bad_inv["tables"].pop("lh.silver.orders")
    ledger_file.write_text(json.dumps(bad_inv))
    with pytest.raises(ValueError, match="Incomplete Silver table inventory"):
        load_publication(ledger_file)

    # Archive content mismatch
    pub_id = compute_publication_id(ledger)
    archive_file = tmp_path / f"publication-{pub_id}.json"
    archive_file.write_text(json.dumps(dict(tampered=True)))
    ledger_file.write_text(json.dumps(ledger))
    with pytest.raises(ValueError, match="Corrupted publication archive"):
        load_publication(ledger_file)


def test_validate_run_membership():
    ledger = sample_ledger()
    assert validate_run_membership(ledger, "test_run_1") == ["batch_001"]

    with pytest.raises(ValueError, match="has no slices"):
        validate_run_membership(ledger, "nonexistent_run")
