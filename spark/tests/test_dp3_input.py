"""Unit tests for Phase 4D Gold publication input reader and run isolation."""

import json
from copy import deepcopy
from pathlib import Path

import pytest

from spark.lib.dp3_input import (
    GOLD_TABLES,
    compute_publication_id,
    load_gold_publication,
    validate_gold_run_membership,
)


def sample_gold_ledger() -> dict:
    tables = {
        f"lh.gold.{name}": {
            "snapshot": 2001,
            "content": {"rows": "100", "h1": "abc", "h2": "def"},
        }
        for name in GOLD_TABLES
    }
    runs = {
        "run_fixture_1": {
            "silver_publication_id": "silver_pub_123",
            "policy_version": "gold-policy-v1",
            "updated_at": 1700000000.0,
        },
        "run_reference_2": {
            "silver_publication_id": "silver_pub_456",
            "policy_version": "gold-policy-v1",
            "updated_at": 1700000050.0,
        },
    }
    return dict(status="completed", tables=tables, runs=runs, duration_seconds=12.5)


def test_compute_gold_publication_id_determinism() -> None:
    ledger = sample_gold_ledger()
    id1 = compute_publication_id(ledger)
    id2 = compute_publication_id(deepcopy(ledger))
    assert id1 == id2
    assert len(id1) == 64


def test_load_gold_publication_success(tmp_path: Path) -> None:
    ledger = sample_gold_ledger()
    pub_id = compute_publication_id(ledger)

    ledger_file = tmp_path / "ledger.json"
    archive_file = tmp_path / f"publication-{pub_id}.json"

    ledger_file.write_text(json.dumps(ledger), encoding="utf-8")
    archive_file.write_text(json.dumps(ledger), encoding="utf-8")

    loaded, returned_id = load_gold_publication(ledger_file)
    assert loaded["status"] == "completed"
    assert returned_id == pub_id

    # Test explicit matching publication_id
    loaded2, returned_id2 = load_gold_publication(ledger_file, publication_id=pub_id)
    assert returned_id2 == pub_id


def test_load_gold_publication_failures(tmp_path: Path) -> None:
    ledger = sample_gold_ledger()
    ledger_file = tmp_path / "ledger.json"

    # Missing file
    with pytest.raises(FileNotFoundError):
        load_gold_publication(tmp_path / "nonexistent.json")

    # Incomplete status
    bad_ledger = deepcopy(ledger)
    bad_ledger["status"] = "running"
    ledger_file.write_text(json.dumps(bad_ledger), encoding="utf-8")
    with pytest.raises(ValueError, match="not completed"):
        load_gold_publication(ledger_file)

    # Wrong publication ID
    ledger_file.write_text(json.dumps(ledger), encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        load_gold_publication(ledger_file, publication_id="0" * 64)

    # Missing table in inventory
    bad_inv = deepcopy(ledger)
    bad_inv["tables"].pop("lh.gold.fact_orders")
    ledger_file.write_text(json.dumps(bad_inv), encoding="utf-8")
    with pytest.raises(ValueError, match="Incomplete Gold table inventory"):
        load_gold_publication(ledger_file)

    # Archive content mismatch
    pub_id = compute_publication_id(ledger)
    archive_file = tmp_path / f"publication-{pub_id}.json"
    archive_file.write_text(json.dumps(dict(tampered=True)), encoding="utf-8")
    ledger_file.write_text(json.dumps(ledger), encoding="utf-8")
    with pytest.raises(ValueError, match="Corrupted publication archive"):
        load_gold_publication(ledger_file)


def test_validate_gold_run_membership() -> None:
    ledger = sample_gold_ledger()
    meta = validate_gold_run_membership(ledger, "run_fixture_1")
    assert meta["silver_publication_id"] == "silver_pub_123"

    with pytest.raises(ValueError, match="not present in Gold publication runs"):
        validate_gold_run_membership(ledger, "nonexistent_run")
