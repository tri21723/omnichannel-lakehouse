"""Unit tests for Phase 4D publication ledger, loading, and recovery guards."""

import hashlib
import json

import pytest

from spark.lib.dp3_publish import load_dp3_publication


def test_load_dp3_publication_missing(tmp_path):
    missing = tmp_path / "ledger.json"
    with pytest.raises(FileNotFoundError, match="DP3 publication ledger not found"):
        load_dp3_publication(missing)


def test_load_dp3_publication_not_completed(tmp_path):
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps({"status": "running"}))
    with pytest.raises(ValueError, match="DP3 publication is not completed"):
        load_dp3_publication(ledger_path)


def test_load_dp3_publication_archive_corrupted(tmp_path):
    ledger = {"status": "completed", "runs": {}, "tables": {}}
    pub_id = hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest()

    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(ledger))

    archive_path = tmp_path / f"publication-{pub_id}.json"
    archive_path.write_text(json.dumps({"status": "corrupted"}))

    with pytest.raises(ValueError, match="Corrupted publication archive"):
        load_dp3_publication(ledger_path)


def test_load_dp3_publication_id_mismatch(tmp_path):
    ledger = {"status": "completed", "runs": {}, "tables": {}}
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(ledger))

    with pytest.raises(ValueError, match="Requested publication ID wrong does not match"):
        load_dp3_publication(ledger_path, publication_id="wrong")


def test_load_dp3_publication_success(tmp_path):
    ledger = {"status": "completed", "runs": {"run_1": {}}, "tables": {}}
    pub_id = hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest()

    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(ledger))

    archive_path = tmp_path / f"publication-{pub_id}.json"
    archive_path.write_text(json.dumps(ledger))

    loaded, returned_id = load_dp3_publication(ledger_path, publication_id=pub_id)
    assert returned_id == pub_id
    assert loaded["status"] == "completed"
    assert "run_1" in loaded["runs"]
