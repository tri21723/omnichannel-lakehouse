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


def test_scoped_provenance_does_not_promote_other_tables_or_dates():
    from spark.lib.dp3_publish import record_scopes, scopes_are_current

    run = {}
    labels = ["lbl_order_return"]
    features = ["feat_customer_daily", "feat_product_daily"]
    dates = ["2026-01-01", "2026-01-02"]
    record_scopes(run, labels, dates, "A", 14, "full")
    record_scopes(run, features, dates, "B", 14, "full")
    assert not scopes_are_current(run, labels, dates, "B", 14, "full")
    record_scopes(run, labels, dates[:1], "B", 7, "incremental")
    assert scopes_are_current(run, labels, dates[:1], "B", 7, "incremental")
    assert not scopes_are_current(run, labels, dates, "B", 7, "incremental")
    assert not scopes_are_current(run, labels, dates[:1], "B", 14, "incremental")
    assert not scopes_are_current(run, labels, dates[:1], "B", 7, "full")
    assert scopes_are_current(run, features, dates, "B", 7, "full")
    run["table_date_provenance"]["lh.gold.feat_customer_daily"][dates[0]][
        "policy"
    ] = "gold-policy-v1"
    assert not scopes_are_current(run, features, dates, "B", 7, "full")
    record_scopes(run, labels, dates[:1], "B", 7, "full")
    assert scopes_are_current(run, labels, dates[:1], "B", 7, "full")
    assert not scopes_are_current(run, labels, dates[1:], "A", 14, "incremental")
    assert not scopes_are_current({"gold_publication_id": "B"}, labels, dates, "B", 14, "full")


@pytest.mark.parametrize("stale", [True, False])
def test_publish_uses_scoped_skip_guard(monkeypatch, tmp_path, stale):
    import sys
    from unittest.mock import MagicMock

    import spark.lib.dp3_publish as dp

    run = {}
    dates = ["2026-01-01"]
    dp.record_scopes(run, ["lbl_order_return"], dates, "A" if stale else "B", 14, "full")
    run.update(gold_publication_id="B", horizon_days=14)
    ledger = {"status": "completed", "runs": {"r": run}, "tables": {"lh.gold.lbl_order_return": {}}}
    (tmp_path / "ledger.json").write_text(json.dumps(ledger))
    monkeypatch.setitem(sys.modules, "pyspark", MagicMock())
    monkeypatch.setitem(sys.modules, "pyspark.sql", MagicMock())
    verified = MagicMock()
    monkeypatch.setattr(dp, "verify_dp3", verified)
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("recompute reached")
    kwargs = dict(root=tmp_path, as_of_dates=dates, mode="incremental")
    if stale:
        with pytest.raises(RuntimeError, match="recompute reached"):
            dp.publish_dp3(spark, {"lbl_order_return": None}, "r", "B", **kwargs)
        verified.assert_not_called()
    else:
        _, skipped = dp.publish_dp3(spark, {"lbl_order_return": None}, "r", "B", **kwargs)
        assert skipped
        verified.assert_called_once()
        spark.sql.assert_not_called()
