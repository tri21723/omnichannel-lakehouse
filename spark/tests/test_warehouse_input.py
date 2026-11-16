"""Unit tests for Phase 4E Gold publication reader and 30-day window slicing."""

import json
import sys
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark.lib.warehouse_input import (
    DEFAULT_GOLD_ROOT,
    EXPORT_TABLES,
    compute_export_window,
    compute_publication_id,
    is_in_export_window,
    load_gold_publication,
    parse_run_date,
    validate_gold_run_membership,
)


def _sample_gold_ledger() -> dict:
    tables = {
        f"lh.gold.{name}": {
            "snapshot": 3000 + idx,
            "content": {"rows": "10", "h1": "aaa", "h2": "bbb"},
        }
        for idx, name in enumerate(EXPORT_TABLES)
    }
    runs = {
        "186ae107d0793d2ee237": {"silver_publication_id": "silver_1"},
        "b1e64afaff0176de180a": {"silver_publication_id": "silver_2"},
    }
    return {"status": "completed", "tables": tables, "runs": runs}


def test_parse_run_date_and_window_bounds():
    assert str(DEFAULT_GOLD_ROOT) == "/runtime/gold/publication"
    assert len(EXPORT_TABLES) == 8

    assert parse_run_date("2026-01-17") == date(2026, 1, 17)
    assert parse_run_date(datetime(2026, 1, 17, 23, 59, tzinfo=UTC)) == date(2026, 1, 17)

    with pytest.raises(ValueError):
        parse_run_date("2026-01")
    with pytest.raises(ValueError):
        parse_run_date("not-a-date")

    w_start, w_end = compute_export_window("2026-01-17")
    assert w_start == datetime(2025, 12, 19, 0, 0, 0, tzinfo=UTC)
    assert w_end == datetime(2026, 1, 18, 0, 0, 0, tzinfo=UTC)
    assert (w_end - w_start).days == 30

    # Advancing run_date to 2026-02-01 prunes 2026-01-01 and 2026-01-02 cleanly.
    pruned_start, pruned_end = compute_export_window("2026-02-01")
    assert pruned_start == datetime(2026, 1, 3, 0, 0, 0, tzinfo=UTC)
    assert pruned_end == datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)


def test_window_half_open_boundaries():
    w_start, w_end = compute_export_window("2026-02-01")
    assert is_in_export_window("2026-01-03T00:00:00Z", w_start, w_end) is True
    assert is_in_export_window("2026-02-01T23:59:59.999999Z", w_start, w_end) is True
    assert is_in_export_window("2026-01-02T23:59:59Z", w_start, w_end) is False
    assert is_in_export_window("2026-02-02T00:00:00Z", w_start, w_end) is False
    # Offset-aware non-UTC timestamps are converted to UTC before checking bounds.
    assert is_in_export_window("2026-01-03T05:00:00+07:00", w_start, w_end) is False
    assert is_in_export_window("2026-02-02T06:00:00+07:00", w_start, w_end) is True


def test_gold_ledger_validation_and_run_isolation(tmp_path: Path):
    ledger = _sample_gold_ledger()
    pub_id = compute_publication_id(ledger)

    ledger_path = tmp_path / "ledger.json"
    archive_path = tmp_path / f"publication-{pub_id}.json"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    archive_path.write_text(json.dumps(ledger), encoding="utf-8")

    loaded, returned_id = load_gold_publication(ledger_path, publication_id=pub_id)
    assert returned_id == pub_id
    assert (
        validate_gold_run_membership(loaded, "186ae107d0793d2ee237")["silver_publication_id"]
        == "silver_1"
    )

    # Reject incomplete ledger
    incomplete = deepcopy(ledger)
    incomplete["status"] = "failed"
    ledger_path.write_text(json.dumps(incomplete), encoding="utf-8")
    with pytest.raises(ValueError, match="not completed"):
        load_gold_publication(ledger_path)

    # Reject missing table
    missing_tbl = deepcopy(ledger)
    missing_tbl["tables"].pop("lh.gold.fact_returns")
    ledger_path.write_text(json.dumps(missing_tbl), encoding="utf-8")
    with pytest.raises(ValueError, match="Incomplete Gold table inventory"):
        load_gold_publication(ledger_path)

    # Reject tampered archive
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    archive_path.write_text(json.dumps({"tampered": True}), encoding="utf-8")
    with pytest.raises(ValueError, match="Corrupted publication archive"):
        load_gold_publication(ledger_path)

    # Reject unknown run_id
    with pytest.raises(ValueError, match="not present"):
        validate_gold_run_membership(ledger, "unknown_run_id")


def test_read_gold_entities_keeps_is_deleted_when_requested(monkeypatch, tmp_path: Path):
    """Verify warehouse reader checks summary digests and preserves is_deleted rows."""
    import spark.lib.silver_publish as silver_publish
    from spark.lib.warehouse_input import read_gold_star_schema

    fake_col = MagicMock()
    fake_functions = SimpleNamespace(
        col=lambda _name: fake_col,
        coalesce=lambda *_args: fake_col,
        lit=lambda _val: fake_col,
    )
    fake_pyspark = SimpleNamespace(sql=SimpleNamespace(functions=fake_functions))
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_pyspark.sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)

    ledger = _sample_gold_ledger()
    pub_id = compute_publication_id(ledger)
    (tmp_path / "ledger.json").write_text(json.dumps(ledger), encoding="utf-8")
    (tmp_path / f"publication-{pub_id}.json").write_text(json.dumps(ledger), encoding="utf-8")

    fake_frame = MagicMock()
    fake_scoped = MagicMock()
    fake_frame.filter.return_value = fake_scoped
    fake_scoped.localCheckpoint.return_value = fake_scoped

    fake_spark = MagicMock()
    fake_spark.catalog.tableExists.return_value = True

    monkeypatch.setattr(silver_publish, "frame_at", lambda *_args, **_kwargs: fake_frame)
    monkeypatch.setattr(
        silver_publish,
        "summary",
        lambda *_args, **_kwargs: {"rows": "10", "h1": "aaa", "h2": "bbb"},
    )

    frames, snapshots, _run_meta, returned_pub_id = read_gold_star_schema(
        fake_spark, "186ae107d0793d2ee237", gold_root=tmp_path
    )
    assert returned_pub_id == pub_id
    assert len(frames) == 8
    assert snapshots["fact_orders"] == 3005
    # fact_orders.filter is called only once for _dataset_run_id, NOT a second time on scoped
    assert fake_scoped.filter.call_count == 0

    # Summary mismatch fails closed before exposing data downstream
    monkeypatch.setattr(
        silver_publish,
        "summary",
        lambda *_args, **_kwargs: {"rows": "999", "h1": "bad", "h2": "bad"},
    )
    with pytest.raises(ValueError, match="Gold snapshot verification failed"):
        read_gold_star_schema(fake_spark, "186ae107d0793d2ee237", gold_root=tmp_path)
