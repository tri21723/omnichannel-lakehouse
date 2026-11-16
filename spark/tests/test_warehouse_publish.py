"""Unit tests for Phase 4E export ledger, POSIX locking, skip, and rollback preservation."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark.lib.warehouse_jdbc import INSERT_ORDER, WarehouseFaultInjectionError
from spark.lib.warehouse_publish import (
    compute_export_id,
    load_warehouse_ledger,
    publish_warehouse,
    should_skip_export,
    verify_warehouse_ledger,
)


def _sample_counts(mult: int = 1) -> dict[str, int]:
    return {name: (idx + 1) * mult for idx, name in enumerate(INSERT_ORDER)}


def _sample_monetary() -> dict[str, dict[str, dict[str, str]]]:
    return {
        "fact_orders": {
            "walmart|USD": {
                "gross_amount": "10.00",
                "discount_amount": "0.00",
                "net_amount": "10.00",
            }
        },
        "fact_order_items": {
            "walmart|USD": {"gross_line_amount": "10.00", "net_line_amount": "10.00"}
        },
        "fact_returns": {"walmart|USD": {"refund_amount": "0.00"}},
    }


def _sample_ledger(
    run_id: str = "run_a", pub_id: str = "gold_1", run_date: str = "2026-01-17"
) -> dict:
    counts = _sample_counts()
    return {
        "status": "completed",
        "runs": {
            run_id: {
                "gold_publication_id": pub_id,
                "run_date": run_date,
                "window_start": "2025-12-19T00:00:00+00:00",
                "window_end": "2026-01-18T00:00:00+00:00",
                "mode": "full",
                "tables": {f"warehouse.{k}": {"rows": v} for k, v in counts.items()},
                "monetary": _sample_monetary(),
                "updated_at": 1700000000.0,
            }
        },
    }


def test_skip_condition_detects_publication_and_run_date_changes():
    ledger = _sample_ledger("run_a", "gold_1", "2026-01-17")
    assert should_skip_export(ledger, "run_a", "gold_1", "2026-01-17") is True
    assert should_skip_export(ledger, "run_a", "gold_1", "2026-01-17", force=True) is False
    assert should_skip_export(ledger, "run_a", "gold_1", "2026-01-17", fail_after="facts") is False
    assert should_skip_export(ledger, "run_a", "gold_2", "2026-01-17") is False
    assert should_skip_export(ledger, "run_a", "gold_1", "2026-02-01") is False
    assert should_skip_export(ledger, "run_b", "gold_1", "2026-01-17") is False


def test_load_and_verify_ledger_guards(tmp_path: Path):
    ledger = _sample_ledger()
    export_id = compute_export_id(ledger)
    ledger_path = tmp_path / "ledger.json"
    archive_path = tmp_path / f"export-{export_id}.json"

    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    archive_path.write_text(json.dumps(ledger), encoding="utf-8")

    loaded, loaded_id = load_warehouse_ledger(ledger_path, export_id=export_id)
    assert loaded_id == export_id
    assert loaded["status"] == "completed"

    # Tampered archive fails
    archive_path.write_text(json.dumps({"tampered": True}), encoding="utf-8")
    with pytest.raises(ValueError, match="Corrupted warehouse export archive"):
        load_warehouse_ledger(ledger_path)

    # Missing run fails verify_warehouse_ledger
    with pytest.raises(ValueError, match="not present"):
        verify_warehouse_ledger(SimpleNamespace(), ledger, target_run_id="missing")


def test_publish_warehouse_writes_ledger_skips_replay_and_preserves_ledger_on_fault(
    monkeypatch, tmp_path: Path
):
    import spark.lib.warehouse_content as wc
    import spark.lib.warehouse_publish as wp

    monkeypatch.setattr(wc, "read_db_ledger", lambda conn: None)
    monkeypatch.setattr(wp, "writer_connection", lambda *a, **k: MagicMock())

    counts = _sample_counts()
    monetary = _sample_monetary()
    from spark.lib.warehouse_jdbc import TABLE_COLUMNS

    payload = {
        name: [tuple(None for _ in TABLE_COLUMNS[name])] * counts[name] for name in INSERT_ORDER
    }

    replace_calls = []

    def fake_replace(_spark, _payload, run_id, *, fail_after=None, **_kwargs):
        replace_calls.append((run_id, fail_after))
        if fail_after is not None:
            raise WarehouseFaultInjectionError(f"Injected {fail_after}")
        return counts

    monkeypatch.setattr(wp, "collect_slice_payload", lambda _frames: (payload, monetary))
    monkeypatch.setattr(wp, "replace_run_slice", fake_replace)
    monkeypatch.setattr(wp, "verify_warehouse_run", lambda *_args, **_kwargs: None)

    spark = MagicMock()
    w_start = datetime(2025, 12, 19, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 1, 18, 0, 0, tzinfo=UTC)

    ledger, skipped = publish_warehouse(
        spark,
        {},
        "run_a",
        "gold_1",
        date(2026, 1, 17),
        w_start,
        w_end,
        root=tmp_path,
    )
    assert skipped is False
    assert len(replace_calls) == 1
    assert (tmp_path / "ledger.json").exists()
    original_bytes = (tmp_path / "ledger.json").read_bytes()
    export_id = compute_export_id(ledger)
    assert (tmp_path / f"export-{export_id}.json").exists()

    # Identical replay must skip rewrite completely
    _ledger2, skipped2 = publish_warehouse(
        spark,
        {},
        "run_a",
        "gold_1",
        date(2026, 1, 17),
        w_start,
        w_end,
        root=tmp_path,
    )
    assert skipped2 is True
    assert len(replace_calls) == 1

    # Fault injection on new run_date preserves ledger.json byte-for-byte
    with pytest.raises(WarehouseFaultInjectionError, match="Injected facts"):
        publish_warehouse(
            spark,
            {},
            "run_a",
            "gold_1",
            date(2026, 2, 1),
            w_start,
            w_end,
            root=tmp_path,
            fail_after="facts",
        )
    assert (tmp_path / "ledger.json").read_bytes() == original_bytes
    attempt = json.loads((tmp_path / "attempt.json").read_text(encoding="utf-8"))
    assert attempt["status"] == "failed"
    assert "WarehouseFaultInjectionError" in attempt["error"]


def test_writer_lock_blocks_concurrent_writer(tmp_path: Path):
    from spark.lib.bronze import writer_lock

    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another Bronze writer/verifier is active"):
            with writer_lock(tmp_path):
                pass
