"""Unit tests for the dry-run Iceberg retention report (ADR-0014)."""

import json
from datetime import datetime, timedelta

import pytest

from spark.lib.iceberg_maintenance import (
    load_runtime_documents,
    pinned_snapshots,
    select_candidates,
    table_summary,
)

AS_OF = datetime(2026, 11, 18, 12, 0)


def snaps(*ages_days):
    return [
        {"snapshot_id": 100 + i, "committed_at": AS_OF - timedelta(days=age)}
        for i, age in enumerate(ages_days)
    ]


def test_pinned_snapshots_found_in_ledgers_archives_and_manifests():
    documents = [
        {"tables": {"lh.gold.fact_orders": {"snapshot": 11, "content": {"rows": "5"}}}},
        {"runs": {"r": {"gold_snapshots": {"fact_order_items": 12, "dim_product": 13}}}},
        {"tables": {"items": {"snapshot": 14, "table": "lh.benchmark.items_x"}}},
        {"source_evidence": {"snapshot_fingerprint": "abc", "tables": {"orders": {"rows": 20}}}},
        {"_silver_snapshot": 15, "rows": 99, "flag": True},
        [{"snapshot_id": 16}],
    ]
    assert pinned_snapshots(documents) == {11, 12, 13, 14, 15, 16}


def test_runtime_documents_report_unreadable_files(tmp_path):
    (tmp_path / "gold").mkdir()
    (tmp_path / "gold" / "ledger.json").write_text(json.dumps({"tables": {"t": {"snapshot": 1}}}))
    (tmp_path / "broken.json").write_text("{not json")
    documents, sources, unreadable = load_runtime_documents(tmp_path)
    assert pinned_snapshots(documents) == {1}
    assert sources == ["gold/ledger.json"] and unreadable == ["broken.json"]


def test_candidates_exclude_pinned_current_recent_and_newest():
    history = snaps(30, 20, 10, 9, 8, 3, 2, 1)  # ids 100..107, oldest first
    candidates = select_candidates(
        history, pinned={101}, current={107}, as_of=AS_OF, retention_days=7, keep_last=3
    )
    # 105-107 are newest (keep_last=3) and younger than 7 days; 101 pinned.
    assert candidates == [100, 102, 103, 104]
    assert select_candidates(history, set(), set(), AS_OF, retention_days=40, keep_last=1) == []
    assert select_candidates(history[:2], set(), set(), AS_OF, 7, keep_last=5) == []


def test_candidates_reject_unsafe_settings():
    with pytest.raises(ValueError):
        select_candidates(snaps(30), set(), set(), AS_OF, retention_days=0, keep_last=1)
    with pytest.raises(ValueError):
        select_candidates(snaps(30), set(), set(), AS_OF, retention_days=7, keep_last=0)


def test_table_summary_counts():
    summary = table_summary(
        "lh.gold.t",
        snaps(30, 20, 1),
        {100},
        {102},
        {"count": 4, "bytes": 10, "small": 3},
        AS_OF,
        7,
        1,
    )
    assert summary["snapshots"] == 3 and summary["pinned_snapshots"] == 1
    assert summary["current_refs"] == 1 and summary["candidate_ids"] == [101]
    assert summary["small_files"] == 3
