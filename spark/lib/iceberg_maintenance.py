"""Dry-run Iceberg snapshot retention report (ADR-0014): pure helpers, no Spark import.

Nothing here expires snapshots or deletes files. Snapshots referenced by any ledger,
publication archive, completion record or benchmark manifest under ``/runtime`` are pinned
and can never be candidates.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

SMALL_FILE_BYTES = 8 * 1024 * 1024


def _collect(value: Any, key_hint: bool, out: set[int]) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, int) and key_hint:
        out.add(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            _collect(item, key_hint or "snapshot" in str(key).lower(), out)
    elif isinstance(value, list):
        for item in value:
            _collect(item, key_hint, out)


def pinned_snapshots(documents: Iterable[Any]) -> set[int]:
    """Integer values under any key containing 'snapshot' (at any depth) are pinned."""
    out: set[int] = set()
    for document in documents:
        _collect(document, False, out)
    return out


def load_runtime_documents(root: Path) -> tuple[list[Any], list[str], list[str]]:
    """All JSON documents under ``root``; unreadable files are reported, never ignored."""
    documents, sources, unreadable = [], [], []
    for path in sorted(root.rglob("*.json")):
        try:
            documents.append(json.loads(path.read_text(encoding="utf-8")))
            sources.append(str(path.relative_to(root)))
        except (OSError, ValueError):
            unreadable.append(str(path.relative_to(root)))
    return documents, sources, unreadable


def select_candidates(
    snapshots: list[dict],
    pinned: set[int],
    current: set[int],
    as_of: datetime,
    retention_days: int,
    keep_last: int,
) -> list[int]:
    """Snapshots that a future expiry *could* remove under ADR-0014 (report only).

    A snapshot is a candidate only if it is not pinned, not referenced by a branch/tag,
    older than ``retention_days`` before ``as_of`` and not among the newest ``keep_last``.
    """
    if retention_days < 1 or keep_last < 1:
        raise ValueError("retention_days and keep_last must be >= 1")
    cutoff = as_of - timedelta(days=retention_days)
    ordered = sorted(snapshots, key=lambda s: s["committed_at"], reverse=True)
    protected = {s["snapshot_id"] for s in ordered[:keep_last]} | pinned | current
    return sorted(
        s["snapshot_id"]
        for s in ordered
        if s["snapshot_id"] not in protected and s["committed_at"] < cutoff
    )


def table_summary(name, snapshots, pinned, current, files, as_of, retention_days, keep_last):
    ids = {s["snapshot_id"] for s in snapshots}
    candidates = select_candidates(snapshots, pinned, current, as_of, retention_days, keep_last)
    return {
        "table": name,
        "snapshots": len(snapshots),
        "pinned_snapshots": len(ids & pinned),
        "current_refs": len(ids & current),
        "expiry_candidates": len(candidates),
        "candidate_ids": candidates[:50],
        "data_files": files.get("count", 0),
        "data_bytes": files.get("bytes", 0),
        "small_files": files.get("small", 0),
    }
