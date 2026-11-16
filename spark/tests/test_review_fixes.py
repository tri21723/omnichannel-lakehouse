"""Regression tests for full-row evidence and post-commit export recovery."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark.lib.warehouse_content import fingerprint, payload_content
from spark.lib.warehouse_jdbc import INSERT_ORDER


def test_fingerprint_detects_nonmonetary_change_and_value_swap():
    spec = [("id", "str"), ("status", "str"), ("amount", "decimal")]
    original = [("a", "paid", "1.00"), ("b", "pending", "2.00")]
    assert fingerprint(original, spec) == fingerprint(list(reversed(original)), spec)
    assert fingerprint(original, spec) != fingerprint([("a", "pending", "1.00"), original[1]], spec)
    assert fingerprint(original, spec) != fingerprint(
        [("a", "paid", "2.00"), ("b", "pending", "1.00")], spec
    )
    assert fingerprint(original, spec) != fingerprint(original + [original[0]], spec)


@pytest.mark.parametrize("mirror", [None, "{broken"])
def test_postcommit_crash_recovers_from_db_without_rewrite(monkeypatch, tmp_path, mirror):
    import spark.lib.warehouse_content as wc
    import spark.lib.warehouse_publish as wp

    state = {}
    calls = []
    payload = {name: [] for name in INSERT_ORDER}
    monkeypatch.setattr(wp, "writer_connection", lambda *a, **k: MagicMock())
    monkeypatch.setattr(wp, "collect_slice_payload", lambda f: (payload, {}))
    monkeypatch.setattr(wp, "verify_warehouse_run", lambda *a, **k: None)
    monkeypatch.setattr(wc, "read_db_ledger", lambda c: state.get("ledger"))
    monkeypatch.setattr(wc, "query_content", lambda c, r: payload_content(payload))
    monkeypatch.setattr(wp, "query_run_monetary_totals", lambda c, r: {})
    monkeypatch.setattr(wc, "write_db_ledger", lambda c, ledger: state.update(ledger=ledger))

    def replace(spark, payload, run, **kwargs):
        calls.append(run)
        counts = {name: 0 for name in INSERT_ORDER}
        kwargs["before_commit"](None, counts)
        return counts

    monkeypatch.setattr(wp, "replace_run_slice", replace)
    args = (
        SimpleNamespace(),
        {},
        "run",
        "pin",
        date(2026, 1, 1),
        date(2025, 12, 3),
        date(2026, 1, 2),
    )
    with pytest.raises(RuntimeError, match="after PostgreSQL commit"):
        wp.publish_warehouse(*args, root=tmp_path, fail_after="db_commit")
    assert not (tmp_path / "ledger.json").exists()
    if mirror is not None:
        (tmp_path / "ledger.json").write_text(mirror)
    authoritative, _ = wp.load_warehouse_ledger(tmp_path / "ledger.json", spark=args[0])
    assert authoritative == state["ledger"]
    ledger, skipped = wp.publish_warehouse(*args, root=tmp_path)
    assert skipped and calls == ["run"]
    assert (tmp_path / "ledger.json").exists()
    assert ledger == authoritative


def test_metadata_failure_rolls_back_before_data_commit(monkeypatch):
    import spark.lib.warehouse_jdbc as wj

    conn = MagicMock()
    conn.isClosed.return_value = False
    monkeypatch.setattr(wj, "writer_connection", lambda *a, **k: conn)
    monkeypatch.setattr(wj, "execute_ddl", lambda *a, **k: None)
    monkeypatch.setattr(wj, "delete_run_slice", lambda *a: None)
    monkeypatch.setattr(wj, "insert_table_rows", lambda *a, **k: None)
    monkeypatch.setattr(wj, "count_run_rows", lambda *a: {name: 0 for name in INSERT_ORDER})

    def fail_metadata(connection, counts):
        connection.commit.assert_not_called()
        raise OSError("metadata unavailable")

    with pytest.raises(OSError, match="metadata unavailable"):
        wj.replace_run_slice(
            MagicMock(), {name: [] for name in INSERT_ORDER}, "fixture", before_commit=fail_metadata
        )
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_old_label_dates_are_recomputed_independently():
    from spark.lib.dp3_entities import LABEL_POLICY
    from spark.lib.dp3_publish import labels_are_current

    assert not labels_are_current({}, ["2026-01-01"])
    prior = {"label_policy_dates": {"2026-01-01": LABEL_POLICY}}
    assert labels_are_current(prior, ["2026-01-01"])
    assert not labels_are_current(prior, ["2026-01-01", "2026-01-02"])
