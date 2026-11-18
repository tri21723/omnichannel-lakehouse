from unittest.mock import Mock

import pytest

from spark.lib.bronze_jdbc import rollback_close


def test_close_even_when_rollback_loses_connection():
    conn = Mock()
    conn.isClosed.return_value = False
    conn.rollback.side_effect = ConnectionError("connection lost")
    with pytest.raises(ConnectionError):
        rollback_close(conn)
    conn.close.assert_called_once()


def test_closed_connection_does_not_mask_original_failure():
    conn = Mock()
    conn.isClosed.return_value = True
    rollback_close(conn)
    conn.rollback.assert_not_called()
    conn.close.assert_called_once()


def test_jdbc_incremental_skips_only_identical_verified_evidence():
    from spark.lib.bronze import jdbc_source_unchanged

    tables = {
        "orders": {"rows": 20, "sha256_stream": "a" * 64},
        "returns": {"rows": 2, "sha256_stream": "b" * 64},
    }

    def evidence(tables, fingerprint):
        return {
            "isolation": "repeatable read",
            "read_only": True,
            "snapshot_fingerprint": fingerprint,
            "tables": tables,
        }

    completed = {"status": "completed", "source_evidence": evidence(dict(tables), "f1")}
    # A new read has a different ephemeral snapshot fingerprint but identical content.
    assert jdbc_source_unchanged(True, completed, evidence(dict(tables), "f2"))
    # Target no longer verifies, previous evidence missing, or nothing read: re-ingest.
    assert not jdbc_source_unchanged(False, completed, evidence(dict(tables), "f2"))
    assert not jdbc_source_unchanged(True, {"status": "completed"}, evidence(dict(tables), "f2"))
    assert not jdbc_source_unchanged(True, {}, evidence(dict(tables), "f2"))
    assert not jdbc_source_unchanged(True, completed, {})
    assert not jdbc_source_unchanged(True, completed, evidence({}, "f2"))
    # Any content difference (rows or digest, e.g. a changed row or order) re-ingests.
    changed_rows = {**tables, "returns": {"rows": 3, "sha256_stream": "b" * 64}}
    changed_digest = {**tables, "orders": {"rows": 20, "sha256_stream": "c" * 64}}
    missing_table = {"orders": tables["orders"]}
    for other in (changed_rows, changed_digest, missing_table):
        assert not jdbc_source_unchanged(True, completed, evidence(other, "f2"))
