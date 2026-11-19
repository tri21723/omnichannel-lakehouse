import importlib.util
import sys
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("orch_e2e", Path("docker/orch/e2e.py").resolve())
e2e = importlib.util.module_from_spec(SPEC)
sys.modules["orch_e2e"] = e2e
SPEC.loader.exec_module(e2e)

DP3 = "dp3/publication/ledger.json"
GOLD = "gold/publication/ledger.json"


def test_first_pass_accepts_any_change():
    assert e2e.unexpected_changes([DP3, GOLD], expect_unchanged=False) == []


def test_converging_replay_allows_only_dp3():
    assert e2e.unexpected_changes([DP3], True, [DP3]) == []
    assert e2e.unexpected_changes([DP3, GOLD], True, [DP3]) == [GOLD]


def test_final_replay_allows_nothing():
    assert e2e.unexpected_changes([DP3], True) == [DP3]
    assert e2e.unexpected_changes([], True) == []


def test_allow_changed_rejects_unknown_ledgers():
    parser_error = pytest.raises(SystemExit)
    with parser_error:
        e2e.main(["--run-id", "5f552019ee41c24af95f", "--allow-changed", "secrets.json"])
