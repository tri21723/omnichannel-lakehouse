"""Verifier logic on synthetic Flink output built from the ground truth, then corrupted."""

import copy

import pytest

from generators.stream.config import WATERMARK_BOUND_MS, StreamConfig
from generators.stream.events import generate
from generators.stream.verify import check_channel

CONFIG = StreamConfig(events_per_channel=1500, events_per_minute=30, late_max_minutes=32)


@pytest.fixture(scope="module")
def perfect():
    sends, truth = generate(CONFIG)
    channel = "amazon"
    accepted, late = [], []
    for s in sends:
        if s.channel != channel or s.kind != "event":
            continue
        row = {
            **s.value,
            "_delay_class": s.delay_class,
            "_watermark_ms": s.send_ms - WATERMARK_BOUND_MS,
        }
        (late if s.delay_class == "too_late" else accepted).append(row)
    windows = [{**w, "firing_seq": 1} for w in truth["channels"][channel]["windows"]]
    stats = {"files": 1, "hidden_files": 0, "rows": 0}
    branches = {
        "accepted": (accepted, stats),
        "late": (late, stats),
        "duplicates": ([], stats),
        "windows": (windows, stats),
    }
    ids = {s.event_id for s in sends if s.kind == "event" and s.channel == channel}
    return truth["channels"][channel], branches, ids


def failed(checks):
    return sorted(c["check"] for c in checks if not c["ok"])


def run(perfect, failover, mutate=None):
    truth, branches, ids = perfect
    branches = copy.deepcopy(branches)
    if mutate:
        mutate(branches)
    return failed(check_channel("amazon", truth, branches, truth["malformed"], ids, failover))


def test_perfect_output_passes_both_modes(perfect):
    assert run(perfect, failover=False) == []
    assert run(perfect, failover=True) == []


def test_reclassification_after_restore_fails_strict_but_not_failover(perfect):
    def reclassify(branches):
        row = next(r for r in branches["accepted"][0] if r["_delay_class"] == "late_accepted")
        row["_delay_class"] = "on_time"
        row["_watermark_ms"] = -(2**63)  # watermark restarted from MIN after a restore

    assert run(perfect, False, reclassify) == ["late_accepted_ids", "on_time_ids"]
    assert run(perfect, True, reclassify) == []


def test_lost_record_fails_both_modes(perfect):
    def lose(branches):
        branches["late"][0].pop()

    assert "nothing_lost" in run(perfect, False, lose)
    assert "event_ids_complete" in run(perfect, True, lose)


def test_duplicated_output_fails_both_modes(perfect):
    def duplicate(branches):
        branches["accepted"][0].append(dict(branches["accepted"][0][0]))

    for failover in (False, True):
        assert "accepted_unique" in run(perfect, failover, duplicate)


def test_window_inconsistent_with_records_fails_failover(perfect):
    def skew(branches):
        branches["windows"][0][0]["events"] += 1

    assert "windows_match_accepted" in run(perfect, True, skew)
    assert "windows_final" in run(perfect, False, skew)
