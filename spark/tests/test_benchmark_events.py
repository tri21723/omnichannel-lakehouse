"""Unit tests for Spark event-log metric summaries (ADR-0012 §8)."""

import json

import pytest

from spark.lib.benchmark_events import summarize_event_log, summarize_events


def app(start=1_000, end=9_000):
    return [
        {"Event": "SparkListenerApplicationStart", "Timestamp": start},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": end},
    ]


def stage(stage_id, attempt=0, *, submitted=1_000, completed=2_000, failure=None, tasks=2):
    info = {
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt,
        "Stage Name": f"stage {stage_id}",
        "Number of Tasks": tasks,
        "Submission Time": submitted,
        "Completion Time": completed,
    }
    if failure:
        info["Failure Reason"] = failure
    return {"Event": "SparkListenerStageCompleted", "Stage Info": info}


def task(stage_id, index, duration, *, attempt=0, stage_attempt=0, reason="Success", **extra):
    metrics = {
        "Executor Run Time": duration,
        "JVM GC Time": 0,
        "Input Metrics": {"Bytes Read": 10, "Records Read": 1},
        "Shuffle Read Metrics": {
            "Remote Bytes Read": 100,
            "Local Bytes Read": 50,
            "Total Records Read": 5,
        },
        "Shuffle Write Metrics": {"Shuffle Bytes Written": 70, "Shuffle Records Written": 7},
        "Memory Bytes Spilled": 0,
        "Disk Bytes Spilled": 0,
    }
    metrics.update(extra.pop("metrics", {}))
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": stage_attempt,
        "Task End Reason": {"Reason": reason},
        "Task Info": {
            "Index": index,
            "Attempt": attempt,
            "Launch Time": 1_000,
            "Finish Time": 1_000 + duration,
            "Speculative": extra.pop("speculative", False),
        },
        "Task Metrics": metrics,
    }


def lines(events):
    return [json.dumps(event) for event in events]


def test_skewed_stage_reports_duration_ratio_and_shuffle_totals():
    events = app() + [
        task(0, 0, 100),
        task(0, 1, 100),
        task(0, 2, 900),
        stage(0, tasks=3),
    ]
    summary = summarize_events(lines(events))

    assert summary["complete"] is True
    assert summary["application"]["duration_ms"] == 8_000
    (stage0,) = summary["stages"]
    assert stage0["task_duration_ms"] == {"median": 100, "p95": 900, "max": 900}
    assert stage0["max_median_ratio"] == 9.0
    assert stage0["shuffle_remote_bytes_read"] == 300
    assert stage0["shuffle_bytes_written"] == 210
    assert summary["totals"]["worst_max_median_ratio"] == 9.0
    assert summary["totals"]["worst_shuffle_stage_ratio"] == 9.0
    assert summary["totals"]["max_shuffle_task_ms"] == 900
    assert summary["totals"]["successful_tasks"] == 3


def test_retry_and_speculation_count_as_overhead_not_work():
    events = app() + [
        task(0, 0, 100, reason="ExceptionFailure"),
        task(0, 0, 120, attempt=1),
        task(0, 1, 100),
        task(0, 1, 300, attempt=1, reason="TaskKilled", speculative=True),
        stage(0),
    ]
    stage0 = summarize_events(lines(events))["stages"][0]

    assert stage0["successful_tasks"] == 2
    assert stage0["failed_tasks"] == 1
    assert stage0["killed_tasks"] == 1
    assert stage0["speculative_tasks"] == 1
    assert stage0["executor_run_time_ms"] == 220
    assert stage0["task_duration_ms"]["max"] == 120


def test_failed_stage_attempt_is_excluded_from_successful_work():
    events = app() + [
        task(1, 0, 500),
        stage(1, 0, failure="FetchFailed"),
        task(1, 0, 200, stage_attempt=1),
        stage(1, 1, tasks=1),
    ]
    summary = summarize_events(lines(events))
    failed, retried = summary["stages"]

    assert failed["status"] == "failed"
    assert failed["successful_tasks"] == 0
    assert failed["executor_run_time_ms"] is None
    assert failed["ratio_unavailable_reason"] == "stage_failed"
    assert retried["executor_run_time_ms"] == 200
    assert summary["totals"]["failed_stage_attempts"] == 1
    assert summary["totals"]["executor_run_time_ms"] == 200


def test_duplicate_success_keeps_first_attempt_only():
    events = app() + [task(0, 0, 100), task(0, 0, 400, attempt=1), stage(0, tasks=1)]
    stage0 = summarize_events(lines(events))["stages"][0]

    assert stage0["successful_tasks"] == 1
    assert stage0["duplicate_success_tasks"] == 1
    assert stage0["executor_run_time_ms"] == 100


def test_empty_stage_and_zero_median_have_no_ratio():
    events = app() + [stage(0, tasks=0), task(1, 0, 0), task(1, 1, 0), stage(1)]
    empty, zero = summarize_events(lines(events))["stages"]

    assert empty["max_median_ratio"] is None
    assert empty["ratio_unavailable_reason"] == "no_successful_tasks"
    assert empty["shuffle_bytes_written"] is None
    assert zero["max_median_ratio"] is None
    assert zero["ratio_unavailable_reason"] == "median_zero"


def test_missing_metric_is_unavailable_not_zero():
    no_spill = task(0, 1, 100)
    del no_spill["Task Metrics"]["Disk Bytes Spilled"]
    no_metrics = task(1, 0, 100)
    del no_metrics["Task Metrics"]
    events = app() + [task(0, 0, 100), no_spill, stage(0), no_metrics, stage(1, tasks=1)]
    summary = summarize_events(lines(events))

    assert summary["stages"][0]["disk_bytes_spilled"] is None
    assert summary["stages"][0]["memory_bytes_spilled"] == 0
    assert summary["stages"][1]["shuffle_bytes_written"] is None
    assert summary["totals"]["disk_bytes_spilled"] is None


def test_truncated_tail_and_malformed_lines_mark_incomplete():
    good = lines(app() + [task(0, 0, 100), stage(0, tasks=1)])
    truncated = summarize_events(good + ['{"Event": "SparkListenerTask'])
    malformed = summarize_events(good[:1] + ["not json"] + good[1:])

    assert truncated["truncated_tail"] is True
    assert truncated["malformed_lines"] == 0
    assert truncated["complete"] is False
    assert truncated["stages"][0]["successful_tasks"] == 1
    assert malformed["malformed_lines"] == 1
    assert malformed["truncated_tail"] is False
    assert malformed["complete"] is False


def test_missing_application_end_or_stage_completion_is_incomplete():
    no_end = summarize_events(lines(app()[:1] + [task(0, 0, 10), stage(0, tasks=1)]))
    running = summarize_events(lines(app() + [task(0, 0, 10)]))

    assert no_end["complete"] is False
    assert no_end["application"]["duration_ms"] is None
    assert running["complete"] is False
    assert running["stages"][0]["status"] == "incomplete"
    assert running["stages"][0]["successful_tasks"] == 0


def test_unknown_events_ignored_and_summary_is_json_serializable():
    events = app() + [{"Event": "SparkListenerExecutorAdded", "Executor ID": "1"}]
    summary = summarize_events(lines(events))

    assert summary["stages"] == []
    assert summary["totals"]["executor_run_time_ms"] is None
    json.dumps(summary)


def test_file_reader_rejects_compressed_logs(tmp_path):
    plain = tmp_path / "app-1"
    plain.write_text("\n".join(lines(app())) + "\n", encoding="utf-8")
    assert summarize_event_log(plain)["complete"] is True

    with pytest.raises(ValueError, match="compressed"):
        summarize_event_log(tmp_path / "app-1.zstd")


def test_shuffle_stage_ratio_ignores_input_scan_stages():
    scan = [
        task(
            0,
            i,
            d,
            metrics={"Shuffle Read Metrics": {"Remote Bytes Read": 0, "Local Bytes Read": 0}},
        )
        for i, d in enumerate((100, 100, 5000))
    ]
    join = [task(1, i, d) for i, d in enumerate((100, 200, 400))]
    summary = summarize_events(lines(app() + scan + [stage(0, tasks=3)] + join + [stage(1)]))

    assert summary["totals"]["worst_max_median_ratio"] == 50.0
    assert summary["totals"]["worst_shuffle_stage_ratio"] == 2.0
    assert summary["totals"]["max_shuffle_task_ms"] == 400
