"""Summarize Spark event logs into allowlisted benchmark metrics (ADR-0012 §8).

Pure Python: no pyspark import, so it runs in unit tests and in report tooling.
Rules: only successful tasks from completed stage attempts count as work; failed,
killed and speculative-loser attempts are overhead; absent metrics are ``None``.
"""

from __future__ import annotations

import json
import math
import statistics
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
COMPRESSED_SUFFIXES = (".lz4", ".lzf", ".snappy", ".zstd", ".zst")

# (output name, path inside "Task Metrics")
TASK_METRICS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("executor_run_time_ms", ("Executor Run Time",)),
    ("jvm_gc_time_ms", ("JVM GC Time",)),
    ("input_bytes", ("Input Metrics", "Bytes Read")),
    ("input_records", ("Input Metrics", "Records Read")),
    ("shuffle_remote_bytes_read", ("Shuffle Read Metrics", "Remote Bytes Read")),
    ("shuffle_local_bytes_read", ("Shuffle Read Metrics", "Local Bytes Read")),
    ("shuffle_records_read", ("Shuffle Read Metrics", "Total Records Read")),
    ("shuffle_bytes_written", ("Shuffle Write Metrics", "Shuffle Bytes Written")),
    ("shuffle_records_written", ("Shuffle Write Metrics", "Shuffle Records Written")),
    ("memory_bytes_spilled", ("Memory Bytes Spilled",)),
    ("disk_bytes_spilled", ("Disk Bytes Spilled",)),
)


@dataclass
class _Task:
    index: int
    attempt: int
    reason: str
    speculative: bool
    duration_ms: int | None
    metrics: dict[str, int | None]


@dataclass
class _StageAttempt:
    stage_id: int
    attempt: int
    name: str | None = None
    num_tasks: int | None = None
    submitted_ms: int | None = None
    completed_ms: int | None = None
    failed: bool = False
    seen_completed: bool = False
    tasks: list[_Task] = field(default_factory=list)


def _get(obj: Any, path: tuple[str, ...]) -> Any:
    for key in path:
        if not isinstance(obj, dict) or key not in obj:
            return None
        obj = obj[key]
    return obj


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return int(value)


def _percentile(sorted_values: list[int], pct: float) -> int:
    """Nearest-rank percentile on a non-empty ascending list."""
    rank = max(1, math.ceil(pct / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def _sum_or_none(tasks: list[_Task], name: str) -> int | None:
    values = [task.metrics.get(name) for task in tasks]
    if not values or any(value is None for value in values):
        return None
    return sum(values)  # type: ignore[arg-type]


def _read_lines(lines: Iterable[str]) -> tuple[list[dict], int, bool]:
    """Decode JSON lines; a bad final line is a truncated tail, others are malformed."""
    raw = [line for line in lines if line.strip()]
    events: list[dict] = []
    malformed = 0
    truncated = False
    for position, line in enumerate(raw):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            if position == len(raw) - 1:
                truncated = True
            else:
                malformed += 1
            continue
        if isinstance(event, dict):
            events.append(event)
        else:
            malformed += 1
    return events, malformed, truncated


def _stage_summary(stage: _StageAttempt) -> dict:
    if stage.failed:
        status = "failed"
    elif stage.seen_completed:
        status = "completed"
    else:
        status = "incomplete"

    winners: dict[int, _Task] = {}
    overhead = {"failed": 0, "killed": 0, "duplicate_success": 0}
    for task in stage.tasks:
        if task.reason == "Success":
            if task.index in winners:
                overhead["duplicate_success"] += 1
            else:
                winners[task.index] = task
        elif task.reason == "TaskKilled":
            overhead["killed"] += 1
        else:
            overhead["failed"] += 1

    successful = list(winners.values()) if status == "completed" else []
    durations = sorted(t.duration_ms for t in successful if t.duration_ms is not None)
    duration_stats: dict[str, int | float | None] = {"median": None, "p95": None, "max": None}
    ratio: float | None = None
    ratio_reason: str | None = None
    if status != "completed":
        ratio_reason = f"stage_{status}"
    elif not successful:
        ratio_reason = "no_successful_tasks"
    elif len(durations) != len(successful):
        ratio_reason = "task_duration_unavailable"
    else:
        median = statistics.median(durations)
        duration_stats = {"median": median, "p95": _percentile(durations, 95), "max": durations[-1]}
        if median <= 0:
            ratio_reason = "median_zero"
        else:
            ratio = round(durations[-1] / median, 3)

    duration_ms = None
    if stage.submitted_ms is not None and stage.completed_ms is not None:
        duration_ms = stage.completed_ms - stage.submitted_ms

    summary = {
        "stage_id": stage.stage_id,
        "attempt": stage.attempt,
        "name": stage.name,
        "status": status,
        "num_tasks": stage.num_tasks,
        "duration_ms": duration_ms,
        "successful_tasks": len(successful),
        "failed_tasks": overhead["failed"],
        "killed_tasks": overhead["killed"],
        "duplicate_success_tasks": overhead["duplicate_success"],
        "speculative_tasks": sum(1 for t in stage.tasks if t.speculative),
        "task_duration_ms": duration_stats,
        "max_median_ratio": ratio,
        "ratio_unavailable_reason": ratio_reason,
    }
    for name, _ in TASK_METRICS:
        summary[name] = _sum_or_none(successful, name) if successful else None
    return summary


def summarize_events(lines: Iterable[str]) -> dict:
    """Return an allowlisted, JSON-serializable summary of one Spark event log."""
    events, malformed, truncated = _read_lines(lines)
    stages: dict[tuple[int, int], _StageAttempt] = {}
    app_start = app_end = None

    def stage_for(stage_id: Any, attempt: Any) -> _StageAttempt | None:
        sid, att = _int_or_none(stage_id), _int_or_none(attempt)
        if sid is None or att is None:
            return None
        return stages.setdefault((sid, att), _StageAttempt(sid, att))

    for event in events:
        kind = event.get("Event")
        if kind == "SparkListenerApplicationStart":
            app_start = _int_or_none(event.get("Timestamp"))
        elif kind == "SparkListenerApplicationEnd":
            app_end = _int_or_none(event.get("Timestamp"))
        elif kind in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            info = event.get("Stage Info") or {}
            stage = stage_for(info.get("Stage ID"), info.get("Stage Attempt ID"))
            if stage is None:
                malformed += 1
                continue
            stage.name = info.get("Stage Name") if isinstance(info.get("Stage Name"), str) else None
            stage.num_tasks = _int_or_none(info.get("Number of Tasks"))
            submitted = _int_or_none(info.get("Submission Time"))
            if submitted is not None:
                stage.submitted_ms = submitted
            if kind == "SparkListenerStageCompleted":
                stage.seen_completed = True
                stage.completed_ms = _int_or_none(info.get("Completion Time"))
                stage.failed = "Failure Reason" in info
        elif kind == "SparkListenerTaskEnd":
            stage = stage_for(event.get("Stage ID"), event.get("Stage Attempt ID"))
            info = event.get("Task Info") or {}
            index = _int_or_none(info.get("Index"))
            if stage is None or index is None:
                malformed += 1
                continue
            launch = _int_or_none(info.get("Launch Time"))
            finish = _int_or_none(info.get("Finish Time"))
            metrics_obj = event.get("Task Metrics")
            metrics = {
                name: _int_or_none(_get(metrics_obj, path)) if metrics_obj else None
                for name, path in TASK_METRICS
            }
            reason = _get(event, ("Task End Reason", "Reason"))
            stage.tasks.append(
                _Task(
                    index=index,
                    attempt=_int_or_none(info.get("Attempt")) or 0,
                    reason=reason if isinstance(reason, str) else "Unknown",
                    speculative=info.get("Speculative") is True,
                    duration_ms=(
                        finish - launch if launch is not None and finish is not None else None
                    ),
                    metrics=metrics,
                )
            )

    stage_summaries = [_stage_summary(stages[key]) for key in sorted(stages)]
    completed = [s for s in stage_summaries if s["status"] == "completed"]
    totals: dict[str, Any] = {
        "completed_stage_attempts": len(completed),
        "failed_stage_attempts": sum(1 for s in stage_summaries if s["status"] == "failed"),
        "incomplete_stage_attempts": sum(1 for s in stage_summaries if s["status"] == "incomplete"),
        "successful_tasks": sum(s["successful_tasks"] for s in completed),
        "failed_tasks": sum(s["failed_tasks"] for s in stage_summaries),
        "killed_tasks": sum(s["killed_tasks"] for s in stage_summaries),
        "speculative_tasks": sum(s["speculative_tasks"] for s in stage_summaries),
    }
    for name, _ in TASK_METRICS:
        values = [s[name] for s in completed if s["successful_tasks"]]
        totals[name] = None if not values or None in values else sum(values)
    ratios = [s["max_median_ratio"] for s in completed if s["max_median_ratio"] is not None]
    totals["worst_max_median_ratio"] = max(ratios) if ratios else None

    complete = (
        malformed == 0
        and not truncated
        and app_end is not None
        and totals["incomplete_stage_attempts"] == 0
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "complete": complete,
        "events_read": len(events),
        "malformed_lines": malformed,
        "truncated_tail": truncated,
        "application": {
            "start_ms": app_start,
            "end_ms": app_end,
            "duration_ms": (
                app_end - app_start if app_start is not None and app_end is not None else None
            ),
        },
        "stages": stage_summaries,
        "totals": totals,
    }


def summarize_event_log(path: str | Path) -> dict:
    """Summarize an uncompressed event log file (ADR-0012 requires compress=false)."""
    path = Path(path)
    if path.suffix.lower() in COMPRESSED_SUFFIXES:
        raise ValueError(f"compressed event log not supported: {path.name}")
    with path.open(encoding="utf-8", errors="replace") as handle:
        return summarize_events(handle)
