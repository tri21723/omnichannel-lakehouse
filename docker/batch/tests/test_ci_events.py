import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("ci_events", Path("docker/batch/ci_events.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_failure_output_does_not_copy_credentials_or_payload():
    secret = "password-do-not-copy"
    lines = [
        f"Traceback: {secret}",
        json.dumps(
            {
                "event": "bronze_failed",
                "detail": secret,
                "error_type": "ValueError",
                "tables": {"_raw": secret},
                "reason": secret,
            }
        ),
        json.dumps({"event": "bronze_completed", "metrics": {"rows": 4, "password": secret}}),
    ]
    result = list(module.safe_events(lines))
    assert secret not in json.dumps(result)
    assert result[0] == {"event": "bronze_failed", "error_type": "ValueError"}
    assert result[1]["metrics"] == {"rows": 4}


def test_untrusted_event_names_and_nonstring_reasons_are_not_copied():
    lines = [
        json.dumps({"event": "bronze_private_value"}),
        json.dumps(
            {
                "event": "bronze_failed",
                "reason": {"private": "value"},
                "error_type": "PrivateValueError",
            }
        ),
    ]
    assert list(module.safe_events(lines)) == [{"event": "bronze_failed"}]


def test_silver_nested_counts_cannot_copy_private_fields():
    event = {
        "event": "silver_published",
        "entities": {
            "customers": {"valid": 5, "email": "private@example.invalid", "rejected": 0},
            "private": "not-a-supported-entity",
        },
    }
    result = list(module.safe_events([json.dumps(event)]))
    assert result == [
        {"event": "silver_published", "entities": {"customers": {"valid": 5, "rejected": 0}}}
    ]


def test_dp3_events_preserve_safe_metrics_and_runs():
    event = {
        "event": "labels_published",
        "run_id": "186ae107d0793d2ee237",
        "horizon_days": 14,
        "skipped": False,
        "secret_token": "super-secret-do-not-leak",
    }
    result = list(module.safe_events([json.dumps(event)]))
    assert result == [
        {
            "event": "labels_published",
            "run_id": "186ae107d0793d2ee237",
            "horizon_days": 14,
            "skipped": False,
        }
    ]


def test_warehouse_events_preserve_safe_fields_only():
    event = {
        "event": "warehouse_exported",
        "run_id": "186ae107d0793d2ee237",
        "run_date": "2026-01-17",
        "export_id": "a" * 64,
        "skipped": True,
        "password": "do-not-leak-db-password",
    }
    result = list(module.safe_events([json.dumps(event)]))
    assert result == [
        {
            "event": "warehouse_exported",
            "run_id": "186ae107d0793d2ee237",
            "run_date": "2026-01-17",
            "export_id": "a" * 64,
            "skipped": True,
        }
    ]


def test_benchmark_events_keep_counts_but_drop_paths_and_details():
    lines = [
        json.dumps(
            {
                "event": "benchmark_progress",
                "done": 3,
                "total": 12,
                "status": "ok",
                "query_s": 9.5,
                "trial": "/runtime/private/path",
                "details": ["s3://secret"],
            }
        ),
        json.dumps(
            {
                "event": "benchmark_dataset_built",
                "dataset": "l1m_skew80",
                "rows": 1005000,
                "keys": 100,
                "per_channel_rows": {"amazon": 1},
            }
        ),
        json.dumps({"event": "benchmark_verified", "experiment": "../x", "problems": 0}),
        json.dumps({"event": "benchmark_trial_finished", "status": "leaked", "correct": True}),
    ]
    result = list(module.safe_events(lines))
    assert result == [
        {"event": "benchmark_progress", "done": 3, "total": 12, "status": "ok", "query_s": 9.5},
        {"event": "benchmark_dataset_built", "dataset": "l1m_skew80", "rows": 1005000, "keys": 100},
        {"event": "benchmark_verified", "problems": 0},
        {"event": "benchmark_trial_finished", "correct": True},
    ]


def test_e2e_summary_keeps_counts_only():
    line = json.dumps(
        {
            "event": "e2e_summary",
            "backfill_id": 3,
            "controller_runs": 3,
            "ledger_hashes": {"gold": "abc"},
            "problems": ["/runtime/private"],
        }
    )
    assert list(module.safe_events([line])) == [
        {"event": "e2e_summary", "backfill_id": 3, "controller_runs": 3}
    ]


def test_json_after_log_prefix_is_read_and_still_allowlisted():
    lines = [
        '2026-11-18 08:00:00,000 INFO {"event": "warehouse_exported", "skipped": true,'
        ' "password": "x"}',
        '2026-11-18 08:00:01,000 INFO {"event": "private_event"}',
        "2026-11-18 08:00:02,000 INFO not json {",
    ]
    assert list(module.safe_events(lines)) == [{"event": "warehouse_exported", "skipped": True}]
