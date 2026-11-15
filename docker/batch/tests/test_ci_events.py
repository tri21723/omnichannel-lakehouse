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
