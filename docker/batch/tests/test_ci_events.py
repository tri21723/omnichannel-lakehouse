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


def test_query_events_keep_counts_only():
    lines = [
        json.dumps(
            {
                "event": "query_verify_finished",
                "checks": 38,
                "ok": True,
                "gold_publication_id": "x" * 64,
                "details": ["s3://secret"],
            }
        ),
        json.dumps(
            {
                "event": "query_smoke_check",
                "check": "denied",
                "ok": False,
                "sql": "SELECT email FROM customers",
                "error": "NOT DENIED",
            }
        ),
        json.dumps({"event": "query_verify_check", "check": "s3://secret", "ok": True}),
    ]
    assert list(module.safe_events(lines)) == [
        {"event": "query_verify_finished", "checks": 38, "ok": True},
        {"event": "query_smoke_check", "check": "denied", "ok": False},
        {"event": "query_verify_check", "ok": True},
    ]


def test_query_sample_keeps_file_and_error_code_only():
    lines = [
        json.dumps(
            {
                "event": "query_sample",
                "run_id": "186ae107d0793d2ee237",
                "file": "07_snapshot_metadata.sql",
                "ok": False,
                "error_name": "INVALID_FUNCTION_ARGUMENT",
                "problems": ["Key not present in map: secret@example.com"],
            }
        ),
        json.dumps({"event": "query_sample", "file": "../etc/passwd", "error_name": "x y"}),
    ]
    assert list(module.safe_events(lines)) == [
        {
            "event": "query_sample",
            "run_id": "186ae107d0793d2ee237",
            "file": "07_snapshot_metadata.sql",
            "error_name": "INVALID_FUNCTION_ARGUMENT",
            "ok": False,
        },
        {"event": "query_sample"},
    ]


def test_stream_events_keep_fixed_names_and_counts_only():
    lines = [
        json.dumps(
            {
                "event": "stream_verify_check",
                "run_id": "e7a188e49b1e4042ef15",
                "channel": "amazon",
                "check": "nothing_lost",
                "ok": True,
                "accepted": 2940,
                "latency_processing_ms": {"p50": 7},
            }
        ),
        json.dumps({"event": "stream_job_started", "job": "amazon_late", "job_id": "x" * 32}),
        json.dumps({"event": "stream_verify_check", "channel": "s3://secret", "check": "evil"}),
    ]
    assert list(module.safe_events(lines)) == [
        {
            "event": "stream_verify_check",
            "run_id": "e7a188e49b1e4042ef15",
            "channel": "amazon",
            "check": "nothing_lost",
            "ok": True,
            "accepted": 2940,
        },
        {"event": "stream_job_started", "job": "amazon_late"},
        {"event": "stream_verify_check"},
    ]


def test_cdc_events_keep_counts_and_fixed_check_names_only():
    lines = [
        json.dumps(
            {"event": "cdc_bronze_completed", "inserted": 136, "records": 136, "seconds": 30.1}
        ),
        json.dumps(
            {
                "event": "cdc_verified",
                "ok": True,
                "exact_ops": True,
                "silver_live": 3205,
                "source_orders": 3205,
                "scenario_ops": {"c": 43},
            }
        ),
        json.dumps({"event": "cdc_verify_check", "check": "checksum_equal", "ok": True}),
        json.dumps({"event": "cdc_verify_check", "check": "SELECT secret", "ok": True}),
    ]
    assert list(module.safe_events(lines)) == [
        {"event": "cdc_bronze_completed", "inserted": 136, "records": 136},
        {
            "event": "cdc_verified",
            "ok": True,
            "exact_ops": True,
            "silver_live": 3205,
            "source_orders": 3205,
        },
        {"event": "cdc_verify_check", "check": "checksum_equal", "ok": True},
        {"event": "cdc_verify_check", "ok": True},
    ]


def test_governance_events_keep_check_names_counts_and_digests_only():
    lines = [
        json.dumps(
            {"event": "gov_smoke_check", "check": "probe_read", "ok": True, "version": "v1"}
        ),
        json.dumps({"event": "gov_smoke_check", "check": "password=hunter2", "ok": False}),
        json.dumps(
            {
                "event": "datahub_inventory",
                "datasets": {"iceberg": 41, "postgres": 14, "secret": 3},
                "data_flows": 12,
                "data_jobs": 39,
                "urn_sha256": "6a50a9dbbc133854",
                "urns": [
                    "urn:li:dataset:(urn:li:dataPlatform:postgres,sources.walmart.customers,PROD)"
                ],
            }
        ),
        json.dumps(
            {
                "event": "g1_check",
                "check": "gold_has_silver_upstream",
                "subjects": 10,
                "failed": ["gold.fact_orders"],
                "ok": False,
            }
        ),
        json.dumps(
            {
                "event": "metadata_verified",
                "datasets": 67,
                "pii": ["postgres:sources.walmart.customers"],
                "drift": [],
                "metadata_sha256": "ea76250eb8f9df7a",
                "ok": True,
            }
        ),
        json.dumps({"event": "g1_negative_finished", "edge_removed": False, "ok": True}),
    ]
    assert list(module.safe_events(lines)) == [
        {"event": "gov_smoke_check", "check": "probe_read", "ok": True},
        {"event": "gov_smoke_check", "ok": False},
        {
            "event": "datahub_inventory",
            "platforms": {"iceberg": 41, "postgres": 14},
            "data_flows": 12,
            "data_jobs": 39,
            "urn_sha256": "6a50a9dbbc133854",
        },
        {
            "event": "g1_check",
            "check": "gold_has_silver_upstream",
            "subjects": 10,
            "failed_count": 1,
            "ok": False,
        },
        {
            "event": "metadata_verified",
            "datasets": 67,
            "pii_count": 1,
            "drift_count": 0,
            "metadata_sha256": "ea76250eb8f9df7a",
            "ok": True,
        },
        {"event": "g1_negative_finished", "edge_removed": False, "ok": True},
    ]
