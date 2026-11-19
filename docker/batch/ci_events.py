"""Emit only allowlisted structured diagnostics; never copy raw logs into CI output."""

import json
import re
import sys
from pathlib import Path

EVENTS = {
    "review_regressions_passed",
    "warehouse_review_regressions_passed",
    "silver_published",
    "silver_verified",
    "silver_candidates_validated",
    "silver_normalization_failed",
    "silver_matrix_slice_passed",
    "silver_matrix_passed",
    "silver_publish_failure_preserved",
    "silver_catalog_failure_preserved",
    "silver_correction_recovery_passed",
    "silver_reference_verified",
    "silver_channel_dedup_passed",
    "silver_dq_cases_passed",
    "bronze_completed",
    "bronze_verified",
    "bronze_failed",
    "bronze_skipped",
    "bronze_staging_probe_passed",
    "bronze_empty_and_isolation_passed",
    "bronze_partial_recovery_passed",
    "bronze_matrix_passed",
    "bronze_snapshot_mutation_passed",
    "bronze_snapshot_failure_recovered",
    "bronze_malformed_input_passed",
    "bronze_input_rejected",
    "bronze_input_failure_matrix_passed",
    "bronze_channel_contract_passed",
    "bronze_raw_contract_passed",
    "gold_published",
    "gold_verified",
    "gold_reconciled",
    "gold_dq_passed",
    "gold_recovery_passed",
    "features_published",
    "features_verified",
    "labels_published",
    "labels_verified",
    "dp3_dq_passed",
    "dp3_recovery_passed",
    "warehouse_exported",
    "warehouse_verified",
    "warehouse_reconciled",
    "warehouse_dq_passed",
    "warehouse_recovery_passed",
    "benchmark_salting_tests_passed",
    "benchmark_dataset_built",
    "benchmark_dataset_skipped",
    "benchmark_dataset_verified",
    "benchmark_preflight_ok",
    "benchmark_progress",
    "benchmark_trial_finished",
    "benchmark_finished",
    "benchmark_verified",
    "spark_driver_busy",
    "iceberg_verified",
    "e2e_backfill_created",
    "e2e_summary",
    "query_sample",
    "cdc_bronze_batch",
    "cdc_bronze_completed",
    "cdc_bronze_verified",
    "cdc_silver_completed",
    "cdc_silver_verified",
    "cdc_verify_check",
    "cdc_verified",
    "cdc_smoke_check",
    "cdc_smoke_finished",
    "cdc_batch_guard",
    "cdc_slot_lag",
    "cdc_connector_registered",
    "cdc_connector_status",
    "cdc_scenario_applied",
    "cdc_scenario_skipped",
    "cdc_source_checked",
    "cdc_resnapshot_requested",
    "stream_smoke_check",
    "stream_smoke_finished",
    "stream_job_started",
    "stream_job_resumed",
    "stream_job_stopped",
    "stream_job_running",
    "stream_published",
    "stream_publish_skipped",
    "stream_checked",
    "stream_verify_check",
    "stream_verified",
    "stream_closed",
    "stream_acceptance_finished",
    "query_smoke_check",
    "query_smoke_finished",
    "query_verify_check",
    "query_samples_finished",
    "query_parity_prepared",
    "query_verify_finished",
    "query_checks_reconciled",
}
BENCHMARK_STATUSES = {"ok", "error", "timeout", "correctness_failed"}
ERROR_TYPES = {
    "ValueError",
    "RuntimeError",
    "Py4JJavaError",
    "ClientError",
    "UnicodeDecodeError",
    "KeyError",
    "OSError",
    "JSONDecodeError",
    "ModuleNotFoundError",
    "ConnectionError",
}

# Fixed check names of docker/query/smoke.py and verify.py (no SQL, values or identifiers).
# Fixed check names of docker/stream/smoke.sh and the stream verifier; channels and job names.
STREAM_CHECKS = {
    "kafka_topic",
    "kafka_partitions",
    "kafka_produce_consume",
    "kafka_host_port",
    "kafka_ui",
    "flink_slots",
    "flink_job_s3_write",
    "flink_user_stream_write",
    "flink_user_checkpoint_write",
    "flink_user_denied_raw_outside_stream",
    "flink_user_denied_lakehouse",
    "flink_user_denied_manifest_read",
    "flink_output_files",
    "smoke_cleanup",
    "no_secret_in_flink_logs",
    "committed_files_only",
    "accepted_unique",
    "on_time_ids",
    "late_accepted_ids",
    "too_late_ids",
    "windows_final",
    "event_ids_complete",
    "accepted_late_disjoint",
    "windows_match_accepted",
    "duplicates",
    "duplicate_ids",
    "rule_per_record",
    "rule_per_late_record",
    "dlq",
    "nothing_lost",
    "counts",
}
CDC_CHECKS = {
    "connector_running",
    "slot_active_pgoutput",
    "publication_orders_only",
    "no_customers_topic",
    "snapshot_complete",
    "probe_dml_applied",
    "probe_events_in_order",
    "decimal_as_string",
    "heartbeat_flowing",
    "slot_lag_bytes",
    "no_secret_in_connect_logs",
    "config_stores_placeholder",
    "live_orders_equal",
    "live_ids_equal",
    "checksum_equal",
    "deleted_absent_in_source",
    "scenario_live_equal",
    "scenario_checksum_equal",
    "scenario_deleted_marked",
    "scenario_events_present",
}
STREAM_NAMES = {"amazon", "tiktok_shop", "amazon_late", "tiktok_dedup"}
QUERY_CHECKS = {
    "catalogs",
    "iceberg_schemas",
    "postgresql_schemas",
    "iceberg_gold_counts",
    "warehouse_counts",
    "iceberg_metadata_table",
    "pii_masked",
    "denied",
    "ledger_rows",
    "spark_parity",
    "warehouse_three_way",
    "time_travel",
}
REASONS = {
    "source_fingerprint_mismatch",
    "source_row_count_mismatch",
    "invalid_selection",
    "staging_limit_exceeded",
    "source_object_missing",
    "source_storage_error",
    "raw_encoding_invalid",
    "runtime_or_validation_failure",
    "missing-file",
    "missing-manifest",
    "checksum",
    "date-outside-manifest",
    "channel-outside-manifest",
    "staging-byte-limit",
    "catalog-not-found",
    "source_unchanged",
}


def _json_object(line):
    """Parse a JSON line, or the JSON object after a log prefix such as `<time> INFO `."""
    for candidate in (line, line[line.find("{") :] if "{" in line else ""):
        try:
            return json.loads(candidate)
        except ValueError:
            continue
    return None


def safe_events(lines):
    for line in lines:
        item = _json_object(line)
        if not isinstance(item, dict) or not isinstance(item.get("event"), str):
            continue
        if item["event"] not in EVENTS:
            continue
        clean = {"event": item["event"]}
        for key in ("batch_id", "attempt", "run_id"):
            if re.fullmatch(r"[a-f0-9]{20,32}", str(item.get(key, ""))):
                clean[key] = item[key]
        if isinstance(item.get("input_id"), str) and re.fullmatch(
            r"[a-f0-9]{64}", item["input_id"]
        ):
            clean["input_id"] = item["input_id"]
        for digest_key in ("export_id", "gold_publication_id"):
            if isinstance(item.get(digest_key), str) and re.fullmatch(
                r"[a-f0-9]{64}", item[digest_key]
            ):
                clean[digest_key] = item[digest_key]
        if isinstance(item.get("run_date"), str) and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", item["run_date"]
        ):
            clean["run_date"] = item["run_date"]
        if item.get("channel") in ("walmart", "amazon", "tiktok_shop", "shopee", "lazada"):
            clean["channel"] = item["channel"]
        for key in ("dataset", "experiment"):
            if isinstance(item.get(key), str) and re.fullmatch(
                r"[a-z0-9][a-z0-9_-]{0,47}", item[key]
            ):
                clean[key] = item[key]
        if item.get("status") in BENCHMARK_STATUSES:
            clean["status"] = item["status"]
        for key in ("query_s",):
            if type(item.get(key)) in (int, float):
                clean[key] = item[key]
        if type(item.get("correct")) is bool:
            clean["correct"] = item["correct"]
        if item.get("stage") in ("contribution", "quarantine", "completion"):
            clean["stage"] = item["stage"]
        for name in (
            "slices",
            "orders",
            "channels",
            "failures",
            "cases",
            "tables",
            "horizon_days",
            "done",
            "total",
            "problems",
            "keys",
            "distinct_orders",
            "backfill_id",
            "controller_runs",
        ):
            if type(item.get(name)) is int:
                clean[name] = item[name]
        for name in ("entities", "counts"):
            if isinstance(item.get(name), dict):
                values = {}
                for entity in ("orders", "order_items", "customers", "products", "returns"):
                    value = item[name].get(entity)
                    if type(value) is int:
                        values[entity] = value
                    elif isinstance(value, dict):
                        values[entity] = {
                            k: v
                            for k, v in value.items()
                            if k in {"input", "valid", "rejected", "winners", "duplicate_losers"}
                            and type(v) is int
                        }
                clean[name] = values
        if isinstance(item.get("file"), str) and re.fullmatch(
            r"\d\d_[a-z0-9_]+\.sql", item["file"]
        ):
            clean["file"] = item["file"]
        if isinstance(item.get("error_name"), str) and re.fullmatch(
            r"[A-Z_]{1,64}", item["error_name"]
        ):
            clean["error_name"] = item["error_name"]
        if item.get("check") in CDC_CHECKS:
            clean["check"] = item["check"]
        for key in ("inserted", "live", "deleted", "quarantined", "silver_live", "source_orders"):
            if type(item.get(key)) is int:
                clean[key] = item[key]
        for key in ("changed", "exact_ops", "equals_latest_bronze", "checksum_equal"):
            if type(item.get(key)) is bool:
                clean[key] = item[key]
        if item.get("check") in STREAM_CHECKS:
            clean["check"] = item["check"]
        for key in ("channel", "job"):
            if item.get(key) in STREAM_NAMES:
                clean[key] = item[key]
        for key in ("failover", "drain"):
            if type(item.get(key)) is bool:
                clean[key] = item[key]
        for key in ("records", "accepted", "late", "dlq", "duplicates_found"):
            if type(item.get(key)) is int:
                clean[key] = item[key]
        if item.get("check") in QUERY_CHECKS:
            clean["check"] = item["check"]
        if isinstance(item.get("reason"), str) and item["reason"] in REASONS:
            clean["reason"] = item["reason"]
        for key in (
            "all_slices",
            "target_unchanged",
            "readers",
            "rows",
            "jsonl_rows",
            "jdbc_orders",
            "skipped",
            "ok",
            "checks",
            "failures",
            "files",
            "groups",
        ):
            if type(item.get(key)) in (int, bool):
                clean[key] = item[key]
        if isinstance(item.get("metrics"), dict):
            clean["metrics"] = {
                key: value
                for key, value in item["metrics"].items()
                if key
                in {"duration_seconds", "rows", "rows_per_second", "staged_bytes", "files", "bytes"}
                and type(value) in (int, float)
            }
        if isinstance(item.get("error_type"), str) and item["error_type"] in ERROR_TYPES:
            clean["error_type"] = item["error_type"]
        yield clean


if __name__ == "__main__":
    with Path(sys.argv[1]).open(errors="replace") as stream:
        for entry in safe_events(stream):
            print(json.dumps(entry, sort_keys=True))
