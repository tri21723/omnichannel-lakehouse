"""Emit only allowlisted structured diagnostics; never copy raw logs into CI output."""

import json
import re
import sys
from pathlib import Path

EVENTS = {
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
}
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
}


def safe_events(lines):
    for line in lines:
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if not isinstance(item, dict) or not isinstance(item.get("event"), str):
            continue
        if item["event"] not in EVENTS:
            continue
        clean = {"event": item["event"]}
        for key in ("batch_id", "attempt"):
            if re.fullmatch(r"[a-f0-9]{20,32}", str(item.get(key, ""))):
                clean[key] = item[key]
        if isinstance(item.get("input_id"), str) and re.fullmatch(
            r"[a-f0-9]{64}", item["input_id"]
        ):
            clean["input_id"] = item["input_id"]
        if item.get("channel") in ("walmart", "amazon", "tiktok_shop", "shopee", "lazada"):
            clean["channel"] = item["channel"]
        if item.get("stage") in ("contribution", "quarantine", "completion"):
            clean["stage"] = item["stage"]
        for name in ("slices", "orders", "channels", "failures"):
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
        if isinstance(item.get("reason"), str) and item["reason"] in REASONS:
            clean["reason"] = item["reason"]
        for key in (
            "all_slices",
            "target_unchanged",
            "readers",
            "rows",
            "jsonl_rows",
            "jdbc_orders",
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
