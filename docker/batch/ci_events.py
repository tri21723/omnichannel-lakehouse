"""Emit only allowlisted structured diagnostics; never copy raw logs into CI output."""

import json
import re
import sys
from pathlib import Path

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
        if not isinstance(item, dict) or not re.fullmatch(
            r"bronze_[a-z_]+", str(item.get("event", ""))
        ):
            continue
        clean = {"event": item["event"]}
        for key in ("batch_id", "attempt"):
            if re.fullmatch(r"[a-f0-9]{20,32}", str(item.get(key, ""))):
                clean[key] = item[key]
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
                if key in {"duration_seconds", "rows", "rows_per_second", "staged_bytes"}
                and type(value) in (int, float)
            }
        if re.fullmatch(r"[A-Za-z]+(?:Error|Exception)", str(item.get("error_type", ""))):
            clean["error_type"] = item["error_type"]
        yield clean


if __name__ == "__main__":
    with Path(sys.argv[1]).open(errors="replace") as stream:
        for entry in safe_events(stream):
            print(json.dumps(entry, sort_keys=True))
