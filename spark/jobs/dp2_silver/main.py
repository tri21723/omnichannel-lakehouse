"""Validate typed Walmart candidates from a pinned input; publishing comes later."""

import argparse
import json
import logging
import os
from pathlib import Path

from spark.lib.bronze import event
from spark.lib.bronze_input import identity
from spark.lib.session import get_spark
from spark.lib.silver_input import load_pin, pin_completion, read_pinned
from spark.lib.silver_transform import walmart_candidates


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", choices=["walmart"], required=True)
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--input-id")
    parser.add_argument("--expect-orders", type=int)
    parser.add_argument("--fail-on-reject", action="store_true")
    parser.add_argument("--validate-only", action="store_true", required=True)
    args = parser.parse_args()
    key = bytes.fromhex(os.environ["SILVER_PII_KEY"])
    version = os.environ["SILVER_PII_KEY_VERSION"]
    if len(key) < 32 or not version:
        raise ValueError("PII key/version required before normalization")
    selection = dict(
        run_id=args.run_id, run_date=args.run_date, channel=args.channel, source="jdbc"
    )
    batch = identity(**selection)
    directory = Path("/runtime/silver/inputs")
    if args.input_id:
        import re

        if not re.fullmatch(r"[0-9a-f]{64}", args.input_id):
            raise ValueError("Invalid pinned input ID")
        pin = directory / f"{args.input_id}.json"
    else:
        pin = pin_completion(Path("/runtime/bronze") / f"{batch}.json", selection, directory)
    state = load_pin(pin)
    if state["selection"] != selection:
        raise ValueError("Input does not match selection")
    spark = get_spark()
    try:
        frames = read_pinned(spark, pin)
        result = walmart_candidates(spark, frames, state, pin.stem, key, version)
        report = {}
        for entity, value in result.items():
            valid = value["valid"].count()
            rejected = value["rejected"].count()
            if valid + rejected != value["input_rows"]:
                raise ValueError("Entity accounting mismatch")
            report[entity] = dict(input=value["input_rows"], valid=valid, rejected=rejected)
        if any(name in result["customers"]["valid"].columns for name in ("full_name", "email")):
            raise ValueError("Clear PII in candidate schema")
        if args.expect_orders is not None and report["orders"]["valid"] != args.expect_orders:
            raise ValueError("Unexpected valid order count")
        if args.fail_on_reject and any(value["rejected"] for value in report.values()):
            raise ValueError("Unexpected rejected fixture rows")
        event("silver_candidates_validated", input_id=pin.stem, entities=report)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        run()
    except Exception as exc:
        # Never emit payloads, secret values or exception messages from worker input.
        logging.error(
            json.dumps({"event": "silver_normalization_failed", "error_type": type(exc).__name__})
        )
        raise SystemExit(1) from None
