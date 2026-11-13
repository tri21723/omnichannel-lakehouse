"""Normalize and publish five Silver entities from immutable Bronze inputs."""

import argparse
import hashlib
import json
import logging
import os
import re
from pathlib import Path

from spark.lib.bronze import event
from spark.lib.bronze_input import CHANNELS, identity
from spark.lib.session import get_spark
from spark.lib.silver_input import load_pin, pin_completion, read_pinned
from spark.lib.silver_pipeline import candidates
from spark.lib.silver_publish import publish, verify

ROOT = Path("/runtime/silver/publication")


def key_settings():
    key = bytes.fromhex(os.environ["SILVER_PII_KEY"])
    version = os.environ["SILVER_PII_KEY_VERSION"]
    if len(key) < 32 or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", version):
        raise ValueError("PII key/version required before normalization")
    return key, version


def run_job(spark, args):
    key, version = key_settings()
    if getattr(args, "verify", False):
        ledger = json.loads((ROOT / "ledger.json").read_text())
        verify(spark, ledger)
        event(
            "silver_verified",
            tables=len(ledger["tables"]),
            slices=len(ledger["slices"]),
            counts={
                name: int(ledger["tables"][f"lh.silver.{name}"]["content"]["rows"])
                for name in ("orders", "order_items", "customers", "products", "returns")
            },
            metrics={
                "duration_seconds": ledger["duration_seconds"],
                "files": sum(value["files"]["count"] for value in ledger["tables"].values()),
                "bytes": sum(value["files"]["bytes"] for value in ledger["tables"].values()),
            },
        )
        return ledger
    selection = dict(
        run_id=args.run_id,
        run_date=args.run_date,
        channel=args.channel,
        source="jdbc" if args.channel == "walmart" else "jsonl",
    )
    batch = identity(**selection)
    directory = Path("/runtime/silver/inputs")
    if args.input_id:
        if not re.fullmatch(r"[0-9a-f]{64}", args.input_id):
            raise ValueError("Invalid pinned input ID")
        pin = directory / f"{args.input_id}.json"
    else:
        pin = pin_completion(Path("/runtime/bronze") / f"{batch}.json", selection, directory)
    state = load_pin(pin)
    if state["selection"] != selection:
        raise ValueError("Input does not match selection")
    frames = read_pinned(spark, pin)
    result = candidates(spark, frames, state, pin.stem, key, version)
    report = {entity: value["counts"] for entity, value in result.items()}
    if args.expect_orders is not None and report["orders"]["valid"] != args.expect_orders:
        raise ValueError("Unexpected valid order count")
    if args.fail_on_reject and any(value["rejected"] for value in report.values()):
        raise ValueError("Unexpected rejected fixture rows")
    if args.validate_only:
        event("silver_candidates_validated", input_id=pin.stem, entities=report)
        return report
    ledger, skipped = publish(
        spark,
        result,
        pin.stem,
        state,
        version,
        ROOT,
        correction=getattr(args, "correction", False),
        key_id=hashlib.sha256(key).hexdigest(),
    )
    event(
        "silver_published",
        input_id=pin.stem,
        entities=report,
        skipped=skipped,
        slices=len(ledger["slices"]),
    )
    return ledger


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--input-id")
    parser.add_argument("--expect-orders", type=int)
    parser.add_argument("--fail-on-reject", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--correction", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    key_settings()
    spark = get_spark()
    try:
        run_job(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        run()
    except Exception as exc:
        logging.error(
            json.dumps({"event": "silver_normalization_failed", "error_type": type(exc).__name__})
        )
        raise SystemExit(1) from None
