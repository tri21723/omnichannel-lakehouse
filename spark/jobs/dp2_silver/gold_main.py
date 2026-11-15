"""Build and publish Gold dimensions and facts from pinned Silver publication."""

import argparse
import json
import logging
from pathlib import Path

from spark.lib.bronze import event
from spark.lib.gold_input import load_publication, read_silver_entities
from spark.lib.gold_pipeline import build_dimensions, build_facts
from spark.lib.gold_publish import publish_gold, verify_gold
from spark.lib.session import get_spark

SILVER_ROOT = Path("/runtime/silver/publication")
GOLD_ROOT = Path("/runtime/gold/publication")


def run_job(spark, args):
    if getattr(args, "verify", False):
        ledger_path = GOLD_ROOT / "ledger.json"
        if not ledger_path.exists():
            raise FileNotFoundError(f"Gold publication ledger not found: {ledger_path}")
        ledger = json.loads(ledger_path.read_text())
        verify_gold(spark, ledger, target_run_id=getattr(args, "run_id", None))
        event(
            "gold_verified",
            tables=len(ledger["tables"]),
            runs=list(ledger.get("runs", {}).keys()),
            metrics={
                "duration_seconds": ledger.get("duration_seconds", 0),
                "files": sum(val["files"]["count"] for val in ledger["tables"].values()),
                "bytes": sum(val["files"]["bytes"] for val in ledger["tables"].values()),
            },
        )
        return ledger

    silver_ledger, silver_pub_id = load_publication(
        SILVER_ROOT / "ledger.json", publication_id=args.publication_id
    )

    frames, snapshots, pub_id = read_silver_entities(
        spark, silver_ledger, args.run_id, publication_id=silver_pub_id
    )

    dims = build_dimensions(spark, frames, args.run_id, pub_id, snapshots)
    facts = build_facts(spark, frames, dims, args.run_id, pub_id, snapshots)

    all_tables = {**dims, **facts}

    ledger, skipped = publish_gold(
        spark,
        all_tables,
        args.run_id,
        pub_id,
        root=GOLD_ROOT,
    )

    event(
        "gold_published",
        run_id=args.run_id,
        silver_publication_id=pub_id,
        skipped=skipped,
        tables=len(ledger["tables"]),
    )
    return ledger


def run():
    parser = argparse.ArgumentParser(description="Publish Gold star schema tables.")
    parser.add_argument("--run-id", help="Dataset run ID to process")
    parser.add_argument("--publication-id", help="Explicit Silver publication ID to pin")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Full recompute or incremental skip",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify committed Gold snapshots without modifying data",
    )
    args = parser.parse_args()

    if not args.verify and not args.run_id:
        parser.error("--run-id is required unless --verify is specified")

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
        logging.error(json.dumps({"event": "gold_job_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1) from None
