"""Build and publish supervised order return labels with maturity right-censoring."""

import argparse
import json
import logging

from pyspark.sql import functions as F

from spark.lib.bronze import event
from spark.lib.dp3_input import DEFAULT_GOLD_ROOT, load_gold_publication, read_gold_entities
from spark.lib.dp3_labels import build_return_labels
from spark.lib.dp3_publish import ROOT as DP3_ROOT
from spark.lib.dp3_publish import publish_dp3, verify_dp3
from spark.lib.session import get_spark


def run_job(spark, args):
    if getattr(args, "verify", False):
        ledger_path = DP3_ROOT / "ledger.json"
        if not ledger_path.exists():
            raise FileNotFoundError(f"DP3 publication ledger not found: {ledger_path}")
        ledger = json.loads(ledger_path.read_text())
        verify_dp3(
            spark,
            ledger,
            target_run_id=getattr(args, "run_id", None),
            target_tables=["lbl_order_return"],
        )
        event(
            "labels_verified",
            runs=list(ledger.get("runs", {}).keys()),
            target_run_id=getattr(args, "run_id", None),
        )
        return ledger

    gold_ledger, gold_pub_id = load_gold_publication(
        DEFAULT_GOLD_ROOT / "ledger.json", publication_id=args.publication_id
    )

    frames, snapshots, pub_id = read_gold_entities(
        spark, gold_ledger, args.run_id, publication_id=gold_pub_id
    )

    horizon_days = getattr(args, "horizon_days", 14) or 14

    if getattr(args, "all_dates", False):
        distinct_rows = (
            frames["fact_orders"]
            .select(F.to_date("order_ts").alias("dt"))
            .filter(F.col("dt").isNotNull())
            .distinct()
            .sort("dt")
            .collect()
        )
        dates = [r["dt"].isoformat() for r in distinct_rows]
        if not dates:
            raise ValueError(f"No order dates found in fact_orders for run {args.run_id}")
    elif getattr(args, "as_of_date", None):
        dates = [d.strip() for d in args.as_of_date.split(",") if d.strip()]
    else:
        max_dt_row = frames["fact_orders"].select(F.to_date(F.max("order_ts"))).first()
        max_dt = max_dt_row[0] if max_dt_row else None
        if not max_dt:
            raise ValueError(f"No orders found in fact_orders for run {args.run_id}")
        dates = [max_dt.isoformat()]

    label_tables = build_return_labels(
        spark, frames, args.run_id, pub_id, dates, horizon_days=horizon_days
    )

    ledger, skipped = publish_dp3(
        spark,
        label_tables,
        args.run_id,
        pub_id,
        root=DP3_ROOT,
        mode=args.mode,
        as_of_dates=dates,
        horizon_days=horizon_days,
        force=getattr(args, "force", False),
    )

    event(
        "labels_published",
        run_id=args.run_id,
        gold_publication_id=pub_id,
        skipped=skipped,
        horizon_days=horizon_days,
        evaluation_dates=dates,
    )
    return ledger


def run():
    parser = argparse.ArgumentParser(
        description="Publish supervised order return labels with maturity guard."
    )
    parser.add_argument("--run-id", help="Dataset run ID to process")
    parser.add_argument("--publication-id", help="Explicit Gold publication ID to pin")
    parser.add_argument(
        "--as-of-date",
        help="Evaluation date (YYYY-MM-DD) or comma-separated list of dates",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Evaluate all distinct order dates for the dataset run",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=14,
        help="Return observation horizon in days (default: 14)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Full recompute or incremental update",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify committed DP3 label tables without modifying data",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recompute even if skip condition met",
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
        logging.error(json.dumps({"event": "labels_job_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1) from None
