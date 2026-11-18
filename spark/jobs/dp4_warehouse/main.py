"""Synchronize a 30-day sliding window of Gold star-schema tables to PostgreSQL."""

import argparse
import logging

from spark.lib.bronze import event
from spark.lib.session import get_spark
from spark.lib.warehouse_input import (
    DEFAULT_GOLD_ROOT,
    read_gold_star_schema,
    resolve_run_date,
    slice_warehouse_window,
)
from spark.lib.warehouse_publish import (
    ROOT as WAREHOUSE_ROOT,
)
from spark.lib.warehouse_publish import (
    load_warehouse_ledger,
    publish_warehouse,
    verify_warehouse_ledger,
)


def run_job(spark, args):
    if getattr(args, "verify", False):
        ledger, export_id = load_warehouse_ledger(WAREHOUSE_ROOT / "ledger.json", spark=spark)
        verified_runs = verify_warehouse_ledger(
            spark, ledger, target_run_id=getattr(args, "run_id", None)
        )
        event(
            "warehouse_verified",
            export_id=export_id,
            runs=verified_runs,
            target_run_id=getattr(args, "run_id", None),
        )
        return ledger

    frames, _snapshots, _run_meta, gold_pub_id = read_gold_star_schema(
        spark,
        args.run_id,
        gold_root=DEFAULT_GOLD_ROOT,
        publication_id=getattr(args, "publication_id", None),
    )
    resolved_date = resolve_run_date(frames, getattr(args, "run_date", None))
    sliced, w_start, w_end = slice_warehouse_window(frames, resolved_date)

    ledger, skipped = publish_warehouse(
        spark,
        sliced,
        args.run_id,
        gold_pub_id,
        resolved_date,
        w_start,
        w_end,
        root=WAREHOUSE_ROOT,
        fail_after=getattr(args, "fail_after", None),
        force=getattr(args, "force", False),
        mode=args.mode,
        no_regress=getattr(args, "no_regress", False),
    )
    run_entry = ledger["runs"][args.run_id]
    counts = {name.split(".", 1)[1]: meta["rows"] for name, meta in run_entry["tables"].items()}
    event(
        "warehouse_exported",
        run_id=args.run_id,
        gold_publication_id=gold_pub_id,
        run_date=resolved_date.isoformat(),
        skipped=skipped,
        row_counts=counts,
    )
    return ledger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a 30-day Gold star-schema slice to PostgreSQL warehouse.*"
    )
    parser.add_argument("--run-id", help="Dataset run ID to export")
    parser.add_argument("--run-date", help="Logical run date (YYYY-MM-DD UTC)")
    parser.add_argument("--publication-id", help="Explicit Gold publication ID to pin")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Export mode (both replace the 30-day run slice atomically)",
    )
    parser.add_argument(
        "--no-regress",
        action="store_true",
        help="Skip a run date older than the committed export from the same Gold publication",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rewrite even if identical export already completed",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify committed PostgreSQL warehouse slice read-only",
    )
    parser.add_argument(
        "--fail-after",
        choices=["dimensions", "facts", "completion", "db_commit"],
        help="Inject deterministic rollback fault for recovery testing",
    )
    return parser


def run() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.verify and not args.run_id:
        parser.error("--run-id is required unless --verify is specified")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark()
    try:
        run_job(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
