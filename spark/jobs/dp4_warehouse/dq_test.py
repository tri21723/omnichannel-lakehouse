"""Comprehensive data-quality, referential integrity, and PII checks for dp4_warehouse."""

import argparse
import json
import logging
from pathlib import Path

from spark.lib.bronze import event
from spark.lib.bronze_jdbc import rollback_close
from spark.lib.dp3_input import DEFAULT_GOLD_ROOT, load_gold_publication
from spark.lib.dp3_publish import ROOT as DP3_ROOT
from spark.lib.dp3_publish import load_dp3_publication
from spark.lib.session import get_spark
from spark.lib.warehouse_jdbc import writer_connection
from spark.lib.warehouse_publish import ROOT as WAREHOUSE_ROOT
from spark.lib.warehouse_publish import load_warehouse_ledger
from spark.lib.warehouse_reconcile import check_postgres_constraints, reconcile_run


def _ledger_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def run() -> None:
    parser = argparse.ArgumentParser(description="Run DP4 warehouse data-quality test suite.")
    parser.add_argument("--run-id", help="Optional single dataset run ID to validate")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Verify Gold & DP3 upstream ledgers remain intact and unmutated
    _gold_ledger, gold_pub_id = load_gold_publication(DEFAULT_GOLD_ROOT / "ledger.json")
    dp3_path = DP3_ROOT / "ledger.json"
    dp3_pub_id = None
    if dp3_path.exists():
        _dp3_ledger, dp3_pub_id = load_dp3_publication(dp3_path)

    spark = get_spark()
    try:
        ledger, export_id = load_warehouse_ledger(WAREHOUSE_ROOT / "ledger.json", spark=spark)
        runs = ledger.get("runs", {})
        target_runs = [args.run_id] if args.run_id else sorted(runs.keys())
        for rid in target_runs:
            if rid not in runs:
                raise ValueError(f"Run {rid} is missing from warehouse export ledger")

        gold_before = _ledger_digest(DEFAULT_GOLD_ROOT / "ledger.json")
        dp3_before = _ledger_digest(dp3_path)
        conn = writer_connection(spark, read_only=True)
        try:
            constraint_summary = check_postgres_constraints(conn)
        finally:
            rollback_close(conn)

        recon_results = {}
        for rid in target_runs:
            recon_results[rid] = reconcile_run(spark, rid, runs[rid], gold_root=DEFAULT_GOLD_ROOT)

        assert _ledger_digest(DEFAULT_GOLD_ROOT / "ledger.json") == gold_before
        assert _ledger_digest(dp3_path) == dp3_before

        event(
            "warehouse_dq_passed",
            export_id=export_id,
            gold_publication_id=gold_pub_id,
            dp3_publication_id=dp3_pub_id,
            runs=target_runs,
            constraints=constraint_summary,
            summary=json.loads(
                json.dumps({rid: res["row_counts"] for rid, res in recon_results.items()})
            ),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
