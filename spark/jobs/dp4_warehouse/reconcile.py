"""Bi-directional Gold Iceberg <-> PostgreSQL warehouse.* reconciler CLI."""

import argparse
import logging

from spark.lib.bronze import event
from spark.lib.session import get_spark
from spark.lib.warehouse_input import DEFAULT_GOLD_ROOT
from spark.lib.warehouse_publish import ROOT as WAREHOUSE_ROOT
from spark.lib.warehouse_publish import load_warehouse_ledger
from spark.lib.warehouse_reconcile import reconcile_run


def run() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile Gold Iceberg pinned slices with PostgreSQL warehouse.*"
    )
    parser.add_argument("--run-id", help="Optional single dataset run ID to reconcile")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark()
    try:
        ledger, export_id = load_warehouse_ledger(WAREHOUSE_ROOT / "ledger.json", spark=spark)
        runs = ledger.get("runs", {})
        if args.run_id:
            if args.run_id not in runs:
                raise ValueError(f"Run {args.run_id} is not present in warehouse export ledger")
            target_runs = [args.run_id]
        else:
            target_runs = sorted(runs.keys())
        results = {}
        for run_id in target_runs:
            results[run_id] = reconcile_run(
                spark, run_id, runs[run_id], gold_root=DEFAULT_GOLD_ROOT
            )
        event(
            "warehouse_reconciled",
            export_id=export_id,
            runs=target_runs,
            row_counts={rid: res["row_counts"] for rid, res in results.items()},
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
