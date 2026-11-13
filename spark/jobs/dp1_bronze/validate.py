"""Explicit distributed integration matrix on the default immutable source run."""

import argparse
import logging

from spark.jobs.dp1_bronze.main import main
from spark.lib.bronze import event
from spark.lib.bronze_input import CHANNELS
from spark.lib.session import get_spark


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--all-slices", action="store_true")
    args = parser.parse_args()
    spark = get_spark()
    try:
        channels = CHANNELS if args.all_slices else ("walmart",)
        dates = ("2026-01-01", "2026-01-02", "2026-01-03") if args.all_slices else ("2026-01-01",)
        for source in ("jsonl", "jdbc"):
            for channel in channels if source == "jsonl" else ("walmart",):
                for day in dates:
                    selected = argparse.Namespace(
                        run_id=args.run_id,
                        run_date=day,
                        channel=channel,
                        source=source,
                        mode="full",
                        verify=False,
                    )
                    main(selected, spark)
                    main(selected, spark)  # Full replay preserves exact multiset.
                    selected.mode = "incremental"
                    main(selected, spark)
                    selected.verify = True
                    main(selected, spark)
        event("bronze_matrix_passed", run_id=args.run_id, all_slices=args.all_slices)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
