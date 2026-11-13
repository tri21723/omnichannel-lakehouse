"""Reproducible five-channel integration, pinned replay and committed-state validation."""

import argparse
import json
import logging
from types import SimpleNamespace

from spark.jobs.dp2_silver.main import ROOT, run_job
from spark.lib.bronze import event
from spark.lib.bronze_input import CHANNELS
from spark.lib.session import get_spark
from spark.lib.silver_publish import frame_at, verify


def release(spark):
    spark.catalog.clearCache()
    for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
        rdd.unpersist(False)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--orders-per-day", type=int, default=25)
    args = parser.parse_args()
    spark = get_spark()
    try:
        for channel in CHANNELS:
            for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
                selected = SimpleNamespace(
                    run_id=args.run_id,
                    run_date=day,
                    channel=channel,
                    mode="full",
                    input_id=None,
                    expect_orders=None,
                    fail_on_reject=True,
                    validate_only=False,
                    correction=False,
                    verify=False,
                )
                ledger = run_job(spark, selected)
                event(
                    "silver_matrix_slice_passed",
                    channel=channel,
                    day=day,
                    slices=len(ledger["slices"]),
                )
                release(spark)
        ledger = json.loads((ROOT / "ledger.json").read_text())
        verify(spark, ledger)
        table = "lh.silver.orders"
        frame = frame_at(spark, table, ledger["tables"][table], spark.table(table).schema)
        from pyspark.sql import functions as F

        counts = {
            row["channel"]: row["count"]
            for row in frame.filter(F.col("_dataset_run_id") == args.run_id)
            .groupBy("channel")
            .count()
            .collect()
        }
        assert counts == {channel: args.orders_per_day * 3 for channel in CHANNELS}
        # Same accepted pin, either mode, produces no new committed publication.
        before = (ROOT / "ledger.json").read_bytes()
        selected.channel = "walmart"
        selected.run_date = "2026-01-01"
        selected.mode = "incremental"
        run_job(spark, selected)
        assert (ROOT / "ledger.json").read_bytes() == before
        event("silver_matrix_passed", channels=5, orders=sum(counts.values()), run_id=args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
