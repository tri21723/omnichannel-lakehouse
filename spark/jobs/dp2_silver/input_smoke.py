"""Verify pinned input reconciliation and replay independence on real Iceberg tables."""

import argparse
import importlib
import json
import logging
from copy import deepcopy
from pathlib import Path

from spark.lib.bronze import content_summary, event
from spark.lib.bronze_input import identity
from spark.lib.session import get_spark
from spark.lib.silver_input import load_pin, pin_completion, read_pinned


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    selection = dict(run_id=args.run_id, run_date="2026-01-01", channel="walmart", source="jdbc")
    batch = identity(**selection)
    completion = Path("/runtime/bronze") / f"{batch}.json"
    directory = Path("/runtime/silver/inputs")
    first = pin_completion(completion, selection, directory)
    original = first.read_bytes()
    spark = get_spark()
    try:
        initial = read_pinned(spark, first)
        timestamps = {
            table: {row[0] for row in df.select("_ingest_ts").distinct().collect()}
            for table, df in initial.items()
        }
        before = {table: content_summary(df) for table, df in initial.items()}
        bronze = importlib.import_module("spark.jobs.dp1_bronze.main")
        bronze.main(argparse.Namespace(**selection, mode="full", verify=False), spark)
        second = pin_completion(completion, selection, directory)
        assert first != second, "Replay should create distinct snapshot references"
        assert first.read_bytes() == original
        assert before == {
            table: content_summary(df) for table, df in read_pinned(spark, first).items()
        }
        assert before == {
            table: content_summary(df) for table, df in read_pinned(spark, second).items()
        }
        old_frames, new_frames = read_pinned(spark, first), read_pinned(spark, second)
        for table, frame in old_frames.items():
            old_times = {row[0] for row in frame.select("_ingest_ts").distinct().collect()}
            new_times = {
                row[0] for row in new_frames[table].select("_ingest_ts").distinct().collect()
            }
            assert old_times == timestamps[table]
            assert old_times != new_times, "Snapshot reader unexpectedly used the latest ingestion"
        # A bogus snapshot must fail without falling back to latest, even if latest reconciles.
        invalid = deepcopy(json.loads(completion.read_text()))
        invalid["tables"]["lh.bronze.walmart_orders_raw"]["snapshot"] = 1
        fake = directory / "invalid-completion.json"
        fake.write_text(json.dumps(invalid))
        try:
            bad = pin_completion(fake, selection, directory)
            try:
                read_pinned(spark, bad)
            except ValueError:
                pass
            else:
                raise AssertionError("Missing snapshot accepted")
        finally:
            fake.unlink()
        assert load_pin(first)["selection"] == selection
        event("silver_input_smoke_passed", tables=len(before), input_id=first.stem)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
