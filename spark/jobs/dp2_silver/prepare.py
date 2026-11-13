"""Prepare missing authoritative Bronze completions for the small Silver fixture."""

import argparse
import json
import logging
from pathlib import Path
from types import SimpleNamespace

from spark.jobs.dp1_bronze.main import main as bronze_job
from spark.lib.bronze_input import CHANNELS, identity
from spark.lib.session import get_spark
from spark.lib.silver_input import validate_completion


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    spark = get_spark()
    try:
        for channel in CHANNELS:
            for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
                selection = dict(
                    run_id=args.run_id,
                    run_date=day,
                    channel=channel,
                    source="jdbc" if channel == "walmart" else "jsonl",
                )
                batch = identity(**selection)
                path = Path("/runtime/bronze") / f"{batch}.json"
                if path.exists():
                    validate_completion(json.loads(path.read_text()), selection)
                    continue
                bronze_job(SimpleNamespace(**selection, mode="full", verify=False), spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
