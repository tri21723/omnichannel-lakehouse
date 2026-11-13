"""Integration fault/recovery and slice-isolation assertions on local fixtures."""

import argparse
import importlib
import logging
import os
import tempfile
from copy import copy
from pathlib import Path
from unittest.mock import patch

from spark.lib.bronze import audit, event, replace_slice, slice_filter, snapshot
from spark.lib.bronze_input import identity
from spark.lib.session import get_spark


def staging_probe(item):
    import socket
    import time
    from pathlib import Path

    time.sleep(0.05)
    return socket.gethostname(), Path(item).read_text()


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parsed = parser.parse_args()
    args = argparse.Namespace(
        run_id=parsed.run_id,
        run_date="2026-01-01",
        channel="walmart",
        source="jdbc",
        mode="full",
        verify=False,
    )
    job = importlib.import_module("spark.jobs.dp1_bronze.main")
    spark = get_spark()
    try:
        with tempfile.TemporaryDirectory(dir=os.environ["BRONZE_STAGE_DIR"]) as directory:
            path = Path(directory) / "probe"
            path.write_text("shared-stage-readable")
            results = (
                spark.sparkContext.parallelize([str(path)] * 16, 16).map(staging_probe).collect()
            )
            hosts = sorted({host for host, text in results if text == "shared-stage-readable"})
            assert len(hosts) == 2, "Expected both workers to read shared staging"
            event("bronze_staging_probe_passed", workers=hosts)

        # Dedicated fixture: prove one-day slices and run IDs cannot overwrite each other.
        fixture = copy(args)
        fixture.run_id = "0" * 20
        fixture.source = "jsonl"
        raw = spark.createDataFrame(
            [('{"fixture":1}', "s3://fixture/input", None, "valid")],
            "_raw string, _source_file string, _source_table string, _parse_status string",
        )
        first = audit(
            raw,
            fixture,
            identity(fixture.run_id, fixture.run_date, fixture.channel, fixture.source),
        )
        empty = replace_slice(spark, "lh.bronze._smoke_initial_empty", first.limit(0), fixture)
        assert empty["content"]["rows"] == "0"
        table = "lh.bronze._smoke_slice_isolation"
        replace_slice(spark, table, first, fixture)
        other = copy(fixture)
        other.run_id = "1" * 20
        second = audit(
            raw, other, identity(other.run_id, other.run_date, other.channel, other.source)
        )
        replace_slice(spark, table, second, other)
        replace_slice(spark, table, first.limit(0), fixture)
        assert spark.table(table).filter(slice_filter(fixture)).count() == 0
        assert spark.table(table).filter(slice_filter(other)).count() == 1
        replace_slice(spark, table, first, fixture)
        assert spark.table(table).filter(slice_filter(fixture)).count() == 1
        from pyspark.sql import functions as F

        before_snapshot = snapshot(spark, table)
        try:
            replace_slice(spark, table, first.withColumn("_dataset_run_id", F.lit(None)), fixture)
        except ValueError:
            pass
        else:
            raise AssertionError("NULL slice identity was accepted")
        assert snapshot(spark, table) == before_snapshot
        event("bronze_empty_and_isolation_passed")

        original = job.replace_slice
        committed = []

        def fail_after_commit(*values):
            result = original(*values)
            committed.append(result)
            raise RuntimeError("Injected failure after a table commit")

        with patch.object(job, "replace_slice", fail_after_commit):
            try:
                job.main(args, spark)
            except RuntimeError:
                assert len(committed) == 1
            else:
                raise AssertionError("Injected failure did not happen")
        args.verify = True
        try:
            job.main(args, spark)
        except ValueError:
            pass
        else:
            raise AssertionError("Partial run was incorrectly completed")
        args.verify = False
        job.main(args, spark)
        args.verify = True
        job.main(args, spark)
        event("bronze_partial_recovery_passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
