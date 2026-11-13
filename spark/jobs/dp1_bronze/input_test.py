"""MinIO-to-Iceberg malformed/input-failure tests on immutable owned fixtures."""

import argparse
import hashlib
import importlib
import json
import logging
import os
from copy import deepcopy
from unittest.mock import patch

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from spark.lib.bronze import event, slice_filter, snapshot
from spark.lib.bronze_input import canonical, identity
from spark.lib.session import get_spark


def publish_fixture(client, seed: int, variant: str) -> str:
    # Full batch-v1 config, deliberately malformed raw syntax as a transport fixture.
    config = {
        "seed": seed,
        "start_date": "2026-01-01",
        "days": 1,
        "orders_per_day_per_channel": 4,
        "channels": ["walmart"],
        "dup_rate": 0.0,
        "skew_top_seller_share": 0.5,
        "schema_drift_at_day": 2,
        "return_rate": 0.0,
        "rows_per_file": 10,
        "late_rate": 0.05,
        "late_max_minutes": 60,
        "burst_multiplier": 50,
    }
    run_id = hashlib.sha256(
        canonical({"contract": "batch-v1", "config": config}).encode()
    ).hexdigest()[:20]
    key = f"walmart/dt=2026-01-01/run_id={run_id}/part-00000.jsonl"
    data = b'{"unknown":{"keep":true}}\nmalformed\n\n{"amount":"10.25"}\n'
    entry = {
        "path": key,
        "rows": 4,
        "bytes": len(data),
        "sha256": "0" * 64 if variant == "checksum" else hashlib.sha256(data).hexdigest(),
    }
    manifest = {
        "contract": "batch-v1",
        "config": config,
        "run_id": run_id,
        "channels": {"walmart": {"rows": 4, "unique_orders": 4, "dup_rows": 0, "files": [entry]}},
    }
    bucket = os.environ["S3_RAW_BUCKET"]
    objects = [(f"manifests/{run_id}/stream_manifest.json", canonical(manifest).encode())]
    if variant != "missing-file":
        objects.insert(0, (key, data))
    for name, content in objects:
        try:
            client.put_object(Bucket=bucket, Key=name, Body=content, IfNoneMatch="*")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "PreconditionFailed":
                raise
            with client.get_object(Bucket=bucket, Key=name)["Body"] as body:
                assert body.read() == content, "Owned fixture content changed"
    return run_id


def run():
    job = importlib.import_module("spark.jobs.dp1_bronze.main")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["GENERATOR_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["GENERATOR_S3_SECRET_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=30),
    )
    good = publish_fixture(client, 991701, "good")
    bad = {
        kind: publish_fixture(client, seed, kind)
        for seed, kind in ((991702, "missing-file"), (991703, "checksum"))
    }
    args = argparse.Namespace(
        run_id=good,
        run_date="2026-01-01",
        channel="walmart",
        source="jsonl",
        mode="full",
        verify=False,
    )
    spark = get_spark()
    try:
        job.main(args, spark)
        table = "lh.bronze.walmart_orders_export_raw"
        rows = (
            spark.table(table).filter(slice_filter(args)).select("_raw", "_parse_status").collect()
        )
        assert sorted(row["_parse_status"] for row in rows) == [
            "blank",
            "invalid",
            "valid",
            "valid",
        ]
        assert any(row["_raw"] == '{"unknown":{"keep":true}}' for row in rows)
        event("bronze_malformed_input_passed", rows=4)

        def rejected(selected, code, changes=None, target_spark=spark):
            before = snapshot(spark, table)
            with patch.dict(os.environ, changes or {}):
                try:
                    job.main(selected, target_spark)
                except Exception:
                    pass
                else:
                    raise AssertionError("Expected input/catalog failure: " + code)
            assert snapshot(spark, table) == before, "Failure changed the existing target"
            if code in {"missing-file", "checksum", "staging-byte-limit"}:
                batch = identity(
                    selected.run_id, selected.run_date, selected.channel, selected.source
                )
                from pathlib import Path

                state = json.loads((Path("/runtime/bronze") / f"{batch}.json").read_text())
                assert state["status"] == "failed"
                assert "completed_at" not in state
            event("bronze_input_rejected", reason=code, target_unchanged=True)

        for kind, run_id in bad.items():
            selected = deepcopy(args)
            selected.run_id = run_id
            rejected(selected, kind)
        absent = deepcopy(args)
        absent.run_id = "00000000000000000000"
        rejected(absent, "missing-manifest")
        selected = deepcopy(args)
        selected.run_date = "2026-01-02"
        rejected(selected, "date-outside-manifest")
        selected = deepcopy(args)
        selected.channel = "amazon"
        rejected(selected, "channel-outside-manifest")
        rejected(args, "staging-byte-limit", {"BRONZE_STAGE_MAX_BYTES": "1"})
        bad_catalog = spark.newSession()
        bad_catalog.conf.set(
            "spark.sql.catalog.lh.uri", os.environ["LAKEKEEPER_URI"] + "/missing/catalog"
        )
        rejected(args, "catalog-not-found", target_spark=bad_catalog)
        job.main(args, spark)
        args.verify = True
        job.main(args, spark)
        event("bronze_input_failure_matrix_passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
