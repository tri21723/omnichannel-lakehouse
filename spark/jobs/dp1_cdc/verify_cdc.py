"""verify-cdc: PostgreSQL walmart.orders ↔ Silver current state ↔ scenario ground truth (read-only).

Run while nothing writes to walmart.orders. Whole table: the live order set and the checksum over
(order_id, status, total_amount, updated_ts) match, and no order deleted in Silver exists in
PostgreSQL. Scenario prefix: live count, checksum and deleted ids equal the ground truth, and Bronze
holds at least the expected c/u/d events (exactly, unless a resnapshot re-read the prefix as `r`).
"""

import argparse
import json
import logging
import os
from datetime import date
from decimal import Decimal

import boto3
from botocore.config import Config

from spark.lib.cdc import BRONZE_TABLE, SILVER_TABLE, scenario_checksum
from spark.lib.session import get_spark

log = logging.getLogger("verify_cdc")


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--channel", default="walmart", choices=("walmart",))
    parser.add_argument("--scenario", required=True, help="cdc_run_id of the applied scenario")
    args = parser.parse_args(argv)
    if date.fromisoformat(args.run_date).isoformat() != args.run_date:
        raise ValueError("Date must be ISO YYYY-MM-DD")
    if len(args.scenario) != 20 or any(c not in "0123456789abcdef" for c in args.scenario):
        raise ValueError("--scenario must be 20 lowercase hex")
    return args


def ground_truth(run_id: str) -> dict:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["SPARK_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SPARK_S3_SECRET_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )
    key = f"manifests/{run_id}/cdc_ground_truth.json"
    with client.get_object(Bucket=os.environ["S3_RAW_BUCKET"], Key=key)["Body"] as body:
        return json.loads(body.read())


def as_rows(rows) -> list[dict]:
    return [
        {
            "order_id": r["order_id"],
            "status": r["status"],
            "total_amount": f"{Decimal(r['total_amount']):.2f}",
            "updated_ts": r["updated_ts"],
        }
        for r in rows
    ]


def main(argv=None) -> int:
    from pyspark.sql import functions as F

    args = arguments(argv)
    spark = get_spark(app_name="verify_cdc")
    try:
        source = as_rows(
            spark.read.jdbc(
                os.environ["BRONZE_JDBC_URL"],
                "walmart.orders",
                properties={
                    "user": "spark_reader",
                    "password": os.environ["SPARK_READER_DB_PASSWORD"],
                    "driver": "org.postgresql.Driver",
                },
            )
            .select("order_id", "status", "total_amount", "updated_ts")
            .collect()
        )
        silver = spark.table(SILVER_TABLE)
        live = as_rows(silver.filter("NOT is_deleted").collect())
        deleted = {r["order_id"] for r in silver.filter("is_deleted").select("order_id").collect()}
        source_ids = {r["order_id"] for r in source}

        truth = ground_truth(args.scenario)
        prefix = truth["prefix"]
        scenario_live = [r for r in live if r["order_id"].startswith(prefix)]
        ops = {
            r["op"]: r["count"]
            for r in spark.table(BRONZE_TABLE)
            .filter(F.col("order_id").startswith(prefix) & (F.col("_parse_status") == "valid"))
            .groupBy("op")
            .count()
            .collect()
        }
        expected_ops = truth["expected_ops"]
        checks = {
            "live_orders_equal": len(live) == len(source),
            "live_ids_equal": {r["order_id"] for r in live} == source_ids,
            "checksum_equal": scenario_checksum(live) == scenario_checksum(source),
            "deleted_absent_in_source": not deleted & source_ids,
            "scenario_live_equal": len(scenario_live) == truth["live_orders"],
            "scenario_checksum_equal": scenario_checksum(scenario_live) == truth["final_checksum"],
            "scenario_deleted_marked": set(truth["deleted_orders"]) <= deleted,
            "scenario_events_present": all(ops.get(op, 0) >= n for op, n in expected_ops.items()),
        }
        summary = {
            "source_orders": len(source),
            "silver_live": len(live),
            "silver_deleted": len(deleted),
            "scenario_ops": {op: ops.get(op, 0) for op in ("c", "u", "d", "r")},
            "expected_ops": expected_ops,
            "exact_ops": all(ops.get(op, 0) == n for op, n in expected_ops.items()),
        }
        for name, ok in checks.items():
            log.info(json.dumps({"event": "cdc_verify_check", "check": name, "ok": ok}))
        ok = all(checks.values())
        log.info(json.dumps({"event": "cdc_verified", "ok": ok, **summary}, sort_keys=True))
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
