"""Read-only business raw contract checks against the default reference dataset."""

import argparse
import logging

from spark.lib.bronze import event
from spark.lib.bronze_input import CHANNELS, identity
from spark.lib.session import get_spark


def run():
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    identity(args.run_id, "2026-01-01", "walmart", "jsonl")
    spark = get_spark()
    try:
        for channel in CHANNELS:
            suffix = "_export" if channel == "walmart" else ""
            df = spark.table(f"lh.bronze.{channel}_orders{suffix}_raw").filter(
                F.col("_dataset_run_id") == args.run_id
            )
            assert df.count() == 3120
            assert df.select(F.get_json_object("_raw", "$.order_id")).distinct().count() == 3000
            assert df.filter(F.col("_parse_status") != "valid").count() == 0
            if channel == "amazon":
                before = df.filter(F.col("_run_date") == "2026-01-01")
                after = df.filter(F.col("_run_date") == "2026-01-03")
                assert (
                    before.filter(F.get_json_object("_raw", "$.buyer_id").isNotNull()).count()
                    == 1040
                )
                assert (
                    before.filter(F.get_json_object("_raw", "$.customer_id").isNotNull()).count()
                    == 0
                )
                assert (
                    after.filter(F.get_json_object("_raw", "$.customer_id").isNotNull()).count()
                    == 1040
                )
                assert (
                    after.filter(F.get_json_object("_raw", "$.fulfillment_type") == "FBA").count()
                    == 1040
                )
            if channel == "shopee":
                assert (
                    df.filter(
                        F.get_json_object("_raw", "$.order_ts").rlike(
                            r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\+0700$"
                        )
                    ).count()
                    == 3120
                )
            if channel == "lazada":
                assert (
                    df.filter(
                        F.get_json_object("_raw", "$.items[0].product.brand").isNull()
                    ).count()
                    == 3120
                )
                assert df.filter(F.get_json_object("_raw", "$.currency") == "VND").count() == 3120
            event(
                "bronze_channel_contract_passed",
                channel=channel,
                physical_rows=3120,
                unique_orders=3000,
                extra_rows=120,
            )
        jdbc = spark.table("lh.bronze.walmart_orders_raw").filter(
            F.col("_dataset_run_id") == args.run_id
        )
        assert jdbc.count() == 3000
        assert jdbc.select(F.get_json_object("_raw", "$.order_id")).distinct().count() == 3000
        event("bronze_raw_contract_passed", jsonl_rows=15600, jdbc_orders=3000)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
