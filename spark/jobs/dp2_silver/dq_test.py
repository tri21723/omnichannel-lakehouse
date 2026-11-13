"""Spark checks for five-entity DQ, parent/child accounting and channel-scoped keys."""

import json
import logging
import os

from spark.jobs.dp2_silver.integration import release
from spark.lib.bronze import event
from spark.lib.session import get_spark
from spark.lib.silver_entities import winners
from spark.lib.silver_normalize import POLICY
from spark.lib.silver_pipeline import candidates


def run():
    from pyspark.sql import functions as F

    spark = get_spark()
    key, version = bytes.fromhex(os.environ["SILVER_PII_KEY"]), os.environ["SILVER_PII_KEY_VERSION"]
    try:
        ts = "2026-01-01T00:00:00Z"
        customer = dict(
            customer_id="same-c",
            full_name="Fixture User",
            email="fixture@example.invalid",
            country="VN",
            updated_ts=ts,
        )
        product = dict(
            product_id="same-p", seller_id="s", title="fixture", category="fixture", updated_ts=ts
        )
        item = dict(
            line_no=1,
            product_id="same-p",
            seller_id="s",
            qty=1,
            unit_price="10.25",
            discount="0",
            product=product,
        )
        returned = dict(
            return_id="r",
            return_line_no=1,
            line_no=1,
            return_ts="2026-01-15T00:00:00Z",
            reason="fixture",
            refund_amount="10.25",
        )
        order = dict(
            order_id="same-o",
            customer_id="same-c",
            order_ts=ts,
            updated_ts=ts,
            status="paid",
            currency="USD",
            shipping_country="VN",
            total_amount="10.25",
            customer=customer,
            items=[item],
            returns=[returned],
        )
        combined = None
        for channel in ("amazon", "tiktok_shop"):
            payload = dict(order)
            if channel == "amazon":
                payload["buyer_id"] = payload.pop("customer_id")
            table = f"lh.bronze.{channel}_orders_raw"
            frames = {
                table: spark.createDataFrame(
                    [(json.dumps(payload),), (json.dumps(payload),)], "_raw string"
                )
            }
            state = dict(
                selection=dict(
                    run_id="e" * 20, run_date="2026-01-01", channel=channel, source="jsonl"
                ),
                batch_id="fixture",
                tables={table: dict(snapshot=None)},
            )
            result = candidates(spark, frames, state, "fixture", key, version)
            for entity, value in result.items():
                assert value["counts"] == dict(input=2, valid=2, rejected=0)
                assert winners(value["valid"], entity).count() == 1
                assert "_raw" not in value["rejected"].columns
            current = result["orders"]["valid"].localCheckpoint(eager=True)
            combined = current if combined is None else combined.unionByName(current)
            # Channel isolation and tie determinism with conflicting same-version business rows.
            conflict = current.withColumn("status", F.lit("cancelled"))
            from spark.lib.silver_entities import rehash

            conflicted = current.unionByName(rehash(conflict, "orders"))
            first = winners(conflicted, "orders").select("status").first()[0]
            second = (
                winners(conflicted.orderBy(F.rand(42)).repartition(3), "orders")
                .select("status")
                .first()[0]
            )
            assert first == second
        assert winners(combined, "orders").count() == 2
        event("silver_channel_dedup_passed", channels=2)
        release(spark)
        # Invalid syntax/scalar root contributes only a rejected parent, no phantom children.
        channel = "amazon"
        table = "lh.bronze.amazon_orders_raw"
        frames = {table: spark.createDataFrame([("broken",), ("42",)], "_raw string")}
        state["selection"]["channel"] = channel
        state["tables"] = {table: dict(snapshot=None)}
        result = candidates(spark, frames, state, "fixture", key, version)
        assert result["orders"]["counts"] == dict(input=2, valid=0, rejected=2)
        assert all(
            result[name]["counts"]["input"] == 0
            for name in ("customers", "products", "order_items", "returns")
        )
        release(spark)
        # A missing product, orphan return and excessive refund produce explicit DQ rejections.
        payload = json.loads(json.dumps(order))
        payload["items"][0]["product"] = None
        frames = {table: spark.createDataFrame([(json.dumps(payload),)], "_raw string")}
        result = candidates(spark, frames, state, "fixture", key, version)
        assert result["products"]["counts"]["rejected"] == 1
        assert result["orders"]["counts"]["rejected"] == 1
        assert result["order_items"]["counts"]["rejected"] == 1
        assert result["returns"]["counts"]["rejected"] == 1
        release(spark)
        payload = json.loads(json.dumps(order))
        payload["returns"][0]["refund_amount"] = "10.26"
        frames = {table: spark.createDataFrame([(json.dumps(payload),)], "_raw string")}
        result = candidates(spark, frames, state, "fixture", key, version)
        assert result["orders"]["counts"]["valid"] == 1
        assert result["returns"]["counts"]["rejected"] == 1
        event("silver_dq_cases_passed", entities=5, policy=POLICY)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
