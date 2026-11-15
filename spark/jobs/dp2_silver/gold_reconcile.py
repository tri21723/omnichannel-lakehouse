"""Three-way and dimension/fact reconciliation for Gold star schema against Silver."""

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from spark.lib.bronze import event
from spark.lib.gold_entities import GOLD_KEYS
from spark.lib.session import get_spark

GOLD_ROOT = Path("/runtime/gold/publication")
SILVER_ROOT = Path("/runtime/silver/publication")


def reconcile_run(spark: Any, run_id: str) -> dict[str, Any]:
    from pyspark.sql import functions as F

    gold_ledger_file = GOLD_ROOT / "ledger.json"
    if not gold_ledger_file.exists():
        raise FileNotFoundError(f"Gold ledger not found at {gold_ledger_file}")
    gold_ledger = json.loads(gold_ledger_file.read_text(encoding="utf-8"))
    if gold_ledger.get("status") != "completed":
        raise ValueError(f"Gold ledger status is {gold_ledger.get('status')}, expected completed")

    silver_ledger_file = SILVER_ROOT / "ledger.json"
    if not silver_ledger_file.exists():
        raise FileNotFoundError(f"Silver ledger not found at {silver_ledger_file}")
    silver_ledger = json.loads(silver_ledger_file.read_text(encoding="utf-8"))
    if silver_ledger.get("status") != "completed":
        raise ValueError(
            f"Silver ledger status is {silver_ledger.get('status')}, expected completed"
        )

    # Load Gold tables filtered by run_id
    gold_tables = {}
    for short in GOLD_KEYS:
        table_name = f"lh.gold.{short}"
        df = spark.table(table_name).filter(F.col("_dataset_run_id") == run_id)
        gold_tables[short] = df

    # Load Silver tables filtered by run_id
    silver_tables = {}
    for entity in ("orders", "order_items", "customers", "products", "returns"):
        full_name = f"lh.silver.{entity}"
        df = spark.table(full_name).filter(F.col("_dataset_run_id") == run_id)
        silver_tables[entity] = df

    # 1. Orders reconciliation
    gold_orders = gold_tables["fact_orders"]
    silver_orders = silver_tables["orders"]

    gold_order_count = gold_orders.count()
    silver_order_count = silver_orders.count()
    if gold_order_count != silver_order_count:
        raise ValueError(
            f"fact_orders count ({gold_order_count}) != silver.orders count ({silver_order_count})"
        )

    # Natural keys bijection
    gk_orders = gold_orders.select("channel", "order_id")
    sk_orders = silver_orders.select("channel", "order_id")
    if gk_orders.exceptAll(sk_orders).count() > 0 or sk_orders.exceptAll(gk_orders).count() > 0:
        raise ValueError("fact_orders natural keys (channel, order_id) mismatch with silver.orders")

    # Monetary total reconciliation by (channel, currency)
    gold_amounts = (
        gold_orders.groupBy("channel", "currency")
        .agg(F.sum("net_amount").alias("net_amount"))
        .localCheckpoint(eager=True)
    )
    silver_amounts = (
        silver_orders.groupBy("channel", "currency")
        .agg(F.sum("total_amount").alias("net_amount"))
        .localCheckpoint(eager=True)
    )
    if (
        gold_amounts.exceptAll(silver_amounts).count() > 0
        or silver_amounts.exceptAll(gold_amounts).count() > 0
    ):
        raise ValueError("fact_orders net_amount mismatch with silver.orders total_amount")

    # 2. Order items reconciliation
    gold_items = gold_tables["fact_order_items"]
    silver_items = silver_tables["order_items"]

    gold_item_count = gold_items.count()
    silver_item_count = silver_items.count()
    if gold_item_count != silver_item_count:
        msg = (
            f"fact_order_items count ({gold_item_count}) != "
            f"silver.order_items count ({silver_item_count})"
        )
        raise ValueError(msg)

    gk_items = gold_items.select("channel", "order_id", "line_no")
    sk_items = silver_items.select("channel", "order_id", "line_no")
    if gk_items.exceptAll(sk_items).count() > 0 or sk_items.exceptAll(gk_items).count() > 0:
        raise ValueError("fact_order_items natural keys mismatch with silver.order_items")

    # Item line net amount sum vs order net amount
    item_sums = (
        gold_items.groupBy("channel", "currency")
        .agg(F.sum("net_line_amount").alias("net_amount"))
        .localCheckpoint(eager=True)
    )
    if (
        item_sums.exceptAll(gold_amounts).count() > 0
        or gold_amounts.exceptAll(item_sums).count() > 0
    ):
        raise ValueError("fact_order_items net_line_amount sum != fact_orders net_amount")

    # 3. Returns reconciliation
    gold_returns = gold_tables["fact_returns"]
    silver_returns = silver_tables["returns"]

    gold_return_count = gold_returns.count()
    silver_return_count = silver_returns.count()
    if gold_return_count != silver_return_count:
        msg = (
            f"fact_returns count ({gold_return_count}) != "
            f"silver.returns count ({silver_return_count})"
        )
        raise ValueError(msg)

    gk_returns = gold_returns.select("channel", "return_id", "return_line_no")
    sk_returns = silver_returns.select("channel", "return_id", "return_line_no")
    if gk_returns.exceptAll(sk_returns).count() > 0 or sk_returns.exceptAll(gk_returns).count() > 0:
        raise ValueError("fact_returns natural keys mismatch with silver.returns")

    gold_refunds = (
        gold_returns.groupBy("channel", "currency")
        .agg(F.sum("refund_amount").alias("refund_amount"))
        .localCheckpoint(eager=True)
    )
    silver_refunds = (
        silver_returns.join(
            silver_orders.select("channel", "order_id", "currency"), ["channel", "order_id"]
        )
        .groupBy("channel", "currency")
        .agg(F.sum("refund_amount").alias("refund_amount"))
        .localCheckpoint(eager=True)
    )
    if (
        gold_refunds.exceptAll(silver_refunds).count() > 0
        or silver_refunds.exceptAll(gold_refunds).count() > 0
    ):
        raise ValueError("fact_returns refund_amount mismatch with silver.returns refund_amount")

    # 4. Customers reconciliation & PII verification
    gold_cust = gold_tables["dim_customer"]
    silver_cust = silver_tables["customers"]
    if gold_cust.count() != silver_cust.count():
        raise ValueError("dim_customer count mismatch with silver.customers")

    # PII columns check: dim_customer must NOT contain full_name or email
    cust_cols = set(gold_cust.columns)
    for forbidden in ("full_name", "email", "name", "phone", "address"):
        if forbidden in cust_cols:
            raise ValueError(f"dim_customer contains forbidden PII column: {forbidden}")

    # Hashes must match
    gc_hashes = gold_cust.select("channel", "customer_id", "name_hash", "email_hash")
    sc_hashes = silver_cust.select("channel", "customer_id", "name_hash", "email_hash")
    if gc_hashes.exceptAll(sc_hashes).count() > 0 or sc_hashes.exceptAll(gc_hashes).count() > 0:
        raise ValueError("dim_customer SHA-256 hashes mismatch with silver.customers")

    # 5. Products reconciliation
    gold_prod = gold_tables["dim_product"]
    silver_prod = silver_tables["products"]
    if gold_prod.count() != silver_prod.count():
        raise ValueError("dim_product count mismatch with silver.products")

    # 6. Sellers reconciliation: count distinct (channel, seller_id) from products & items
    gold_sellers = gold_tables["dim_seller"]
    expected_sellers = (
        silver_prod.select("channel", "seller_id")
        .unionByName(silver_items.select("channel", "seller_id"))
        .distinct()
    )
    if gold_sellers.count() != expected_sellers.count():
        raise ValueError("dim_seller count mismatch with distinct (channel, seller_id)")

    # 7. Channels: exactly 5
    gold_chan = gold_tables["dim_channel"]
    if gold_chan.count() != 5:
        raise ValueError(f"dim_channel count ({gold_chan.count()}) != 5")

    # 8. Referential Integrity
    # fact_orders -> dim_customer, dim_channel, dim_date
    if gold_orders.join(gold_cust, "customer_key", "left_anti").count() > 0:
        raise ValueError("fact_orders -> dim_customer orphan foreign keys")
    if gold_orders.join(gold_chan, "channel_key", "left_anti").count() > 0:
        raise ValueError("fact_orders -> dim_channel orphan foreign keys")
    if (
        gold_orders.join(
            gold_tables["dim_date"].withColumnRenamed("date_key", "order_date_key"),
            "order_date_key",
            "left_anti",
        ).count()
        > 0
    ):
        raise ValueError("fact_orders -> dim_date orphan foreign keys")

    # fact_order_items -> fact_orders, dim_product, dim_seller
    if gold_items.join(gold_orders, "order_key", "left_anti").count() > 0:
        raise ValueError("fact_order_items -> fact_orders orphan foreign keys")
    if gold_items.join(gold_prod, "product_key", "left_anti").count() > 0:
        raise ValueError("fact_order_items -> dim_product orphan foreign keys")
    if gold_items.join(gold_sellers, "seller_key", "left_anti").count() > 0:
        raise ValueError("fact_order_items -> dim_seller orphan foreign keys")

    # fact_returns -> fact_order_items, dim_date
    if gold_returns.join(gold_items, "order_item_key", "left_anti").count() > 0:
        raise ValueError("fact_returns -> fact_order_items orphan foreign keys")
    if (
        gold_returns.join(
            gold_tables["dim_date"].withColumnRenamed("date_key", "return_date_key"),
            "return_date_key",
            "left_anti",
        ).count()
        > 0
    ):
        raise ValueError("fact_returns -> dim_date orphan foreign keys")

    counts = {name: df.count() for name, df in gold_tables.items()}
    event("gold_reconciled", run_id=run_id, counts=counts)
    return counts


def run() -> None:
    parser = argparse.ArgumentParser(description="Reconcile Gold star schema against Silver")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = get_spark()
    try:
        reconcile_run(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
