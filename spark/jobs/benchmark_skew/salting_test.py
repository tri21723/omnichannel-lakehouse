"""Real-Spark correctness tests for benchmark variants (no tables written; ADR-0012 §5–6)."""

import logging
import sys
from decimal import Decimal

from spark.lib.benchmark_skew import VARIANTS
from spark.lib.benchmark_workload import (
    PRODUCT_COLUMNS,
    amplify_dataset,
    build_workload,
    check_dimension_unique,
    check_equivalence,
    check_totals,
    distribution,
    unmatched_lines,
    workload_keys,
)
from spark.lib.bronze import event
from spark.lib.session import get_spark

ITEM_DDL = (
    "order_item_key string, order_key string, channel string, currency string, "
    "order_ts timestamp, product_key string, product_id string, seller_key string, "
    "seller_id string, qty int, gross_line_amount decimal(18,2), "
    "net_line_amount decimal(18,2), _dataset_run_id string"
)
PRODUCT_DDL = (
    "product_key string, seller_key string, channel string, product_id string, "
    "seller_id string, category string, brand string, _dataset_run_id string"
)
RUN = "fixture"


def fixture(spark):
    from datetime import datetime

    ts = datetime(2026, 1, 1, 12)
    products = [
        ("hot", "sk0", "amazon", "r-amazon-p00", "amazon-s00", "electronics", "b", RUN),
        ("cold1", "sk1", "amazon", "r-amazon-p01", "amazon-s01", "home", "b", RUN),
        ("cold2", "sk2", "lazada", "r-lazada-p02", "lazada-s02", "fashion", "b", RUN),
    ]
    items = []
    # 400 hot lines (one per order), 30 cold lines, one order with three lines on two
    # products, and one line whose product is missing from the dimension (NULL key).
    for i in range(400):
        items.append(
            (
                f"h{i}",
                f"oh{i}",
                "amazon",
                "USD",
                ts,
                "hot",
                "x",
                "x",
                "x",
                1,
                Decimal("1.10"),
                Decimal("1.00"),
                RUN,
            )
        )
    for i in range(30):
        items.append(
            (
                f"c{i}",
                f"oc{i}",
                "lazada",
                "VND",
                ts,
                "cold2",
                "x",
                "x",
                "x",
                2,
                Decimal("25000.00"),
                Decimal("24000.00"),
                RUN,
            )
        )
    for line, key in enumerate(("hot", "cold1", "cold1")):
        items.append(
            (
                f"m{line}",
                "multi",
                "amazon",
                "USD",
                ts,
                key,
                "x",
                "x",
                "x",
                3,
                Decimal("2.00"),
                Decimal("2.00"),
                RUN,
            )
        )
    items.append(
        (
            "n0",
            "onull",
            "amazon",
            "USD",
            ts,
            None,
            "x",
            "x",
            "x",
            1,
            Decimal("9.99"),
            Decimal("9.99"),
            RUN,
        )
    )
    return (
        spark.createDataFrame(items, ITEM_DDL),
        spark.createDataFrame(products, PRODUCT_DDL),
    )


def check_variants_equal_baseline(spark, items, products):
    for workload, variants in (
        ("w1_join_agg", ("B2", "B3", "B1T")),
        ("w2_agg", ("B1T",)),
    ):
        reference = build_workload(workload, items, products, VARIANTS["B0"]).cache()
        for variant in variants:
            result = build_workload(workload, items, products, VARIANTS[variant])
            report = check_equivalence(result, reference, workload_keys(workload))
            assert report["passed"], (workload, variant, report)
        if workload == "w1_join_agg":
            row = reference.where("seller_id = 'amazon-s01'").first()
            assert row.line_count == 2 and row.order_count == 1, row
            hot = reference.where("seller_id = 'amazon-s00'").first()
            assert hot.line_count == 401 and hot.order_count == 401, hot
            assert reference.schema["net_amount"].dataType.simpleString() == "decimal(38,2)"


def check_salt_counts_distinct_orders(spark, items, products):
    for salt in (2, 7, 64):
        spec = {"salt": salt}
        result = build_workload("w1_join_agg", items, products, spec)
        amazon = {r.seller_id: r for r in result.where("channel = 'amazon'").collect()}
        assert amazon["amazon-s00"].order_count == 401, (salt, amazon)
        assert amazon["amazon-s01"].order_count == 1, (salt, amazon)
        assert sum(r.line_count for r in result.collect()) == 433, salt


def check_unmatched_and_totals(spark, items, products):
    assert unmatched_lines(items, products) == 1
    matched = items.where("product_key IS NOT NULL")
    result = build_workload("w1_join_agg", items, products, {"salt": 8})
    assert check_totals(result, matched)["passed"]
    assert not check_totals(result, items)["passed"], "NULL-key line must not vanish silently"
    w2 = build_workload("w2_agg", items, products, VARIANTS["B0"])
    assert check_totals(w2, items)["passed"]


def check_dimension_fanout_detected(spark, products):
    check_dimension_unique(products)
    try:
        check_dimension_unique(products.unionByName(products.limit(1)))
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:
        raise AssertionError("duplicate dimension key accepted")


def check_gate_detects_differences(spark, items, products):
    from pyspark.sql import functions as F

    reference = build_workload("w1_join_agg", items, products, VARIANTS["B0"]).cache()
    keys = workload_keys("w1_join_agg")
    changed = reference.withColumn(
        "net_amount",
        F.when(F.col("seller_id") == "amazon-s00", F.col("net_amount") + F.lit(Decimal("0.01")))
        .otherwise(F.col("net_amount"))
        .cast("decimal(38,2)"),
    )
    assert not check_equivalence(changed, reference, keys)["passed"]
    duplicated = reference.unionByName(reference.limit(1))
    report = check_equivalence(duplicated, reference, keys)
    assert not report["passed"] and report["duplicate_keys"] == 1
    widened = reference.withColumn("line_count", F.col("line_count").cast("decimal(20,0)"))
    assert not check_equivalence(widened, reference, keys)["schema_equal"]


def check_amplification(spark):
    rows, dims = [], []
    from datetime import datetime

    for channel in ("amazon", "lazada"):
        for p in range(20):
            dims.append(
                (
                    f"{channel}-pk{p}",
                    f"sk{p}",
                    channel,
                    f"src-{channel}-p{p:02d}",
                    f"{channel}-s{p:02d}",
                    "home",
                    "b",
                    "src",
                )
            )
        for i in range(500):
            rows.append(
                (
                    f"{channel}-i{i}",
                    f"{channel}-o{i}",
                    channel,
                    "USD",
                    datetime(2026, 1, 1),
                    f"{channel}-pk{i % 20}",
                    "x",
                    "x",
                    "x",
                    1,
                    Decimal("1.00"),
                    Decimal("1.00"),
                    "src",
                )
            )
    items = spark.createDataFrame(rows, ITEM_DDL)
    products = spark.createDataFrame(dims, PRODUCT_DDL)
    first, dim1 = amplify_dataset(items, products, "src", "bench_x", 4, 0.8)
    second, _ = amplify_dataset(items, products, "src", "bench_x", 4, 0.8)
    assert first.exceptAll(second).count() == 0 and second.exceptAll(first).count() == 0
    assert set(dim1.columns) == set(PRODUCT_COLUMNS)
    dist = distribution(first)
    assert dist["rows"] == 4000 and dist["distinct_orders"] == 4000, dist
    for share in dist["per_channel_top_share"].values():
        assert abs(share - 0.8) < 0.05, dist
    assert unmatched_lines(first, dim1) == 0
    control, _ = amplify_dataset(items, products, "src", "bench_c", 2, 0.0)
    assert control.where("product_id LIKE '%-p00'").count() == 0
    # A run with only a few cold products (small fixture) keeps every line.
    sparse = products.where("product_id RLIKE '-p(00|01|05)$'")
    kept, sparse_dims = amplify_dataset(items, sparse, "src", "bench_s", 3, 0.5)
    sparse_dist = distribution(kept)
    assert sparse_dist["rows"] == 3000 and set(sparse_dist["per_channel_keys"].values()) == {3}
    assert unmatched_lines(kept, sparse_dims) == 0
    totals = first.groupBy().sum("net_line_amount").first()[0]
    assert totals == Decimal("4000.00"), totals
    try:
        amplify_dataset(items, products, "src", "bench_x", 0, 0.5)
    except ValueError:
        pass
    else:
        raise AssertionError("factor 0 accepted")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark(app_name="benchmark-salting-test")
    try:
        items, products = fixture(spark)
        cases = (
            lambda: check_variants_equal_baseline(spark, items, products),
            lambda: check_salt_counts_distinct_orders(spark, items, products),
            lambda: check_unmatched_and_totals(spark, items, products),
            lambda: check_dimension_fanout_detected(spark, products),
            lambda: check_gate_detects_differences(spark, items, products),
            lambda: check_amplification(spark),
        )
        for case in cases:
            case()
    finally:
        spark.stop()
    event("benchmark_salting_tests_passed", cases=len(cases))
    return 0


if __name__ == "__main__":
    sys.exit(main())
