"""PySpark workloads, dataset amplification and correctness gate for Phase 4F (ADR-0012)."""

from __future__ import annotations

from typing import Any

PRODUCTS_PER_CHANNEL = 20
HOT_INDEX = 0
SHARE_SCALE = 1_000_000

ITEM_COLUMNS = (
    "order_item_key",
    "order_key",
    "channel",
    "currency",
    "order_ts",
    "product_key",
    "product_id",
    "seller_key",
    "seller_id",
    "qty",
    "gross_line_amount",
    "net_line_amount",
    "_dataset_run_id",
)
PRODUCT_COLUMNS = (
    "product_key",
    "seller_key",
    "channel",
    "product_id",
    "seller_id",
    "category",
    "brand",
    "_dataset_run_id",
)
W1_KEYS = ("channel", "currency", "category", "seller_id")
W2_KEYS = ("channel", "currency", "seller_id")
MONEY_TYPE = "decimal(38,2)"


def _product_index(column):
    from pyspark.sql import functions as F

    return F.regexp_extract(column, r"-p(\d{2})$", 1).cast("int")


def amplify_dataset(items, products, source_run: str, bench_run: str, factor: int, hot_share):
    """Deterministically replicate Gold order lines ``factor`` times and reassign products.

    Each replica gets new order/item keys (SHA-256 of the source key and replica number).
    A line goes to the hot product ``p00`` when ``hash(order_key) mod 1e6 < share * 1e6``,
    otherwise uniformly to the run's cold products ``p01..p19`` (by rank, so a run that
    lacks some cold products still keeps every line), matching the generator semantics
    (ADR-0005/0012). Returns ``(items, products)`` scoped to ``bench_run``; money and qty
    come from the source line. A channel without ``p00`` loses its hot lines, which the
    dataset ground-truth check rejects.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not 1 <= factor <= 200:
        raise ValueError(f"amplification factor out of range: {factor}")
    if not 0 <= float(hot_share) <= 1:
        raise ValueError(f"hot_share out of range: {hot_share}")

    dims = (
        products.where(F.col("_dataset_run_id") == source_run)
        .withColumn("_idx", _product_index(F.col("product_id")))
        .where(F.col("_idx").between(0, PRODUCTS_PER_CHANNEL - 1))
        .withColumn("_dataset_run_id", F.lit(bench_run))
    )
    pick = ("product_key", "product_id", "seller_key", "seller_id")
    hot_dims = dims.where(F.col("_idx") == HOT_INDEX).select("channel", *pick)
    cold_dims = (
        dims.where(F.col("_idx") != HOT_INDEX)
        .withColumn("_rank", F.row_number().over(Window.partitionBy("channel").orderBy("_idx")) - 1)
        .withColumn("_cold_count", F.count(F.lit(1)).over(Window.partitionBy("channel")))
    )
    cold_counts = cold_dims.select("channel", "_cold_count").distinct()

    replicas = items.where(F.col("_dataset_run_id") == source_run).crossJoin(
        items.sparkSession.range(factor).withColumnRenamed("id", "_rep")
    )
    order_key = F.sha2(F.concat_ws(":", F.col("order_key"), F.col("_rep").cast("string")), 256)
    item_key = F.sha2(F.concat_ws(":", F.col("order_item_key"), F.col("_rep").cast("string")), 256)
    threshold = int(round(float(hot_share) * SHARE_SCALE))
    bucket = F.pmod(F.xxhash64(F.col("order_key")), F.lit(SHARE_SCALE))
    assigned = (
        replicas.withColumn("order_key", order_key)
        .withColumn("order_item_key", item_key)
        .drop(*pick, "_rep")
        .join(F.broadcast(cold_counts), ["channel"], "left")
        .withColumn("_hot", bucket < threshold)
        .withColumn(
            "_rank",
            F.pmod(F.xxhash64(F.col("order_key"), F.lit("cold")), F.col("_cold_count")),
        )
    )
    hot = assigned.where("_hot").join(F.broadcast(hot_dims), ["channel"], "inner")
    cold = assigned.where("NOT _hot").join(
        F.broadcast(cold_dims.select("channel", "_rank", *pick)), ["channel", "_rank"], "inner"
    )
    joined = (
        hot.select(*ITEM_COLUMNS)
        .unionByName(cold.select(*ITEM_COLUMNS))
        .withColumn("_dataset_run_id", F.lit(bench_run))
    )
    return joined.select(*ITEM_COLUMNS), dims.select(*PRODUCT_COLUMNS)


def check_dimension_unique(products) -> None:
    """Fail when the join side has duplicate keys (salting replication must not fan out)."""
    from pyspark.sql import functions as F

    dupes = (
        products.groupBy("product_key", "_dataset_run_id").count().where(F.col("count") > 1).count()
    )
    if dupes:
        raise ValueError(f"dim_product has {dupes} duplicate (product_key, run) keys")


def _aggregates(qty="qty", net="net_line_amount", gross="gross_line_amount"):
    from pyspark.sql import functions as F

    return [
        F.count(F.lit(1)).alias("line_count"),
        F.countDistinct("order_key").alias("order_count"),
        F.sum(F.col(qty).cast("bigint")).alias("total_qty"),
        F.sum(net).cast(MONEY_TYPE).alias("net_amount"),
        F.sum(gross).cast(MONEY_TYPE).alias("gross_amount"),
    ]


def build_w1(items, products, *, broadcast: bool = False, salt: int | None = None):
    """W1: join order lines to dim_product on (product_key, run), aggregate per seller."""
    from pyspark.sql import functions as F

    dim = products.select(*PRODUCT_COLUMNS)
    if salt is None:
        if broadcast:
            dim = F.broadcast(dim)
        joined = items.join(
            dim.select(
                "product_key",
                "_dataset_run_id",
                "category",
                F.col("seller_id").alias("_dim_seller_id"),
            ),
            ["product_key", "_dataset_run_id"],
            "inner",
        )
        return (
            joined.withColumnRenamed("seller_id", "_fact_seller_id")
            .withColumnRenamed("_dim_seller_id", "seller_id")
            .groupBy(*W1_KEYS)
            .agg(*_aggregates())
        )

    if not 2 <= salt <= 64:
        raise ValueError(f"salt factor out of range: {salt}")
    salted_items = items.withColumn("_salt", F.pmod(F.xxhash64("order_key"), F.lit(salt)))
    salted_dim = dim.select(
        "product_key",
        "_dataset_run_id",
        "category",
        F.col("seller_id").alias("_dim_seller_id"),
        F.explode(F.sequence(F.lit(0).cast("bigint"), F.lit(salt - 1).cast("bigint"))).alias(
            "_salt"
        ),
    )
    joined = (
        salted_items.join(salted_dim, ["product_key", "_dataset_run_id", "_salt"], "inner")
        .withColumnRenamed("seller_id", "_fact_seller_id")
        .withColumnRenamed("_dim_seller_id", "seller_id")
    )
    # Every order maps to exactly one salt, so distinct-order counts add up across salts.
    partial = joined.groupBy(*W1_KEYS, "_salt").agg(
        F.count(F.lit(1)).alias("line_count"),
        F.countDistinct("order_key").alias("order_count"),
        F.sum(F.col("qty").cast("bigint")).alias("total_qty"),
        F.sum("net_line_amount").alias("net_amount"),
        F.sum("gross_line_amount").alias("gross_amount"),
    )
    return partial.groupBy(*W1_KEYS).agg(
        F.sum("line_count").cast("bigint").alias("line_count"),
        F.sum("order_count").cast("bigint").alias("order_count"),
        F.sum("total_qty").cast("bigint").alias("total_qty"),
        F.sum("net_amount").cast(MONEY_TYPE).alias("net_amount"),
        F.sum("gross_amount").cast(MONEY_TYPE).alias("gross_amount"),
    )


def build_w2(items):
    """W2: aggregate-only skew (no join), grouped by the hot seller key."""
    return items.groupBy(*W2_KEYS).agg(*_aggregates())


def build_workload(workload: str, items, products, variant_spec: dict[str, Any]):
    if workload == "w1_join_agg":
        return build_w1(
            items,
            products,
            broadcast=bool(variant_spec.get("broadcast")),
            salt=variant_spec.get("salt"),
        )
    if workload == "w2_agg":
        if variant_spec.get("broadcast") or variant_spec.get("salt"):
            raise ValueError("broadcast/salt variants do not apply to w2_agg")
        return build_w2(items)
    raise ValueError(f"unknown workload: {workload}")


def workload_keys(workload: str) -> tuple[str, ...]:
    return W1_KEYS if workload == "w1_join_agg" else W2_KEYS


def check_equivalence(result, reference, keys: tuple[str, ...]) -> dict[str, Any]:
    """Compare a variant result with the B0 reference without collecting rows to the driver."""
    from pyspark.sql import functions as F

    report: dict[str, Any] = {
        "schema_equal": [(f.name, f.dataType.simpleString()) for f in result.schema]
        == [(f.name, f.dataType.simpleString()) for f in reference.schema],
    }
    report["rows"] = result.count()
    report["reference_rows"] = reference.count()
    report["duplicate_keys"] = result.groupBy(*keys).count().where(F.col("count") > 1).count()
    if report["schema_equal"]:
        report["missing"] = reference.exceptAll(result).count()
        report["extra"] = result.exceptAll(reference).count()
    else:
        report["missing"] = report["extra"] = None
    report["passed"] = (
        report["schema_equal"]
        and report["rows"] == report["reference_rows"]
        and report["duplicate_keys"] == 0
        and report["missing"] == 0
        and report["extra"] == 0
    )
    return report


def check_totals(result, items) -> dict[str, Any]:
    """Exact totals per (channel, currency) must equal the input lines (ground truth)."""
    from pyspark.sql import functions as F

    expected = items.groupBy("channel", "currency").agg(
        F.count(F.lit(1)).cast("bigint").alias("line_count"),
        F.sum("net_line_amount").cast(MONEY_TYPE).alias("net_amount"),
        F.sum("gross_line_amount").cast(MONEY_TYPE).alias("gross_amount"),
        F.sum(F.col("qty").cast("bigint")).alias("total_qty"),
    )
    actual = result.groupBy("channel", "currency").agg(
        F.sum("line_count").cast("bigint").alias("line_count"),
        F.sum("net_amount").cast(MONEY_TYPE).alias("net_amount"),
        F.sum("gross_amount").cast(MONEY_TYPE).alias("gross_amount"),
        F.sum("total_qty").cast("bigint").alias("total_qty"),
    )
    mismatches = expected.exceptAll(actual).count() + actual.exceptAll(expected).count()
    return {"groups": expected.count(), "mismatches": mismatches, "passed": mismatches == 0}


def unmatched_lines(items, products) -> int:
    """Order lines without a dimension row (inner join would drop them silently)."""
    return items.join(
        products.select("product_key", "_dataset_run_id"),
        ["product_key", "_dataset_run_id"],
        "left_anti",
    ).count()


def distribution(items) -> dict[str, Any]:
    """Measured key distribution (≤ 100 keys, safe to collect)."""
    from pyspark.sql import functions as F

    per_key = items.groupBy("channel", "product_key").count().collect()
    counts = sorted(row["count"] for row in per_key)
    total = sum(counts)
    per_channel: dict[str, dict[str, Any]] = {}
    for row in per_key:
        entry = per_channel.setdefault(row["channel"], {"rows": 0, "top": 0, "keys": 0})
        entry["rows"] += row["count"]
        entry["keys"] += 1
        entry["top"] = max(entry["top"], row["count"])
    median = counts[len(counts) // 2] if counts else 0
    orders = items.select("order_key").distinct().count()
    lines_per_order = items.groupBy("order_key").count().agg(F.max("count")).first()[0]
    return {
        "rows": total,
        "distinct_orders": orders,
        "max_lines_per_order": lines_per_order,
        "keys": len(counts),
        "top_key_share": round(counts[-1] / total, 4) if total else None,
        "max_rows_per_key": counts[-1] if counts else None,
        "median_rows_per_key": median,
        "max_median_ratio": round(counts[-1] / median, 2) if median else None,
        "per_channel_top_share": {
            channel: round(v["top"] / v["rows"], 4) for channel, v in sorted(per_channel.items())
        },
        "per_channel_rows": {channel: v["rows"] for channel, v in sorted(per_channel.items())},
        "per_channel_keys": {channel: v["keys"] for channel, v in sorted(per_channel.items())},
    }
