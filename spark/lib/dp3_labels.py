"""Return label generation with horizon windows and maturity right-censoring."""

from datetime import date, datetime, time, timedelta
from typing import Any

from spark.lib.dp3_entities import (
    POLICY,
    rehash_dp3,
)


def _to_date(val: str | date) -> date:
    if isinstance(val, str):
        return date.fromisoformat(val)
    return val


def compute_order_return_labels(
    spark: Any,
    frames: dict[str, Any],
    run_id: str,
    pub_id: str,
    as_of_date: str | date,
    horizon_days: int = 14,
) -> Any:
    """Compute point-in-time lbl_order_return for a single evaluation date."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        IntegerType,
        TimestampType,
    )

    eval_date = _to_date(as_of_date)
    next_date = eval_date + timedelta(days=1)
    as_of_ts = datetime.combine(next_date, time(0, 0, 0))
    date_key = int(eval_date.strftime("%Y%m%d"))

    # Active orders placed strictly before as_of_ts
    orders = frames["fact_orders"].filter(
        (F.col("order_ts") < F.lit(as_of_ts)) & (~F.coalesce(F.col("is_deleted"), F.lit(False)))
    )

    # Valid returns occurring:
    # 1. return_ts >= order_ts
    # 2. return_ts < as_of_ts (prior to cutoff)
    # 3. return_ts <= order_ts + INTERVAL horizon_days DAYS (within observation horizon)
    returns = frames["fact_returns"].join(
        orders.select("order_key", "order_ts"),
        on="order_key",
        how="inner",
    )
    valid_returns = returns.filter(
        (F.col("return_ts") >= F.col("order_ts"))
        & (F.col("return_ts") < F.lit(as_of_ts))
        & (F.col("return_ts") <= F.col("order_ts") + F.expr(f"INTERVAL {horizon_days} DAYS"))
    )

    # Aggregate returns to order grain (order_key) to avoid Cartesian fan-out
    ret_aggs = valid_returns.groupBy("order_key").agg(
        F.min("return_ts").alias("first_return_ts"),
        F.count("return_key").cast(IntegerType()).alias("return_line_count"),
        F.coalesce(F.sum("refund_amount"), F.lit(0))
        .cast(DecimalType(18, 2))
        .alias("horizon_refund_amount"),
    )

    # Join orders with aggregated returns
    joined = orders.join(ret_aggs, on="order_key", how="left")

    # Maturity condition: order_ts + INTERVAL horizon_days DAYS <= as_of_ts
    is_mature_col = (F.col("order_ts") + F.expr(f"INTERVAL {horizon_days} DAYS")) <= F.lit(as_of_ts)

    result = joined.select(
        F.sha2(
            F.concat_ws(
                ":",
                F.lit(run_id),
                F.col("channel"),
                F.col("order_id"),
                F.lit(str(horizon_days)),
                F.lit(eval_date.isoformat()),
            ),
            256,
        ).alias("order_label_key"),
        "order_key",
        "customer_key",
        "channel_key",
        "order_date_key",
        F.lit(date_key).cast(IntegerType()).alias("as_of_date_key"),
        "channel",
        "order_id",
        "order_ts",
        F.lit(eval_date).cast(DateType()).alias("as_of_date"),
        F.lit(as_of_ts).cast(TimestampType()).alias("as_of_ts"),
        F.lit(horizon_days).cast(IntegerType()).alias("horizon_days"),
        "currency",
        F.col("net_amount").cast(DecimalType(18, 2)).alias("net_amount"),
        is_mature_col.cast(BooleanType()).alias("is_mature"),
        # Right-censored labels: if not mature, set to NULL; if mature, resolve
        F.when(
            ~is_mature_col,
            F.lit(None).cast(IntegerType()),
        )
        .when(
            F.coalesce(F.col("return_line_count"), F.lit(0)) > 0,
            F.lit(1).cast(IntegerType()),
        )
        .otherwise(F.lit(0).cast(IntegerType()))
        .alias("is_returned"),
        F.when(
            ~is_mature_col,
            F.lit(None).cast(TimestampType()),
        )
        .when(
            F.coalesce(F.col("return_line_count"), F.lit(0)) > 0,
            F.col("first_return_ts"),
        )
        .otherwise(F.lit(None).cast(TimestampType()))
        .alias("first_return_ts"),
        F.when(
            ~is_mature_col,
            F.lit(None).cast(IntegerType()),
        )
        .when(
            F.coalesce(F.col("return_line_count"), F.lit(0)) > 0,
            F.datediff(
                F.to_date(F.col("first_return_ts")),
                F.to_date(F.col("order_ts")),
            ).cast(IntegerType()),
        )
        .otherwise(F.lit(None).cast(IntegerType()))
        .alias("days_to_return"),
        F.when(
            ~is_mature_col,
            F.lit(None).cast(IntegerType()),
        )
        .otherwise(F.coalesce(F.col("return_line_count"), F.lit(0)).cast(IntegerType()))
        .alias("return_line_count"),
        F.when(
            ~is_mature_col,
            F.lit(None).cast(DecimalType(18, 2)),
        )
        .otherwise(F.coalesce(F.col("horizon_refund_amount"), F.lit(0)).cast(DecimalType(18, 2)))
        .alias("refund_amount"),
        F.when(
            ~is_mature_col,
            F.lit(None).cast(DecimalType(18, 4)),
        )
        .when(
            F.coalesce(F.col("net_amount"), F.lit(0)) > 0,
            (F.coalesce(F.col("horizon_refund_amount"), F.lit(0)) / F.col("net_amount")).cast(
                DecimalType(18, 4)
            ),
        )
        .otherwise(F.lit(0).cast(DecimalType(18, 4)))
        .alias("refund_ratio"),
        F.lit(run_id).alias("_dataset_run_id"),
        F.lit(pub_id).alias("_gold_publication_id"),
        F.lit(POLICY).alias("_policy_version"),
    )

    return rehash_dp3(result, "lbl_order_return")


def build_return_labels(
    spark: Any,
    frames: dict[str, Any],
    run_id: str,
    pub_id: str,
    as_of_dates: list[str | date],
    horizon_days: int = 14,
) -> dict[str, Any]:
    """Build lbl_order_return across multiple evaluation dates."""
    label_frames = []

    for dt in as_of_dates:
        lbl_df = compute_order_return_labels(
            spark, frames, run_id, pub_id, dt, horizon_days=horizon_days
        )
        label_frames.append(lbl_df)

    combined_lbl = label_frames[0]
    for df in label_frames[1:]:
        combined_lbl = combined_lbl.unionByName(df)

    return {"lbl_order_return": combined_lbl}
