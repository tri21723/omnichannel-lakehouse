"""Publication ledger, scoped MERGE replacement, and read-back verification for Phase 4D."""

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from spark.lib.bronze import save_state, snapshot, writer_lock
from spark.lib.dp3_entities import DP3_KEYS
from spark.lib.silver_publish import frame_at, summary

ROOT = Path("/runtime/dp3/publication")


def load_dp3_publication(
    ledger_path: str | Path = ROOT / "ledger.json",
    publication_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Load and validate completed DP3 publication ledger and optional archive."""
    ledger_path = Path(ledger_path)
    if not ledger_path.exists():
        raise FileNotFoundError(f"DP3 publication ledger not found: {ledger_path}")

    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    if ledger.get("status") != "completed":
        raise ValueError(f"DP3 publication is not completed (status={ledger.get('status')})")

    computed_id = hashlib.sha256(json.dumps(ledger, sort_keys=True).encode()).hexdigest()
    if publication_id and publication_id != computed_id:
        raise ValueError(
            f"Requested publication ID {publication_id} does not match ledger {computed_id}"
        )

    archive_path = ledger_path.parent / f"publication-{computed_id}.json"
    if archive_path.exists():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        if archive != ledger:
            raise ValueError("Corrupted publication archive: content mismatch")

    return ledger, computed_id


def replace_dp3_table(spark: Any, table: str, df: Any) -> dict[str, Any]:
    """Replace Iceberg table contents atomically using delete-marker MERGE."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField, StructType

    expected = summary(df)
    if not spark.catalog.tableExists(table):
        writer = (
            df.limit(0)
            .writeTo(table)
            .using("iceberg")
            .partitionedBy(F.col("as_of_date"))
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
        )
        writer.create()

    show_create = spark.sql(f"SHOW CREATE TABLE {table}").first()[0].upper()
    if "PARTITIONED BY" not in show_create:
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD as_of_date")

    actual_schema = {f.name: f.dataType for f in spark.table(table).schema}
    if actual_schema != {f.name: f.dataType for f in df.schema}:
        raise ValueError(f"DP3 schema mismatch on {table}; migration required")

    df = df.select(*spark.table(table).columns)
    current = spark.table(table)
    if summary(current) == expected and not df.exceptAll(current).limit(1).count():
        files = (
            spark.sql(
                f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes), 0) "
                f"AS bytes FROM {table}.data_files"
            )
            .first()
            .asDict()
        )
        return dict(snapshot=snapshot(spark, table), content=expected, files=files)

    nullable = StructType([StructField(f.name, f.dataType, True) for f in df.schema])
    marker = spark.createDataFrame([tuple(None for _ in df.columns)], nullable)
    source = df.withColumn("_replace", F.lit(False)).unionByName(
        marker.withColumn("_replace", F.lit(True))
    )
    source.createOrReplaceTempView("dp3_replacement")
    names = ", ".join("`" + c + "`" for c in df.columns)
    values = ", ".join("s.`" + c + "`" for c in df.columns)
    spark.sql(
        f"MERGE INTO {table} t USING dp3_replacement s ON s._replace "
        f"WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT s._replace "
        f"THEN INSERT ({names}) VALUES ({values})"
    )

    current = spark.table(table)
    if summary(current) != expected or df.exceptAll(current).limit(1).count():
        raise ValueError(f"DP3 read-back failed for table {table}")

    files = (
        spark.sql(
            f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes), 0) "
            f"AS bytes FROM {table}.data_files"
        )
        .first()
        .asDict()
    )
    return dict(snapshot=snapshot(spark, table), content=expected, files=files)


def publish_dp3(
    spark: Any,
    tables: dict[str, Any],
    run_id: str,
    gold_pub_id: str,
    root: str | Path = ROOT,
    fail_after: str | None = None,
    force: bool = False,
    mode: str = "full",
    as_of_dates: list[Any] | None = None,
    horizon_days: int = 14,
) -> tuple[dict[str, Any], bool]:
    """Publish DP3 feature or label tables atomically with per-run scoping and update ledger."""
    from pyspark.sql import functions as F

    root = Path(root)
    with writer_lock(root):
        ledger_path = root / "ledger.json"
        old = (
            json.loads(ledger_path.read_text())
            if ledger_path.exists()
            else dict(status="pending", runs={}, tables={})
        )

        prior_run = old.get("runs", {}).get(run_id)
        table_names = sorted(tables.keys())
        is_features_publish = any(t.startswith("feat_") for t in table_names)
        is_labels_publish = any(t.startswith("lbl_") for t in table_names)

        # Check skip condition if identical publication already completed
        if (
            not force
            and fail_after is None
            and prior_run
            and prior_run.get("gold_publication_id") == gold_pub_id
            and old.get("status") == "completed"
            and all(f"lh.gold.{t}" in prior_run.get("tables", []) for t in table_names)
        ):
            verify_dp3(spark, old, target_run_id=run_id, target_tables=table_names)
            return old, True

        pending = dict(
            status="running",
            run_id=run_id,
            gold_publication_id=gold_pub_id,
            tables=table_names,
            horizon_days=horizon_days,
            updated_at=round(time.time(), 3),
        )
        save_state(root / "attempt.json", pending)
        current = json.loads(json.dumps(old))
        started = time.monotonic()

        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")

            for name, new_df in tables.items():
                table_fqdn = f"lh.gold.{name}"

                # Validate run ID scope
                if (
                    new_df.filter(
                        F.col("_dataset_run_id").isNull() | (F.col("_dataset_run_id") != run_id)
                    )
                    .limit(1)
                    .count()
                ):
                    raise ValueError(f"Out-of-scope or null dataset run ID in {name}")

                for key in DP3_KEYS[name]:
                    if (
                        new_df.filter(
                            F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
                        )
                        .limit(1)
                        .count()
                    ):
                        raise ValueError(f"Null or empty key {key} in {name}")

                # Combine with other dataset runs (and other dates if incremental)
                prior_evidence = old.get("tables", {}).get(table_fqdn)
                base = frame_at(spark, table_fqdn, prior_evidence, new_df.schema)

                if mode == "incremental" and as_of_dates:
                    dates_str = [str(d) for d in as_of_dates]
                    other_rows = base.filter(
                        (F.col("_dataset_run_id") != run_id)
                        | (~F.col("as_of_date").cast("string").isin(dates_str))
                    )
                else:
                    other_rows = base.filter(F.col("_dataset_run_id") != run_id)

                combined = other_rows.unionByName(new_df).localCheckpoint(eager=True)

                # Validate key uniqueness within run
                dup_within_run = (
                    combined.groupBy("_dataset_run_id", *DP3_KEYS[name])
                    .count()
                    .filter(F.col("count") > 1)
                )
                if dup_within_run.limit(1).count():
                    raise ValueError(f"Duplicate key within run in {name}")

                # Ensure surrogate keys don't collide between runs
                collision = combined.groupBy(*DP3_KEYS[name]).agg(
                    F.countDistinct("_dataset_run_id").alias("_runs")
                )
                if collision.filter("_runs > 1").limit(1).count():
                    raise ValueError(f"Cross-run key collision in {name}")

                evidence = replace_dp3_table(spark, table_fqdn, combined)
                current.setdefault("tables", {})[table_fqdn] = evidence

            if fail_after == "features" and is_features_publish:
                raise RuntimeError("Injected features failure")
            if fail_after == "labels" and is_labels_publish:
                raise RuntimeError("Injected labels failure")

            current.setdefault("runs", {})
            current["runs"].setdefault(run_id, {})
            current["runs"][run_id].update(
                {
                    "gold_publication_id": gold_pub_id,
                    "updated_at": round(time.time(), 3),
                    "horizon_days": horizon_days,
                }
            )
            # Update list of published tables for this run
            published_tables = set(current["runs"][run_id].get("tables", []))
            published_tables.update([f"lh.gold.{t}" for t in table_names])
            current["runs"][run_id]["tables"] = sorted(published_tables)

            current["status"] = "completed"
            current["duration_seconds"] = round(time.monotonic() - started, 3)

            verify_dp3(spark, current, target_run_id=run_id, target_tables=table_names)

            if fail_after == "completion":
                raise RuntimeError("Injected completion failure")

            dp3_pub_id = hashlib.sha256(json.dumps(current, sort_keys=True).encode()).hexdigest()
            save_state(root / f"publication-{dp3_pub_id}.json", current)
            save_state(ledger_path, current)

            pending["status"] = "completed"
            save_state(root / "attempt.json", pending)
            return current, False

        except Exception:
            pending["status"] = "failed"
            save_state(root / "attempt.json", pending)
            raise


def verify_dp3(
    spark: Any,
    ledger: dict[str, Any],
    target_run_id: str | None = None,
    target_tables: list[str] | None = None,
) -> None:
    """Read-back verification of DP3 tables, key uniqueness, schema, and censoring."""
    if ledger.get("status") != "completed":
        raise ValueError("DP3 publication is not marked completed")

    from pyspark.sql import functions as F

    tables_to_check = target_tables or [
        t.replace("lh.gold.", "") for t in ledger.get("tables", {}).keys()
    ]

    for name in tables_to_check:
        table_fqdn = f"lh.gold.{name}" if not name.startswith("lh.gold.") else name
        base_name = name.replace("lh.gold.", "")
        if table_fqdn not in ledger.get("tables", {}):
            continue

        evidence = ledger["tables"][table_fqdn]
        schema = spark.table(table_fqdn).schema
        frame = frame_at(spark, table_fqdn, evidence, schema)

        # Content summary match
        if summary(frame) != evidence["content"]:
            raise ValueError(f"Snapshot summary mismatch on {table_fqdn}")

        # Unique keys
        for key in DP3_KEYS[base_name]:
            if (
                frame.filter(
                    F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
                )
                .limit(1)
                .count()
            ):
                raise ValueError(f"Null or empty surrogate key {key} in {base_name}")

        if frame.groupBy(*DP3_KEYS[base_name]).count().filter("count > 1").limit(1).count():
            raise ValueError(f"Duplicate surrogate key in {base_name}")

        # No clear PII
        if any(col in frame.columns for col in ("_raw", "email", "full_name")):
            raise ValueError(f"Clear PII detected in {base_name}")

        scoped = frame.filter(F.col("_dataset_run_id") == target_run_id) if target_run_id else frame
        if target_run_id and table_fqdn in ledger.get("runs", {}).get(target_run_id, {}).get(
            "tables", []
        ):
            if scoped.limit(1).count() == 0:
                raise ValueError(f"Table {table_fqdn} has 0 rows for target run {target_run_id}")

        # Table-specific checks
        if base_name == "feat_customer_daily":
            # Return rate must be between 0 and 1
            bad_rate = scoped.filter(
                (F.col("return_rate_30d") < 0) | (F.col("return_rate_30d") > 1)
            )
            if bad_rate.limit(1).count():
                raise ValueError("return_rate_30d out of range [0, 1] in feat_customer_daily")

        elif base_name == "lbl_order_return":
            # Right-censoring check: is_mature == False MUST have all label columns NULL
            immature_not_null = scoped.filter(
                (~F.col("is_mature"))
                & (
                    F.col("is_returned").isNotNull()
                    | F.col("first_return_ts").isNotNull()
                    | F.col("days_to_return").isNotNull()
                    | F.col("return_line_count").isNotNull()
                    | F.col("refund_amount").isNotNull()
                    | F.col("refund_ratio").isNotNull()
                )
            )
            if immature_not_null.limit(1).count():
                raise ValueError(
                    "Immature orders in lbl_order_return must have NULL labels "
                    "(censoring violation)"
                )

            # Mature check: is_returned in (0, 1) and 0 <= refund_ratio <= 1
            mature = scoped.filter(F.col("is_mature"))
            bad_returned = mature.filter(
                F.col("is_returned").isNull() | (~F.col("is_returned").isin(0, 1))
            )
            if bad_returned.limit(1).count():
                raise ValueError("Mature orders must have is_returned IN (0, 1)")

            bad_ratio = mature.filter(
                F.col("refund_ratio").isNotNull()
                & ((F.col("refund_ratio") < 0) | (F.col("refund_ratio") > 1))
            )
            if bad_ratio.limit(1).count():
                raise ValueError("refund_ratio out of range [0, 1] in lbl_order_return")
