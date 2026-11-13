"""Serialized publication ledger: committed snapshot references are the visibility boundary."""

import hashlib
import json
import time
from pathlib import Path

from spark.lib.bronze import save_state, snapshot, writer_lock
from spark.lib.silver_entities import FIELDS, KEYS, winners
from spark.lib.silver_normalize import POLICY


def summary(df):
    from pyspark.sql import functions as F

    columns = sorted(df.columns)
    digest = F.sha2(
        F.to_json(
            F.struct(*columns),
            {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
        ),
        256,
    )
    result = (
        df.select(digest.alias("h"))
        .agg(
            F.count("*").alias("rows"),
            F.sum(F.conv(F.substring("h", 1, 15), 16, 10).cast("decimal(38,0)")).alias("h1"),
            F.sum(F.conv(F.substring("h", 16, 15), 16, 10).cast("decimal(38,0)")).alias("h2"),
        )
        .first()
    )
    return {
        name: str(value) if value is not None else "0" for name, value in result.asDict().items()
    }


def frame_at(spark, table, evidence, schema):
    if evidence is None:
        return spark.createDataFrame([], schema)
    snap = evidence["snapshot"]
    if snap is None:
        if evidence["content"]["rows"] != "0":
            raise ValueError("Nonempty table has no snapshot")
        return spark.createDataFrame([], schema)
    return spark.read.format("iceberg").option("snapshot-id", str(snap)).load(table)


def replace(spark, table, df):
    from pyspark.sql import functions as F

    expected = summary(df)
    if not spark.catalog.tableExists(table):
        df.limit(0).writeTo(table).using("iceberg").tableProperty(
            "format-version", "2"
        ).tableProperty("write.parquet.compression-codec", "zstd").create()
    partition = None
    if "_contrib_" in table or "quarantine_" in table:
        partition = "_run_date"
    elif table.endswith(".orders"):
        partition = "days(order_ts)"
    elif table.endswith(".returns"):
        partition = "days(return_ts)"
    if (
        partition
        and "PARTITIONED BY" not in spark.sql(f"SHOW CREATE TABLE {table}").first()[0].upper()
    ):
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD {partition}")
    actual_schema = {f.name: f.dataType for f in spark.table(table).schema}
    if actual_schema != {f.name: f.dataType for f in df.schema}:
        raise ValueError("Silver schema mismatch; reviewed migration required")
    df = df.select(*spark.table(table).columns)
    current = spark.table(table)
    if summary(current) == expected and not df.exceptAll(current).limit(1).count():
        files = (
            spark.sql(
                f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes),0) "
                f"AS bytes FROM {table}.data_files"
            )
            .first()
            .asDict()
        )
        return dict(snapshot=snapshot(spark, table), content=expected, files=files)
    # One delete marker matches every old row, insert occurrences never match it.
    from pyspark.sql.types import StructField, StructType

    nullable = StructType([StructField(f.name, f.dataType, True) for f in df.schema])
    marker = spark.createDataFrame([tuple(None for _ in df.columns)], nullable)
    source = df.withColumn("_replace", F.lit(False)).unionByName(
        marker.withColumn("_replace", F.lit(True))
    )
    source.createOrReplaceTempView("silver_replacement")
    names = ", ".join("`" + c + "`" for c in df.columns)
    values = ", ".join("s.`" + c + "`" for c in df.columns)
    spark.sql(
        f"MERGE INTO {table} t USING silver_replacement s ON s._replace "
        f"WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT s._replace "
        f"THEN INSERT ({names}) VALUES ({values})"
    )
    current = spark.table(table)
    if summary(current) != expected or df.exceptAll(current).limit(1).count():
        raise ValueError("Silver read-back failed")
    files = (
        spark.sql(
            f"SELECT count(*) AS count, coalesce(sum(file_size_in_bytes),0) "
            f"AS bytes FROM {table}.data_files"
        )
        .first()
        .asDict()
    )
    return dict(snapshot=snapshot(spark, table), content=expected, files=files)


def revision(previous, pin_id, key_version, correction=False):
    if previous is None:
        return 1
    if previous["input_id"] == pin_id and previous["key_version"] == key_version:
        return previous["revision"]
    if pin_id in previous.get("superseded", []) and not correction:
        raise ValueError("Superseded input cannot reactivate without explicit correction")
    if previous["key_version"] != key_version:
        raise ValueError("PII rotation requires a separate complete backfill")
    return previous["revision"] + 1


def check_source_order(spark, previous, state, root):
    """Reject previously unseen stale pins using snapshot ancestry, not numeric IDs."""
    if previous is None:
        return
    tables = previous.get("input_tables")
    if tables is None:
        from spark.lib.silver_input import load_pin

        tables = load_pin(Path(root).parent / "inputs" / (previous["input_id"] + ".json"))["tables"]
    for table, prior in tables.items():
        old, new = prior["snapshot"], state["tables"][table]["snapshot"]
        if old == new or old is None:
            continue
        if new is None:
            raise ValueError("Stale empty source revision requires explicit correction")
        parents = {
            int(r["snapshot_id"]): r["parent_id"]
            for r in spark.sql(f"SELECT snapshot_id, parent_id FROM {table}.snapshots").collect()
        }
        cursor, seen = new, set()
        while cursor is not None and cursor != old and cursor not in seen:
            seen.add(cursor)
            cursor = parents.get(cursor)
        if cursor != old:
            raise ValueError("Source snapshot is older or has unrelated ancestry")


def publish(
    spark, result, pin_id, state, key_version, root, correction=False, fail_after=None, key_id=None
):
    from pyspark.sql import functions as F

    root = Path(root)
    with writer_lock(root):
        ledger_path = root / "ledger.json"
        old = (
            json.loads(ledger_path.read_text())
            if ledger_path.exists()
            else dict(slices={}, tables={})
        )
        select = state["selection"]
        batch = state["batch_id"]
        if key_id is None:
            raise ValueError("PII key fingerprint required")
        if any(s["policy"] != POLICY for s in old["slices"].values()):
            raise ValueError("Policy version change requires a complete backfill")
        if old.get("key_id", key_id) != key_id:
            raise ValueError("PII key changed without a complete backfill")
        previous = old["slices"].get(batch)
        rev = revision(previous, pin_id, key_version, correction)
        if previous and previous["input_id"] != pin_id and not correction:
            check_source_order(spark, previous, state, root)
        if previous and rev == previous["revision"]:
            verify(spark, old)
            return old, True
        if any(s["key_version"] != key_version for s in old["slices"].values()):
            raise ValueError("Cannot mix PII key versions in one publication")
        pending = dict(status="running", input_id=pin_id, batch_id=batch, tables={})
        save_state(root / "attempt.json", pending)
        current = json.loads(json.dumps(old))
        current["key_id"] = key_id
        started = time.monotonic()
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
            for entity, value in result.items():
                contrib = f"lh.silver._contrib_{entity}"
                quarantine = f"lh.silver.quarantine_{entity}"
                target = f"lh.silver.{entity}"
                valid = value["valid"]
                rejected = value["rejected"]
                scope = (
                    (F.col("_dataset_run_id") == select["run_id"])
                    & (F.col("channel") == select["channel"])
                    & (F.col("_run_date") == F.lit(select["run_date"]).cast("date"))
                )
                invalid = None
                for name in KEYS[entity] + [
                    "_dataset_run_id",
                    "_run_date",
                    "_input_id",
                    "_source_batch_id",
                ]:
                    condition = F.col(name).isNull() | (
                        F.length(F.trim(F.col(name).cast("string"))) == 0
                    )
                    invalid = condition if invalid is None else invalid | condition
                invalid = (
                    invalid
                    | (F.col("_source_batch_id") != batch)
                    | (F.col("_input_id") != pin_id)
                    | F.col("_source_snapshot").isNull()
                    | F.col("_source_channel").isNull()
                    | (F.col("_source_channel") != select["channel"])
                    | F.col("_policy_version").isNull()
                    | (F.col("_policy_version") != POLICY)
                )
                if valid.filter(invalid | ~F.coalesce(scope, F.lit(False))).limit(1).count():
                    raise ValueError("Invalid or out-of-scope Silver row identity")
                qinvalid = (
                    F.col("_dataset_run_id").isNull()
                    | F.col("_run_date").isNull()
                    | (F.col("_dataset_run_id") != select["run_id"])
                    | (F.col("_source_batch_id") != batch)
                    | (F.col("_input_id") != pin_id)
                    | (F.col("_source_channel") != select["channel"])
                    | F.col("_source_snapshot").isNull()
                    | (F.col("_run_date") != F.lit(select["run_date"]).cast("date"))
                )
                if rejected.filter(F.coalesce(qinvalid, F.lit(True))).limit(1).count():
                    raise ValueError("Invalid quarantine identity")
                # Rebuild exclusively from the last committed snapshots, even after partial failure.
                base = frame_at(spark, contrib, old["tables"].get(contrib), valid.schema)
                combined = base.filter(~scope).unionByName(valid).localCheckpoint(eager=True)
                collision = combined.groupBy(*KEYS[entity]).agg(
                    F.countDistinct("_dataset_run_id").alias("n")
                )
                if collision.filter("n>1").limit(1).count():
                    raise ValueError("Natural key is owned by different dataset runs")
                if (
                    combined.filter(F.col("channel").isNull() | F.col("_run_date").isNull())
                    .limit(1)
                    .count()
                ):
                    raise ValueError("NULL publication scope")
                current["tables"][contrib] = replace(spark, contrib, combined)
                pending["tables"][contrib] = current["tables"][contrib]
                save_state(root / "attempt.json", pending)
                if fail_after == "contribution":
                    raise RuntimeError("Injected contribution failure")
                # Shared entities reduce over all active date contributions.
                reduced = winners(combined, entity)
                if entity == "customers":
                    first = combined.groupBy(*KEYS[entity]).agg(
                        F.min("first_order_ts").alias("_first")
                    )
                    reduced = (
                        reduced.drop("first_order_ts")
                        .join(first, KEYS[entity])
                        .withColumnRenamed("_first", "first_order_ts")
                    )
                reduced = reduced.select(*valid.columns).localCheckpoint(eager=True)
                current["tables"][target] = replace(spark, target, reduced)
                # Quarantine stores only references/reasons; one row per rejected occurrence.
                qscope = (F.col("_dataset_run_id") == select["run_id"]) & (
                    F.col("_source_batch_id") == batch
                )
                qbase = frame_at(spark, quarantine, old["tables"].get(quarantine), rejected.schema)
                qnew = qbase.filter(~qscope).unionByName(rejected).localCheckpoint(eager=True)
                current["tables"][quarantine] = replace(spark, quarantine, qnew)
                counts = value["counts"]
                winner_count = winners(valid, entity).count()
                current.setdefault("accounting", {}).setdefault(batch, {})[entity] = {
                    **counts,
                    "winners": winner_count,
                    "duplicate_losers": counts["valid"] - winner_count,
                }
                if fail_after == "quarantine":
                    raise RuntimeError("Injected quarantine failure")
                combined.unpersist()
                reduced.unpersist()
                qnew.unpersist()
            current["slices"][batch] = dict(
                selection=select,
                input_id=pin_id,
                key_version=key_version,
                input_tables=state["tables"],
                policy=POLICY,
                revision=rev,
                superseded=(
                    previous.get("superseded", []) + [previous["input_id"]] if previous else []
                ),
            )
            current["status"] = "completed"
            verify(spark, current)
            current["duration_seconds"] = round(time.monotonic() - started, 3)
            if fail_after == "completion":
                raise RuntimeError("Injected completion failure")
            # Immutable publication archive then one atomic committed pointer.
            publication_id = hashlib.sha256(
                json.dumps(current, sort_keys=True).encode()
            ).hexdigest()
            save_state(root / f"publication-{publication_id}.json", current)
            save_state(ledger_path, current)
            pending["status"] = "completed"
            save_state(root / "attempt.json", pending)
            return current, False
        except Exception:
            pending["status"] = "failed"
            save_state(root / "attempt.json", pending)
            raise


def verify(spark, ledger):
    if ledger.get("status") != "completed":
        raise ValueError("No completed Silver publication")
    required = {
        f"lh.silver.{prefix}{entity}"
        for entity in FIELDS
        for prefix in ("", "_contrib_", "quarantine_")
    }
    if set(ledger["tables"]) != required:
        raise ValueError("Incomplete Silver table inventory")
    from pyspark.sql import functions as F

    outputs = {}
    for table, evidence in ledger["tables"].items():
        # Table names are produced by this module, but validate persisted control-plane input.
        if table not in {
            f"lh.silver.{prefix}{entity}"
            for entity in FIELDS
            for prefix in ("", "_contrib_", "quarantine_")
        }:
            raise ValueError("Unexpected Silver table in ledger")
        schema = spark.table(table).schema
        frame = frame_at(spark, table, evidence, schema)
        entity = table.rsplit(".", 1)[-1]
        base_entity = entity.removeprefix("_contrib_").removeprefix("quarantine_")
        from spark.lib.silver_entities import AUDIT

        declared = (
            AUDIT + ", _error_reason array<string>"
            if entity.startswith("quarantine_")
            else FIELDS[base_entity] + ", " + AUDIT
        )
        expected_schema = spark.createDataFrame([], declared).schema
        if {f.name: f.dataType for f in schema} != {f.name: f.dataType for f in expected_schema}:
            raise ValueError("Published Silver schema differs from contract")
        if entity.startswith("quarantine_"):
            if (
                frame.filter(F.col("_error_reason").isNull() | (F.size("_error_reason") < 1))
                .limit(1)
                .count()
            ):
                raise ValueError("Quarantine record has no error reason")
        else:
            for key in KEYS[base_entity]:
                if (
                    frame.filter(
                        F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
                    )
                    .limit(1)
                    .count()
                ):
                    raise ValueError("Published natural key is NULL or empty")
            if base_entity == "customers":
                condition = (
                    F.col("name_hash").isNull()
                    | F.col("email_hash").isNull()
                    | ~F.col("name_hash").rlike("^[0-9a-f]{64}$")
                    | ~F.col("email_hash").rlike("^[0-9a-f]{64}$")
                )
                if frame.filter(condition).limit(1).count():
                    raise ValueError("Customer PII hashes violate policy")

        if summary(frame) != evidence["content"]:
            raise ValueError("Committed Silver snapshot failed verification")
        if not any(token in table for token in ("quarantine_", "_contrib_")):
            entity = table.rsplit(".", 1)[-1]
            outputs[entity] = frame
            if frame.groupBy(*KEYS[entity]).count().filter("count>1").limit(1).count():
                raise ValueError("Silver natural key is not unique")
        if any(column in frame.columns for column in ("_raw", "email", "full_name")):
            raise ValueError("Clear PII in Silver publication")
    for entities in ledger.get("accounting", {}).values():
        for counts in entities.values():
            if (
                counts["input"] != counts["valid"] + counts["rejected"]
                or counts["valid"] != counts["winners"] + counts["duplicate_losers"]
            ):
                raise ValueError("Published accounting mismatch")

    def no_orphans(child, parent, keys):
        if child.join(parent.select(*keys).distinct(), keys, "left_anti").limit(1).count():
            raise ValueError("Published Silver relationship failed")

    no_orphans(outputs["orders"], outputs["customers"], ["channel", "customer_id"])
    no_orphans(outputs["order_items"], outputs["orders"], ["channel", "order_id"])
    no_orphans(outputs["order_items"], outputs["products"], ["channel", "product_id"])
    no_orphans(outputs["returns"], outputs["order_items"], ["channel", "order_id", "line_no"])
    amounts = (
        outputs["order_items"]
        .groupBy("channel", "order_id")
        .agg(F.sum(F.col("qty") * F.col("unit_price") - F.col("discount")).alias("_total"))
    )
    if (
        outputs["orders"]
        .join(amounts, ["channel", "order_id"], "left")
        .filter(F.col("_total").isNull() | (F.col("total_amount") != F.col("_total")))
        .limit(1)
        .count()
    ):
        raise ValueError("Published Silver monetary reconciliation failed")
    paid = outputs["order_items"].select(
        "channel",
        "order_id",
        "line_no",
        (F.col("qty") * F.col("unit_price") - F.col("discount")).alias("_paid"),
    )
    refunds = (
        outputs["returns"]
        .groupBy("channel", "order_id", "line_no")
        .agg(F.sum("refund_amount").alias("_refund"))
    )
    if (
        refunds.join(paid, ["channel", "order_id", "line_no"])
        .filter("_refund > _paid")
        .limit(1)
        .count()
    ):
        raise ValueError("Published refunds exceed paid line amount")
    if (
        outputs["returns"]
        .join(outputs["orders"].select("channel", "order_id", "order_ts"), ["channel", "order_id"])
        .filter("return_ts < order_ts")
        .limit(1)
        .count()
    ):
        raise ValueError("Published return predates its order")
