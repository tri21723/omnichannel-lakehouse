"""Spark side of Trino parity (ADR-0015): run sql/trino parity files at ledger snapshots.

Read-only. Writes /runtime/query/parity.json with the pinned snapshots, expected row counts,
warehouse windows/monetary controls and the Spark results that `docker/query/verify.py`
compares with Trino.
"""

import json
import logging
import os
from pathlib import Path

from spark.lib.benchmark_skew import write_json_atomic
from spark.lib.bronze import event
from spark.lib.dp3_input import load_gold_publication
from spark.lib.query_parity import AS_OF_DATE, PARITY_FILES, canonical_rows, for_spark, gold_tables
from spark.lib.sql_params import render

RUNTIME = Path("/runtime")
SQL_DIR = Path("/app/sql/trino")
OUT = RUNTIME / "query" / "parity.json"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    gold, gold_id = load_gold_publication(RUNTIME / "gold/publication/ledger.json")
    dp3 = json.loads((RUNTIME / "dp3/publication/ledger.json").read_text())
    warehouse = json.loads((RUNTIME / "warehouse/export/ledger.json").read_text())
    snapshots = {t.split(".")[-1]: e["snapshot"] for t, e in gold["tables"].items()}
    snapshots.update({t.split(".")[-1]: e["snapshot"] for t, e in dp3["tables"].items()})
    expected_rows = {t.split(".")[-1]: int(e["content"]["rows"]) for t, e in gold["tables"].items()}
    expected_rows.update(
        {t.split(".")[-1]: int(e["content"]["rows"]) for t, e in dp3["tables"].items()}
    )

    wh = {}
    for run_id, entry in warehouse["runs"].items():
        archived, pub_id = load_gold_publication(
            RUNTIME / "gold/publication/ledger.json", publication_id=entry["gold_publication_id"]
        )
        wh[run_id] = {
            "gold_publication_id": pub_id,
            "gold_snapshots": {
                t.split(".")[-1]: e["snapshot"] for t, e in archived["tables"].items()
            },
            "window_start": entry["window_start"],
            "window_end": entry["window_end"],
            "rows": {t.split(".")[-1]: v["rows"] for t, v in entry["tables"].items()},
            "monetary": entry["monetary"],
        }

    from spark.lib.session import get_spark
    from spark.lib.silver_publish import frame_at, summary

    spark = get_spark(app_name="query-parity")
    try:
        needed = set()
        sql_files = {name: (SQL_DIR / name).read_text(encoding="utf-8") for name in PARITY_FILES}
        for sql in sql_files.values():
            needed |= gold_tables(sql)
        for table in sorted(needed):
            ledger = gold if f"lh.gold.{table}" in gold["tables"] else dp3
            evidence = ledger["tables"][f"lh.gold.{table}"]
            frame = frame_at(spark, f"lh.gold.{table}", evidence, None)
            if summary(frame) != evidence["content"]:
                raise ValueError(f"lh.gold.{table} content differs from its ledger")
            frame.createOrReplaceTempView(f"gold_{table}")
        spark.read.jdbc(
            os.environ["BRONZE_JDBC_URL"],
            "walmart.orders",
            properties={
                "user": "spark_reader",
                "password": os.environ["SPARK_READER_DB_PASSWORD"],
                "driver": "org.postgresql.Driver",
            },
        ).createOrReplaceTempView("walmart_orders")

        results, params = {}, {}
        for run_id in sorted(gold["runs"]):
            params[run_id] = {"dataset_run_id": run_id, "as_of_date": AS_OF_DATE}
            results[run_id] = {}
            for name, sql in sql_files.items():
                rows = spark.sql(for_spark(render(sql, params[run_id]))).collect()
                results[run_id][name] = canonical_rows([tuple(r) for r in rows])
    finally:
        spark.stop()

    write_json_atomic(
        OUT,
        {
            "schema_version": 1,
            "gold_publication_id": gold_id,
            "snapshots": snapshots,
            "expected_rows": expected_rows,
            "warehouse": wh,
            "params": params,
            "spark_results": results,
        },
    )
    event(
        "query_parity_prepared",
        runs=len(results),
        files=len(PARITY_FILES),
        tables=len(needed),
    )


if __name__ == "__main__":
    main()
