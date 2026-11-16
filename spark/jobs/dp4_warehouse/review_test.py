"""Real JDBC post-commit recovery and nonmonetary tampering regression."""

import logging
from datetime import date

from pyspark.sql import functions as F

from spark.lib.bronze import event, save_state, writer_lock
from spark.lib.bronze_jdbc import rollback_close
from spark.lib.session import get_spark
from spark.lib.warehouse_input import read_gold_star_schema
from spark.lib.warehouse_jdbc import writer_connection
from spark.lib.warehouse_publish import (
    ROOT,
    compute_export_id,
    load_warehouse_ledger,
    publish_warehouse,
    verify_warehouse_ledger,
)


def run():
    spark = get_spark()
    run_id = "review-export-regression"
    try:
        frames, *_ = read_gold_star_schema(spark, "186ae107d0793d2ee237")
        empty = {name: df.limit(0) for name, df in frames.items()}
        fixture = dict(empty)
        fixture["dim_channel"] = (
            frames["dim_channel"].limit(1).withColumn("_dataset_run_id", F.lit(run_id))
        )

        def publish(day, data=fixture, **kw):
            return publish_warehouse(
                spark,
                data,
                run_id,
                "review-fixture",
                date(2026, 1, day),
                date(2025, 12, 3),
                date(2026, 1, day + 1),
                **kw,
            )

        publish(1, force=True)
        before = (ROOT / "ledger.json").read_bytes()
        try:
            publish(2, fail_after="db_commit")
        except RuntimeError:
            pass
        else:
            raise AssertionError("Post-commit injection did not fail")
        assert before == (ROOT / "ledger.json").read_bytes()
        ledger, _ = load_warehouse_ledger(spark=spark)
        assert ledger["runs"][run_id]["run_date"] == "2026-01-02"
        verify_warehouse_ledger(spark, ledger, run_id)
        (ROOT / "ledger.json").write_text("{broken", encoding="utf-8")
        _, skipped = publish(2)
        assert skipped
        conn = writer_connection(spark)
        try:
            ps = conn.prepareStatement(
                "UPDATE warehouse.dim_channel SET channel_name = 'changed' "
                "WHERE _dataset_run_id = ?"
            )
            try:
                ps.setString(1, run_id)
                ps.executeUpdate()
                conn.commit()
            finally:
                ps.close()
        finally:
            rollback_close(conn)
        try:
            verify_warehouse_ledger(spark, ledger, run_id)
        except ValueError as exc:
            assert "full-row content" in str(exc)
        else:
            raise AssertionError("Nonmonetary corruption was accepted")
        finally:
            publish(2, empty, force=True)
        # Remove only this reserved fixture's completion after its rows are retracted.
        from spark.lib.warehouse_content import read_db_ledger, write_db_ledger

        with writer_lock(ROOT):
            conn = writer_connection(spark)
            try:
                ledger = read_db_ledger(conn)
                ledger["runs"].pop(run_id)
                write_db_ledger(conn, ledger)
                conn.commit()
            finally:
                rollback_close(conn)
            save_state(ROOT / f"export-{compute_export_id(ledger)}.json", ledger)
            save_state(ROOT / "ledger.json", ledger)
        verify_warehouse_ledger(spark, ledger)
        event("warehouse_review_regressions_passed", cases=2)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
