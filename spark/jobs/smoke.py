"""Iceberg fixture with persistent pre-MERGE snapshot; reruns only verify."""

import argparse
import json
from pathlib import Path

from spark.lib.session import get_spark


def verify(spark):
    evidence = json.loads(Path("/runtime/fixture.json").read_text())
    rows = spark.sql("SELECT id, amount FROM lh.bronze._smoke_orders ORDER BY id").collect()
    assert [(r.id, str(r.amount)) for r in rows] == [(1, "15.00"), (2, "20.00"), (3, "30.00")]
    old = spark.sql(
        f"SELECT id, amount FROM lh.bronze._smoke_orders VERSION AS OF {int(evidence['snapshot'])} "
        "ORDER BY id"
    ).collect()
    assert [(r.id, str(r.amount)) for r in old] == [(1, "10.00"), (2, "20.00")]
    assert "note" in spark.table("lh.bronze._smoke_orders").columns
    assert spark.sql("SELECT * FROM lh.bronze._smoke_orders WHERE note IS NOT NULL").count() == 0
    print(json.dumps({"event": "iceberg_verified", **evidence, "rows": len(rows)}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    spark = get_spark()
    try:
        for name in ["bronze", "silver", "gold"]:
            if not args.verify:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lh.{name}")
        if args.verify or Path("/runtime/fixture.json").exists():
            verify(spark)
            return
        # Never reset an existing fixture automatically after a partial run.
        assert not spark.catalog.tableExists("lh.bronze._smoke_orders"), "Inspect partial fixture"
        spark.sql(
            """CREATE TABLE lh.bronze._smoke_orders
            (id BIGINT, order_ts TIMESTAMP, amount DECIMAL(18,2)) USING iceberg
            PARTITIONED BY (days(order_ts)) TBLPROPERTIES
            ('format-version'='2', 'write.parquet.compression-codec'='zstd')"""
        )
        spark.sql(
            """INSERT INTO lh.bronze._smoke_orders VALUES
            (1, TIMESTAMP '2026-01-01 00:00:00', 10.00),
            (2, TIMESTAMP '2026-01-02 00:00:00', 20.00)"""
        )
        snapshot = spark.sql("SELECT snapshot_id FROM lh.bronze._smoke_orders.snapshots").first()[0]
        spark.sql(
            """MERGE INTO lh.bronze._smoke_orders t USING
            (SELECT CAST(1 AS BIGINT) id, CAST(15 AS DECIMAL(18,2)) amount UNION ALL
             SELECT CAST(3 AS BIGINT), CAST(30 AS DECIMAL(18,2))) s ON t.id=s.id
            WHEN MATCHED THEN UPDATE SET t.amount=s.amount
            WHEN NOT MATCHED THEN INSERT (id, order_ts, amount)
            VALUES (s.id, TIMESTAMP '2026-01-03 00:00:00', s.amount)"""
        )
        spark.sql("ALTER TABLE lh.bronze._smoke_orders ADD COLUMN note STRING")
        Path("/runtime/fixture.json").write_text(json.dumps({"snapshot": snapshot}))
        verify(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
