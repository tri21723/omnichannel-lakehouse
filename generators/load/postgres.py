"""Transactional COPY staging, idempotent upsert and full-row reconciliation."""

import json
import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
from psycopg import sql

from generators.batch.dataset import verify_local

# Explicit allowlist: no manifest/config values are interpolated into SQL identifiers.
TABLES = {
    "customers": (("customer_id",), ("customer_id", "full_name", "email", "country", "updated_ts")),
    "products": (
        ("product_id",),
        ("product_id", "seller_id", "title", "category", "brand", "updated_ts"),
    ),
    "orders": (
        ("order_id",),
        (
            "order_id",
            "customer_id",
            "order_ts",
            "updated_ts",
            "status",
            "currency",
            "total_amount",
            "shipping_country",
        ),
    ),
    "order_items": (
        ("order_id", "line_no"),
        ("order_id", "line_no", "product_id", "seller_id", "qty", "unit_price", "discount"),
    ),
    "returns": (
        ("return_id", "return_line_no"),
        (
            "return_id",
            "return_line_no",
            "order_id",
            "line_no",
            "return_ts",
            "reason",
            "refund_amount",
        ),
    ),
}


def connect():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_SOURCES_DB"],
        user="generator",
        password=os.environ["GENERATOR_DB_PASSWORD"],
        connect_timeout=10,
    )


def entity_rows(root: Path, manifest: dict, table: str) -> Iterator[tuple]:
    columns = TABLES[table][1]
    for entry in manifest["channels"]["walmart"]["files"]:
        with (root / entry["path"]).open() as stream:
            for line in stream:
                order = json.loads(line)
                if table == "customers":
                    entities = [order["customer"]]
                elif table == "products":
                    entities = [item["product"] for item in order["items"]]
                elif table == "orders":
                    entities = [order]
                elif table == "order_items":
                    entities = [{**item, "order_id": order["order_id"]} for item in order["items"]]
                else:
                    entities = order["returns"]
                for entity in entities:
                    yield tuple(entity.get(column) for column in columns)


def load(root: Path, connection, *, verify_only: bool = False) -> dict:
    manifest = verify_local(root)
    if "walmart" not in manifest["channels"]:
        raise ValueError("Walmart must be selected for the relational load")
    result = {}
    with connection.transaction(), connection.cursor() as cur:
        # Serialize loaders to avoid concurrent FK/upsert races; no permanent state table needed.
        cur.execute("SELECT pg_advisory_xact_lock(482913)")
        for table, (keys, columns) in TABLES.items():
            target = sql.Identifier("walmart", table)
            stage = sql.Identifier(f"stage_{table}")
            cols = sql.SQL(", ").join(map(sql.Identifier, columns))
            pk = sql.SQL(", ").join(map(sql.Identifier, keys))
            cur.execute(
                sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                    stage, target
                )
            )
            with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(stage, cols)) as copy:
                for row in entity_rows(root, manifest, table):
                    copy.write_row(row)
            # Repeated nested entities and source duplicates collapse before upsert.
            selected = sql.SQL("SELECT DISTINCT ON ({}) {} FROM {} ORDER BY {}")
            selected = selected.format(pk, cols, stage, pk)
            changed = 0
            if not verify_only:
                assignments = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in columns
                    if c not in keys
                )
                target_row = sql.SQL(", ").join(sql.Identifier("t", c) for c in columns)
                incoming_row = sql.SQL(", ").join(sql.Identifier("excluded", c) for c in columns)
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} AS t ({}) {} ON CONFLICT ({}) DO UPDATE SET {} "
                        "WHERE ROW({}) IS DISTINCT FROM ROW({})"
                    ).format(target, cols, selected, pk, assignments, target_row, incoming_row)
                )
                changed = cur.rowcount
            predicate = sql.SQL(" AND ").join(
                sql.SQL("t.{} = s.{}").format(sql.Identifier(k), sql.Identifier(k)) for k in keys
            )
            target_row = sql.SQL(", ").join(sql.Identifier("t", c) for c in columns)
            stage_row = sql.SQL(", ").join(sql.Identifier("s", c) for c in columns)
            cur.execute(
                sql.SQL(
                    "SELECT count(*), count(*) FILTER (WHERE ROW({}) IS DISTINCT FROM ROW({})) "
                    "FROM ({}) s LEFT JOIN {} t ON {}"
                ).format(target_row, stage_row, selected, target, predicate)
            )
            expected, mismatches = cur.fetchone()
            if mismatches:
                raise ValueError(f"PostgreSQL reconciliation failed: {table}")
            result[table] = {"rows": expected, "changed": changed}
        if result["orders"]["rows"] != manifest["channels"]["walmart"]["unique_orders"]:
            raise ValueError("Walmart order count differs from manifest")
    return result
