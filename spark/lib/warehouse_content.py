"""Canonical full-row warehouse evidence, shared by payload and JDBC read-back."""

import hashlib
import json
from decimal import Decimal

from spark.lib.warehouse_jdbc import INSERT_ORDER, TABLE_COLUMNS


def fingerprint(rows, spec):
    digests = []
    for row in rows:
        normalized = []
        for value, (_, dtype) in zip(row, spec, strict=True):
            if value is None:
                normalized.append(None)
            elif dtype == "decimal":
                normalized.append(str(Decimal(str(value)).quantize(Decimal("0.01"))))
            elif dtype in ("int", "bigint", "timestamptz"):
                normalized.append(int(value))
            elif dtype == "bool":
                normalized.append(bool(value))
            else:
                normalized.append(str(value))
        digests.append(hashlib.sha256(json.dumps(normalized).encode()).hexdigest())
    return hashlib.sha256(json.dumps(sorted(digests)).encode()).hexdigest()


def payload_content(payload):
    return {name: fingerprint(payload[name], TABLE_COLUMNS[name]) for name in INSERT_ORDER}


def query_content(conn, run_id):
    result = {}
    for name in INSERT_ORDER:
        spec = TABLE_COLUMNS[name]
        expressions = [
            (
                f"(extract(epoch FROM {col}) * 1000000)::bigint"
                if dtype == "timestamptz"
                else f"{col}::text" if dtype == "decimal" else col
            )
            for col, dtype in spec
        ]
        # The exporter already bounds/collects each table on the driver. One JSON
        # result avoids a Py4J round trip per cell; decimals stay strings and UTC
        # timestamps stay integer microseconds, preserving exact comparison.
        ps = conn.prepareStatement(
            f"SELECT coalesce(jsonb_agg(jsonb_build_array({', '.join(expressions)})), "
            f"'[]'::jsonb)::text FROM warehouse.{name} WHERE _dataset_run_id = ?"
        )
        try:
            ps.setString(1, run_id)
            rs = ps.executeQuery()
            try:
                rs.next()
                rows = json.loads(rs.getString(1))
            finally:
                rs.close()
        finally:
            ps.close()
        result[name] = fingerprint(rows, spec)
    return result


def read_db_ledger(conn):
    stmt = conn.createStatement()
    try:
        rs = stmt.executeQuery("SELECT to_regclass('warehouse.export_state') IS NOT NULL")
        try:
            rs.next()
            exists = rs.getBoolean(1)
        finally:
            rs.close()
        if not exists:
            return None
        rs = stmt.executeQuery(
            "SELECT ledger::text FROM warehouse.export_state WHERE singleton = true"
        )
        try:
            return json.loads(rs.getString(1)) if rs.next() else None
        finally:
            rs.close()
    finally:
        stmt.close()


def write_db_ledger(conn, ledger):
    stmt = conn.createStatement()
    try:
        stmt.execute(
            "CREATE TABLE IF NOT EXISTS warehouse.export_state "
            "(singleton boolean PRIMARY KEY CHECK (singleton), ledger jsonb NOT NULL)"
        )
    finally:
        stmt.close()
    ps = conn.prepareStatement(
        "INSERT INTO warehouse.export_state VALUES (true, ?::jsonb) "
        "ON CONFLICT (singleton) DO UPDATE SET ledger = EXCLUDED.ledger"
    )
    try:
        ps.setString(1, json.dumps(ledger, sort_keys=True))
        ps.executeUpdate()
    finally:
        ps.close()
