"""CDC scenario: determinism, isolation from batch prefixes, ground truth, apply-once semantics."""

import threading
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import yaml

from generators.cdc import apply as cdc_apply
from generators.cdc.scenario import (
    DELETE_ORDER,
    INSERT_ORDER,
    UPDATE_ORDER,
    CdcConfig,
    build,
    checksum,
    normalize_ts,
)
from generators.tests.test_storage import MemoryS3

BATCH_PREFIXES = ("186ae107d0793d2ee237", "b1e64afaff0176de180a", "5f552019ee41c24af95f")
CONFIG = CdcConfig.model_validate(yaml.safe_load(open("generators/cdc.yaml")))


def test_scenario_is_deterministic_and_seed_dependent():
    assert build(CONFIG) == build(CONFIG)
    other = CONFIG.model_copy(update={"seed": 7})
    assert other.cdc_run_id != CONFIG.cdc_run_id
    assert build(other)[1]["final_checksum"] != build(CONFIG)[1]["final_checksum"]


def test_only_the_scenario_prefix_is_touched():
    steps, truth = build(CONFIG)
    prefix = truth["prefix"]
    assert prefix == f"{CONFIG.cdc_run_id}-walmart-"
    assert not prefix.startswith(BATCH_PREFIXES)
    for step in steps:
        for sql, params in step.ops + (step.concurrent or []):
            ids = [p for p in params if isinstance(p, str) and "-walmart-" in p]
            assert ids and all(i.startswith(prefix) for i in ids), (sql, params)


def test_expected_operation_counts():
    _, truth = build(CONFIG)
    c = CONFIG
    assert truth["expected_ops"] == {
        "c": c.orders + c.reinserts,
        "u": c.updated_orders * c.updates_per_order + c.multi_row_update + 2,
        "d": c.deletes + c.reinserts,
    }
    assert truth["live_orders"] == c.orders - c.deletes
    assert len(truth["deleted_orders"]) == c.deletes


def apply_in_memory(steps):
    rows = {}
    for step in steps:
        for sql, params in step.ops + (step.concurrent or []):
            if sql == INSERT_ORDER:
                order_id, customer, order_ts, updated_ts, status, amount = params
                rows[order_id] = {
                    "order_id": order_id,
                    "status": status,
                    "total_amount": f"{Decimal(amount):.2f}",
                    "updated_ts": updated_ts,
                }
            elif sql == UPDATE_ORDER:
                status, amount, updated_ts, order_id = params
                rows[order_id].update(
                    status=status, total_amount=f"{Decimal(amount):.2f}", updated_ts=updated_ts
                )
            elif sql == DELETE_ORDER:
                rows.pop(params[0])
    return rows


def test_ground_truth_equals_replaying_the_transactions():
    steps, truth = build(CONFIG)
    rows = apply_in_memory(steps)
    assert sorted(rows) == sorted(truth["final_state"])
    assert checksum(rows.values()) == truth["final_checksum"]
    concurrent = [s for s in steps if s.concurrent]
    assert len(concurrent) == 1
    first_id = concurrent[0].ops[0][1][-1]
    assert concurrent[0].concurrent[0][1][-1] == first_id  # same row: B must wait for A
    assert truth["final_state"][first_id]["status"] == "delivered"  # B committed last


def test_checksum_normalizes_timestamps():
    ts = datetime(2026, 4, 1, 0, 0, 1, tzinfo=UTC)
    assert (
        normalize_ts(ts)
        == normalize_ts("2026-04-01T00:00:01+00:00")
        == normalize_ts("2026-04-01T07:00:01+07:00")
    )


class FakeDb:
    """Rows plus row locks held until commit, so a second session really waits."""

    def __init__(self, rows=None):
        self.rows = dict(rows or {})
        self.locks: dict[str, threading.Event] = {}
        self.guard = threading.Lock()

    def connect(self):
        return FakeConn(self)


class FakeCursor:
    def __init__(self, rows, names):
        self._rows, self.description = rows, [type("D", (), {"name": n}) for n in names]

    def fetchall(self):
        return self._rows


class FakeConn:
    NAMES = [
        "order_id",
        "customer_id",
        "order_ts",
        "updated_ts",
        "status",
        "currency",
        "total_amount",
        "shipping_country",
    ]

    def __init__(self, db):
        self.db, self.held, self.pending = db, [], []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def lock(self, order_id):
        while True:
            with self.db.guard:
                event = self.db.locks.get(order_id)
                if event is None or order_id in self.held:
                    self.db.locks[order_id] = event or threading.Event()
                    if order_id not in self.held:
                        self.held.append(order_id)
                    return
            event.wait(5)

    def execute(self, sql, params=()):
        if sql.startswith("SELECT"):
            prefix = params[0]
            rows = [
                tuple(r.get(n) for n in self.NAMES)
                for k, r in sorted(self.db.rows.items())
                if k.startswith(prefix)
            ]
            return FakeCursor(rows, self.NAMES)
        if "walmart.customers" in sql:
            return None
        order_id = params[0] if sql == INSERT_ORDER or sql == DELETE_ORDER else params[-1]
        self.lock(order_id)
        self.pending.append((sql, params))
        return None

    def commit(self):
        with self.db.guard:
            for sql, params in self.pending:
                if sql == INSERT_ORDER:
                    order_id, customer, order_ts, updated_ts, status, amount = params
                    self.db.rows[order_id] = dict(
                        order_id=order_id,
                        customer_id=customer,
                        order_ts=order_ts,
                        updated_ts=updated_ts,
                        status=status,
                        currency="USD",
                        total_amount=f"{Decimal(amount):.2f}",
                        shipping_country="VN",
                    )
                elif sql == UPDATE_ORDER:
                    status, amount, updated_ts, order_id = params
                    self.db.rows[order_id].update(
                        status=status, total_amount=f"{Decimal(amount):.2f}", updated_ts=updated_ts
                    )
                else:
                    self.db.rows.pop(params[0], None)
            for order_id in self.held:
                self.db.locks.pop(order_id).set()
            self.pending, self.held = [], []


def test_apply_once_with_a_real_lock_wait_then_skip_and_refuse(tmp_path, monkeypatch):
    monkeypatch.setattr(cdc_apply, "LOCK_WAIT_S", 0.2)
    db, s3 = FakeDb(), MemoryS3()
    first = cdc_apply.apply_scenario(CONFIG, tmp_path, s3, "raw", db.connect)
    assert not first["skipped"]
    assert cdc_apply.check_source(CONFIG, db.connect)["ok"]
    truth_key, marker_key = cdc_apply.manifest_keys(CONFIG.cdc_run_id)
    assert set(s3.puts) == {truth_key, marker_key}

    again = cdc_apply.apply_scenario(CONFIG, tmp_path, s3, "raw", db.connect)
    assert again["skipped"]

    del s3.objects[marker_key]
    with pytest.raises(RuntimeError, match="interrupted"):
        cdc_apply.apply_scenario(CONFIG, tmp_path, s3, "raw", db.connect)


def test_existing_scenario_rows_are_refused(tmp_path):
    prefix = build(CONFIG)[1]["prefix"]
    db = FakeDb({f"{prefix}o000000": {"order_id": f"{prefix}o000000"}})
    with pytest.raises(RuntimeError, match="interrupted"):
        cdc_apply.apply_scenario(CONFIG, tmp_path, MemoryS3(), "raw", db.connect)
