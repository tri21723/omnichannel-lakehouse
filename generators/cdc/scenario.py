"""Pure scenario model: transactions to run and the resulting ground truth (no I/O, no clock).

Every id starts with `<cdc_run_id>-walmart-`, never a batch dataset prefix (ADR-0017 §6).
Timestamps are fixed offsets from `base_ts`, so the final state is reproducible.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from generators.common.config import canonical_json
from generators.common.orders import stable_seed

CDC_CONTRACT = "cdc-v1"
STATUSES = ("paid", "packed", "shipped", "delivered", "cancelled")


class CdcConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    seed: int = Field(default=42, ge=0)
    base_ts: datetime = datetime(2026, 4, 1, tzinfo=UTC)
    customers: int = Field(default=5, ge=1, le=1000)
    orders: int = Field(default=40, ge=10, le=100_000)
    updated_orders: int = Field(default=20, ge=1)
    updates_per_order: int = Field(default=3, ge=1, le=20)
    multi_row_update: int = Field(default=15, ge=1)
    deletes: int = Field(default=5, ge=1)
    reinserts: int = Field(default=3, ge=1)

    @property
    def cdc_run_id(self) -> str:
        payload = {"contract": CDC_CONTRACT, "config": self.model_dump(mode="json")}
        return hashlib.sha256(canonical_json(payload).encode()).hexdigest()[:20]


@dataclass
class Step:
    """One transaction (or the concurrent pair). `ops` are (sql, params) tuples."""

    name: str
    ops: list[tuple[str, tuple]] = field(default_factory=list)
    concurrent: list[tuple[str, tuple]] | None = None  # second session, blocked by the first


INSERT_ORDER = (
    "INSERT INTO walmart.orders (order_id, customer_id, order_ts, updated_ts, status, currency, "
    "total_amount, shipping_country) VALUES (%s, %s, %s, %s, %s, 'USD', %s, 'VN')"
)
UPDATE_ORDER = (
    "UPDATE walmart.orders SET status = %s, total_amount = %s, updated_ts = %s WHERE order_id = %s"
)
DELETE_ORDER = "DELETE FROM walmart.orders WHERE order_id = %s"


def money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def build(config: CdcConfig) -> tuple[list[Step], dict]:
    run = config.cdc_run_id
    prefix = f"{run}-walmart-"
    rng = random.Random(stable_seed(config.seed, CDC_CONTRACT))
    clock = iter(config.base_ts + timedelta(seconds=s) for s in range(10**6))
    state: dict[str, dict | None] = {}
    ops = {"c": 0, "u": 0, "d": 0}
    steps: list[Step] = []

    def row_update(order_id: str, status: str, amount: Decimal) -> tuple[str, tuple]:
        ts = next(clock)
        state[order_id].update(status=status, total_amount=money(amount), updated_ts=ts.isoformat())
        ops["u"] += 1
        return UPDATE_ORDER, (status, amount, ts, order_id)

    def row_insert(order_id: str, customer: str, amount: Decimal) -> tuple[str, tuple]:
        ts = next(clock)
        state[order_id] = {
            "order_id": order_id,
            "customer_id": customer,
            "order_ts": ts.isoformat(),
            "updated_ts": ts.isoformat(),
            "status": "paid",
            "currency": "USD",
            "total_amount": money(amount),
            "shipping_country": "VN",
        }
        ops["c"] += 1
        return INSERT_ORDER, (order_id, customer, ts, ts, "paid", amount)

    def row_delete(order_id: str) -> tuple[str, tuple]:
        state[order_id] = None
        ops["d"] += 1
        return DELETE_ORDER, (order_id,)

    customers = [f"{prefix}c{i:05d}" for i in range(config.customers)]
    steps.append(
        Step(
            "customers",
            [
                (
                    "INSERT INTO walmart.customers VALUES (%s, %s, %s, 'VN', %s)",
                    (c, f"CDC Customer {i}", f"{c}@example.invalid", config.base_ts),
                )
                for i, c in enumerate(customers)
            ],
        )
    )
    orders = [f"{prefix}o{i:06d}" for i in range(config.orders)]
    steps.append(
        Step(
            "insert_orders",
            [
                row_insert(o, rng.choice(customers), Decimal(rng.randint(100, 20000)) / 100)
                for o in orders
            ],
        )
    )
    for order_id in rng.sample(orders, min(config.updated_orders, len(orders))):
        for _ in range(config.updates_per_order):
            amount = Decimal(rng.randint(100, 20000)) / 100
            steps.append(
                Step(f"update {order_id}", [row_update(order_id, rng.choice(STATUSES), amount)])
            )
    multi = rng.sample(orders, min(config.multi_row_update, len(orders)))
    steps.append(
        Step(
            "multi_row_update",
            [
                row_update(o, "packed", Decimal(state[o]["total_amount"]) + Decimal("1.00"))
                for o in multi
            ],
        )
    )
    deleted = rng.sample(orders, config.deletes + config.reinserts)
    for order_id in deleted[: config.deletes]:
        steps.append(Step(f"delete {order_id}", [row_delete(order_id)]))
    for order_id in deleted[config.deletes :]:
        customer = state[order_id]["customer_id"]
        steps.append(Step(f"delete {order_id}", [row_delete(order_id)]))
        steps.append(
            Step(f"reinsert {order_id}", [row_insert(order_id, customer, Decimal("9.99"))])
        )
    # Concurrency: session A updates a row and holds the lock; session B's update of the same row
    # waits until A commits, so commit order (A then B) is deterministic and B's value wins.
    contested = next(o for o in orders if state[o] is not None)
    first = row_update(contested, "shipped", Decimal("10.00"))
    second = row_update(contested, "delivered", Decimal("20.00"))
    steps.append(Step(f"concurrent {contested}", [first], concurrent=[second]))

    live = {k: v for k, v in sorted(state.items()) if v is not None}
    truth = {
        "contract": CDC_CONTRACT,
        "cdc_run_id": run,
        "prefix": prefix,
        "config": config.model_dump(mode="json"),
        "expected_ops": ops,
        "transactions": len(steps),
        "live_orders": len(live),
        "deleted_orders": sorted(k for k, v in state.items() if v is None),
        "final_state": live,
        "final_checksum": checksum(live.values()),
    }
    return steps, truth


def checksum(rows) -> str:
    """SHA-256 over (order_id, status, total_amount, updated_ts) in order_id order."""
    lines = [
        f"{r['order_id']}|{r['status']}|{r['total_amount']}|{normalize_ts(r['updated_ts'])}"
        for r in sorted(rows, key=lambda r: r["order_id"])
    ]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def normalize_ts(value) -> str:
    if isinstance(value, datetime):
        value = value.astimezone(UTC)
    else:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
