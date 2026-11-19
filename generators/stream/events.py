"""Pure, deterministic event generation and ground truth (no I/O, no wall clock).

Each channel gets a simulated timeline: event i happens at `event_ts`, and the generator sends it
at `send_ms = event_ts + delay`. Sends are ordered by (send_ms, channel, seq), so the delay of a
record relative to what was sent before it decides its lateness class (ADR-0016 §6). `emitted_ts`
(wall clock) is added only when publishing and is not part of the determinism contract.
"""

from __future__ import annotations

import hashlib
import json
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

from generators.common.orders import stable_seed
from generators.stream.config import (
    AMAZON_WINDOW_MS,
    LATE_MAX_MS,
    LATE_MIN_MS,
    ON_TIME_MAX_MS,
    RETRY_MAX_MS,
    RETRY_MIN_MS,
    STREAM_CONTRACT,
    TIKTOK_WINDOW_MS,
    TOO_LATE_MIN_MS,
    TOPICS,
    WATERMARK_BOUND_MS,
    StreamConfig,
)

WINDOW_MS = {"amazon": AMAZON_WINDOW_MS, "tiktok_shop": TIKTOK_WINDOW_MS}
MALFORMED_KINDS = ("truncated_json", "missing_event_id", "bad_event_ts", "unknown_schema")


@dataclass(frozen=True)
class Send:
    send_ms: int
    channel: str
    seq: int
    topic: str
    key: str
    kind: str  # event | retry | malformed
    event_id: str | None
    delay_class: str | None  # on_time | late_accepted | too_late (valid records)
    value: dict | None  # valid records; emitted_ts is added at publish time
    raw: bytes | None  # malformed records, sent as-is


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds")[:-6] + "Z"


def money(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def digest(values: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(values)).encode()).hexdigest()


def _timeline(config: StreamConfig, channel: str) -> tuple[list[int], dict]:
    """Event times in ms. TikTok has `bursts` dense segments (spacing / burst_multiplier)."""
    n = config.events_per_channel
    spacing = max(1, 60_000 // config.events_per_minute)
    burst_spacing = max(1, spacing // config.burst_multiplier)
    in_burst = [False] * n
    burst_info = {"count": 0, "events": 0, "spacing_ms": spacing, "burst_spacing_ms": spacing}
    if channel == "tiktok_shop" and config.burst_share:
        total = round(n * config.burst_share)
        sizes = [
            total // config.bursts + (1 if i < total % config.bursts else 0)
            for i in range(config.bursts)
        ]
        gap = (n - total) // (config.bursts + 1)
        position = gap
        for size in sizes:
            for i in range(position, min(n, position + size)):
                in_burst[i] = True
            position += size + gap
        burst_info = {
            "count": config.bursts,
            "events": sum(in_burst),
            "spacing_ms": spacing,
            "burst_spacing_ms": burst_spacing,
        }
    start = int(config.start_ts.timestamp() * 1000)
    times, t = [], start
    for i in range(n):
        times.append(t)
        t += burst_spacing if in_burst[i] else spacing
    return times, burst_info


def _payload(channel: str, run_id: str, order_no: int, rng: random.Random) -> tuple[dict, str]:
    buyer = f"{run_id}-{channel}-b{order_no % 997:04d}"
    qty = rng.randint(1, 4)
    cents = rng.randint(100, 20000) * qty
    sku = f"{channel}-sku{rng.randint(0, 49):02d}"
    if channel == "amazon":
        payload = {
            "notification_type": "ORDER_CHANGE",
            "marketplace_id": "ATVPDKIKX0DER",
            "buyer": {"buyer_id": buyer, "email": f"{buyer}@example.invalid"},
            "items": [{"sku": sku, "qty": qty}],
        }
    else:
        payload = {
            "webhook_type": 1,
            "shop_id": f"shop-{order_no % 7}",
            "order_status": "AWAITING_SHIPMENT",
            "buyer_email": f"{buyer}@example.invalid",
            "line_items": [{"sku": sku, "quantity": qty}],
        }
    return payload, money(cents)


def _malformed(kind: str, channel: str, run_id: str, i: int, ts_ms: int) -> bytes:
    base = {
        "schema_version": 1,
        "event_id": f"{run_id}-{channel}-m{i:07d}",
        "event_type": "order_created",
        "stream_run_id": run_id,
        "channel": channel,
        "order_id": f"{run_id}-{channel}-mo{i:07d}",
        "event_ts": iso_ms(ts_ms),
        "currency": "USD",
        "amount": "1.00",
        "payload": {},
    }
    if kind == "truncated_json":
        return json.dumps(base, sort_keys=True).encode()[:40]
    if kind == "missing_event_id":
        base.pop("event_id")
    elif kind == "bad_event_ts":
        base["event_ts"] = "not-a-timestamp"
    else:
        base["schema_version"] = 99
    return json.dumps(base, sort_keys=True).encode()


def channel_sends(config: StreamConfig, channel: str) -> tuple[list[Send], dict]:
    run_id = config.stream_run_id
    rng = random.Random(stable_seed(config.seed, channel, STREAM_CONTRACT))
    n = config.events_per_channel
    times, burst_info = _timeline(config, channel)
    end = times[-1]
    topic = TOPICS[channel]

    # Late classes only where enough later events exist to move the watermark past them.
    classes = ["on_time"] * n
    if channel == "amazon" and config.late_rate:
        horizon = config.late_max_minutes * 60_000 + WATERMARK_BOUND_MS + 60_000
        eligible = [i for i in range(n) if times[i] + horizon <= end]
        wanted = round(n * config.late_rate)
        if len(eligible) < wanted:
            raise ValueError(
                f"{channel}: stream too short for {wanted} late events; raise events_per_channel "
                f"or lower events_per_minute/late_max_minutes"
            )
        late = sorted(rng.sample(eligible, wanted))
        too_late = set(rng.sample(late, round(len(late) * config.too_late_share)))
        for i in late:
            classes[i] = "too_late" if i in too_late else "late_accepted"

    sends: list[Send] = []
    orders: list[int] = []
    windows: dict[int, list] = defaultdict(lambda: [0, 0])
    ids: dict[str, list[str]] = defaultdict(list)
    for i in range(n):
        if orders and rng.random() < config.update_share:
            order_no, event_type = rng.choice(orders), "order_updated"
        else:
            order_no, event_type = len(orders), "order_created"
            orders.append(order_no)
        payload, amount = _payload(channel, run_id, order_no, rng)
        if classes[i] == "on_time":
            delay = rng.randint(0, ON_TIME_MAX_MS)
        elif classes[i] == "late_accepted":
            delay = rng.randint(LATE_MIN_MS, LATE_MAX_MS)
        else:
            delay = rng.randint(TOO_LATE_MIN_MS, config.late_max_minutes * 60_000)
        event_id = f"{run_id}-{channel}-e{i:07d}"
        order_id = f"{run_id}-{channel}-o{order_no:07d}"
        value = {
            "schema_version": 1,
            "event_id": event_id,
            "event_type": event_type,
            "stream_run_id": run_id,
            "channel": channel,
            "order_id": order_id,
            "event_ts": iso_ms(times[i]),
            "currency": "USD",
            "amount": amount,
            "payload": payload,
        }
        sends.append(
            Send(
                times[i] + delay,
                channel,
                i,
                topic,
                order_id,
                "event",
                event_id,
                classes[i],
                value,
                None,
            )
        )
        ids[classes[i]].append(event_id)
        if classes[i] != "too_late":
            cell = windows[times[i] - times[i] % WINDOW_MS[channel]]
            cell[0] += 1
            cell[1] += int(amount.replace(".", ""))

    retries = []
    if channel == "tiktok_shop" and config.dup_rate:
        for i in sorted(rng.sample(range(n), round(n * config.dup_rate))):
            first = sends[i]
            retries.append(first.event_id)
            sends.append(
                Send(
                    first.send_ms + rng.randint(RETRY_MIN_MS, RETRY_MAX_MS),
                    channel, n + len(retries), topic, first.key, "retry",
                    first.event_id, first.delay_class, first.value, None,
                )
            )  # fmt: skip

    malformed = round(n * config.malformed_rate)
    for i in range(malformed):
        kind = MALFORMED_KINDS[i % len(MALFORMED_KINDS)]
        at = rng.randint(times[0], end)
        key = f"{run_id}-{channel}-malformed-{i:05d}"
        raw = _malformed(kind, channel, run_id, i, at)
        sends.append(Send(at, channel, 2 * n + i, topic, key, "malformed", None, None, None, raw))

    truth = {
        "topic": topic,
        "records_sent": len(sends),
        "valid_records": n + len(retries),
        "unique_events": n,
        "retries": len(retries),
        "malformed": malformed,
        "classes": {name: len(ids[name]) for name in ("on_time", "late_accepted", "too_late")},
        "event_id_sha256": {name: digest(ids[name]) for name in sorted(ids)},
        "retry_event_id_sha256": digest(retries),
        "event_ts_min": iso_ms(times[0]),
        "event_ts_max": iso_ms(end),
        "window_ms": WINDOW_MS[channel],
        "windows": [
            {"window_start": iso_ms(start), "events": cell[0], "amount": money(cell[1])}
            for start, cell in sorted(windows.items())
        ],
        "bursts": burst_info,
    }
    return sends, truth


def generate(config: StreamConfig) -> tuple[list[Send], dict]:
    """All sends in publish order plus the ground truth document."""
    sends: list[Send] = []
    channels = {}
    for channel in config.channels:
        channel_list, truth = channel_sends(config, channel)
        sends.extend(channel_list)
        channels[channel] = truth
    sends.sort(key=lambda s: (s.send_ms, s.channel, s.seq))
    truth = {
        "contract": STREAM_CONTRACT,
        "stream_run_id": config.stream_run_id,
        "config": config.model_dump(mode="json"),
        "records_total": len(sends),
        "channels": channels,
    }
    return sends, truth


def wire_value(send: Send, emitted_ms: int) -> bytes:
    """Serialized Kafka value; valid records get the wall-clock `emitted_ts`."""
    if send.raw is not None:
        return send.raw
    return json.dumps({**send.value, "emitted_ts": iso_ms(emitted_ms)}, sort_keys=True).encode()
