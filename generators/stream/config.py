"""Stream generator settings. Separate from GeneratorConfig so batch run IDs never change."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from generators.common.config import canonical_json

StreamChannel = Literal["amazon", "tiktok_shop"]
STREAM_CONTRACT = "stream-v1"
TOPICS: dict[str, str] = {
    "amazon": "amazon.order-notifications",
    "tiktok_shop": "tiktok_shop.order-webhooks",
}

# Delay classes in milliseconds (ADR-0016 §6). Mirrored by the Flink jobs; change both together.
WATERMARK_BOUND_MS = 2 * 60_000
AMAZON_WINDOW_MS = 5 * 60_000
TIKTOK_WINDOW_MS = 60_000
ALLOWED_LATENESS_MS = 20 * 60_000
ON_TIME_MAX_MS = 60_000
LATE_MIN_MS = 8 * 60_000
LATE_MAX_MS = 15 * 60_000
TOO_LATE_MIN_MS = 30 * 60_000
RETRY_MIN_MS = 5_000
RETRY_MAX_MS = 5 * 60_000


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    seed: int = Field(default=42, ge=0)
    start_ts: datetime = datetime(2026, 2, 1, tzinfo=UTC)
    channels: tuple[StreamChannel, ...] = ("amazon", "tiktok_shop")
    events_per_channel: int = Field(default=5000, ge=1, le=1_000_000)
    # Simulated event-time density. With 3 partitions the gap inside one partition is ~3/rate
    # minutes, far below the 1-minute classification margins (ADR-0016 §6).
    events_per_minute: int = Field(default=60, ge=30, le=60_000)
    late_rate: float = Field(default=0.05, ge=0, le=0.5)
    too_late_share: float = Field(default=0.4, ge=0, le=1)
    late_max_minutes: int = Field(default=60, ge=31, le=240)
    dup_rate: float = Field(default=0.10, ge=0, le=0.5)
    burst_multiplier: int = Field(default=50, ge=1, le=1000)
    burst_share: float = Field(default=0.2, ge=0, le=0.9)
    bursts: int = Field(default=4, ge=1, le=100)
    malformed_rate: float = Field(default=0.002, ge=0, le=0.1)
    update_share: float = Field(default=0.2, ge=0, le=0.9)

    @field_validator("start_ts")
    @classmethod
    def utc_window_aligned(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("start_ts must include a timezone")
        value = value.astimezone(UTC)
        if value.second or value.microsecond or value.minute % 5:
            raise ValueError("start_ts must align to a 5-minute boundary")
        return value

    @field_validator("channels")
    @classmethod
    def unique_channels(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("channels must be nonempty and unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def burst_fits(self) -> "StreamConfig":
        if self.burst_share and round(self.events_per_channel * self.burst_share) < self.bursts:
            raise ValueError("burst_share too small for the number of bursts")
        return self

    @property
    def stream_run_id(self) -> str:
        payload = {"contract": STREAM_CONTRACT, "config": self.model_dump(mode="json")}
        return hashlib.sha256(canonical_json(payload).encode()).hexdigest()[:20]


def read_stream_config(path: Path, **overrides: object) -> StreamConfig:
    content = yaml.safe_load(path.read_text())
    if not isinstance(content, dict):
        raise ValueError("Stream config must be a YAML mapping")
    content.update({key: value for key, value in overrides.items() if value is not None})
    return StreamConfig.model_validate(content)
