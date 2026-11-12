"""Validated configuration and stable dataset identity."""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

Channel = Literal["walmart", "amazon", "tiktok_shop", "shopee", "lazada"]
CONTRACT_VERSION = "batch-v1"


class GeneratorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    seed: int = Field(default=42, ge=0)
    start_date: date = date(2026, 1, 1)
    days: int = Field(default=3, ge=1, le=366)
    orders_per_day_per_channel: int = Field(default=1000, ge=1, le=1_000_000)
    channels: tuple[Channel, ...] = ("walmart", "amazon", "tiktok_shop", "shopee", "lazada")
    dup_rate: float = Field(default=0.04, ge=0, le=1)
    skew_top_seller_share: float = Field(default=0.5, ge=0, le=1)
    schema_drift_at_day: int = Field(default=2, ge=0)
    return_rate: float = Field(default=0.1, ge=0, le=1)
    rows_per_file: int = Field(default=10_000, ge=1, le=100_000)
    # Reserved for Phase 7 streaming; batch must not mislabel them as implemented.
    late_rate: float = Field(default=0.05, ge=0, le=1)
    late_max_minutes: int = Field(default=60, ge=1)
    burst_multiplier: int = Field(default=50, ge=1)

    @field_validator("channels")
    @classmethod
    def validate_channels(cls, value: tuple[Channel, ...]) -> tuple[Channel, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("channels must be nonempty and unique")
        return tuple(sorted(value))

    @property
    def run_id(self) -> str:
        payload = {"contract": CONTRACT_VERSION, "config": self.model_dump(mode="json")}
        return hashlib.sha256(canonical_json(payload).encode()).hexdigest()[:20]


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def read_config(path: Path, **overrides: object) -> GeneratorConfig:
    content = yaml.safe_load(path.read_text())
    if not isinstance(content, dict):
        raise ValueError("Generator config must be a YAML mapping")
    content.update({key: value for key, value in overrides.items() if value is not None})
    return GeneratorConfig.model_validate(content)
