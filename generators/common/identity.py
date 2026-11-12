"""Stable, channel-scoped identities for downstream joins."""

import json

CHANNELS = frozenset({"walmart", "amazon", "tiktok_shop", "shopee", "lazada"})


def entity_key(channel: str, source_id: str) -> str:
    """Encode an opaque source ID without delimiter collisions or normalization."""
    if channel not in CHANNELS:
        raise ValueError(f"Unsupported channel: {channel}")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source_id must be a non-empty string")
    return json.dumps([channel, source_id], ensure_ascii=False, separators=(",", ":"))
