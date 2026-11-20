"""Add missing Phase 7 settings to .env without rewriting or rotating existing lines."""

import os
import secrets
from pathlib import Path


def merge_env(path: Path, settings: dict[str, str]) -> None:
    """Fill keys that are missing or empty (a fresh .env copies empty values from .env.example);
    never overwrite a non-empty value, so credentials are not rotated."""
    lines = path.read_text().splitlines()
    present = set()
    for index, line in enumerate(lines):
        key, sep, value = line.partition("=")
        if sep and key in settings:
            present.add(key)
            if value == "":
                lines[index] = f"{key}={settings[key]}"
    lines += [f"{key}={value}" for key, value in settings.items() if key not in present]
    path.write_text("\n".join(lines) + "\n")
    os.chmod(path, 0o600)


path = Path(".env")
settings = {
    "KAFKA_IMAGE": "apache/kafka:4.3.2",
    "KAFKA_UI_IMAGE": "kafbat/kafka-ui:v1.5.0",
    "FLINK_IMAGE": "flink:2.2.1-java17",
    # KRaft cluster id: 22-char base64url of 16 random bytes, generated once per environment.
    "KAFKA_CLUSTER_ID": secrets.token_urlsafe(16)[:22],
    "FLINK_S3_ACCESS_KEY": "flink_local",
    "FLINK_S3_SECRET_KEY": secrets.token_hex(24),
}
merge_env(path, settings)
