"""Add missing Phase 7 settings to .env without rewriting or rotating existing lines."""

import os
import secrets
from pathlib import Path

path = Path(".env")
existing = path.read_text()
keys = {line.split("=", 1)[0] for line in existing.splitlines() if "=" in line}
settings = {
    "KAFKA_IMAGE": "apache/kafka:4.3.2",
    "KAFKA_UI_IMAGE": "kafbat/kafka-ui:v1.5.0",
    "FLINK_IMAGE": "flink:2.2.1-java17",
    # KRaft cluster id: 22-char base64url of 16 random bytes, generated once per environment.
    "KAFKA_CLUSTER_ID": secrets.token_urlsafe(16)[:22],
    "FLINK_S3_ACCESS_KEY": "flink_local",
    "FLINK_S3_SECRET_KEY": secrets.token_hex(24),
}
with path.open("a") as stream:
    if not existing.endswith("\n"):
        stream.write("\n")
    for key, value in settings.items():
        if key not in keys:
            stream.write(f"{key}={value}\n")
os.chmod(path, 0o600)
