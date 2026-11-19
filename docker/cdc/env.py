"""Add missing Phase 8 settings to .env without rewriting or rotating existing lines."""

import os
from pathlib import Path

path = Path(".env")
existing = path.read_text()
keys = {line.split("=", 1)[0] for line in existing.splitlines() if "=" in line}
settings = {"DEBEZIUM_CONNECT_IMAGE": "quay.io/debezium/connect:3.6.3.Final"}
with path.open("a") as stream:
    if not existing.endswith("\n"):
        stream.write("\n")
    for key, value in settings.items():
        if key not in keys:
            stream.write(f"{key}={value}\n")
os.chmod(path, 0o600)
