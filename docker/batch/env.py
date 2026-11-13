"""Add missing Phase 3 settings without rewriting existing environment lines."""

import os
import secrets
from pathlib import Path

path = Path(".env")
existing = path.read_text()
if "LAKEKEEPER_ENCRYPTION_KEY=\n" in existing:
    existing = existing.replace(
        "LAKEKEEPER_ENCRYPTION_KEY=\n", "LAKEKEEPER_ENCRYPTION_KEY=" + secrets.token_hex(32) + "\n"
    )
    path.write_text(existing)
keys = {line.split("=", 1)[0] for line in existing.splitlines() if "=" in line}
settings = {
    "LAKEKEEPER_IMAGE": "quay.io/lakekeeper/catalog:v0.9.3",
    "LAKEKEEPER_ENCRYPTION_KEY": secrets.token_hex(32),
    "LAKEHOUSE_WAREHOUSE": "local_lakehouse",
}
with path.open("a") as stream:
    if not existing.endswith("\n"):
        stream.write("\n")
    for key, value in settings.items():
        if key not in keys:
            stream.write(f"{key}={value}\n")
os.chmod(path, 0o600)
