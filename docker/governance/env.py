"""Add missing Phase 9 settings to .env without rotating existing lines; write the UI login file.

The frontend reads its JAAS users from `.local-data/governance/user.props` (git-ignored); it is
regenerated from `.env` on every run so it always matches DATAHUB_ADMIN_PASSWORD.
"""

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
    "DATAHUB_VERSION": "v1.7.0.1",
    "DATAHUB_OPENSEARCH_IMAGE": "opensearchproject/opensearch:2.19.3",
    "DATAHUB_DB_PASSWORD": secrets.token_hex(24),
    "DATAHUB_RO_DB_PASSWORD": secrets.token_hex(24),
    "DATAHUB_SYSTEM_CLIENT_SECRET": secrets.token_hex(24),
    "DATAHUB_TOKEN_SERVICE_SALT": secrets.token_hex(24),
    "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": secrets.token_hex(32),
    "DATAHUB_SECRET": secrets.token_hex(32),
    "DATAHUB_ADMIN_PASSWORD": secrets.token_hex(16),
}
merge_env(path, settings)

values = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
props = Path(".local-data/governance/user.props")
props.parent.mkdir(parents=True, exist_ok=True)
props.write_text(f"datahub:{values['DATAHUB_ADMIN_PASSWORD']}\n")
os.chmod(props, 0o644)  # read by the frontend container user; the directory is git-ignored
