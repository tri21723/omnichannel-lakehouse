"""Initialize a local PII key once; never print or rotate an existing key."""

import os
import secrets
from pathlib import Path

path = Path(".env")
text = path.read_text().rstrip("\n") + "\n"
values = dict(
    line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#")
)
for name, value in {
    "SILVER_PII_KEY": secrets.token_hex(32),
    "SILVER_PII_KEY_VERSION": "local-v1",
}.items():
    if not values.get(name):
        if name in values:
            text = text.replace(name + "=\n", name + "=" + value + "\n")
        else:
            text = text.rstrip("\n") + "\n" + name + "=" + value + "\n"
path.write_text(text)
os.chmod(path, 0o600)
