"""Initialize local Airflow secrets once; never print or rotate existing values."""

import base64
import os
import secrets
from pathlib import Path

path = Path(".env")
text = path.read_text().rstrip("\n") + "\n"
values = dict(
    line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#")
)
for name, factory in {
    "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_hex(16),
    "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
    "AIRFLOW_API_SECRET_KEY": lambda: secrets.token_hex(32),
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_hex(32),
    "AIRFLOW_IMAGE": lambda: "omnichannel/airflow:3.3.2-spark3.5.5",
}.items():
    if not values.get(name):
        value = factory()
        if name in values:
            text = text.replace(name + "=\n", name + "=" + value + "\n")
        else:
            text = text.rstrip("\n") + "\n" + name + "=" + value + "\n"
path.write_text(text)
os.chmod(path, 0o600)
