"""Create local credentials without overwriting an existing environment file."""

import os
import secrets
from pathlib import Path


def main() -> None:
    target = Path(".env")
    if target.exists():
        print(".env already exists; preserved. See .env.example for new variables.")
        return
    lines = []
    for line in Path(".env.example").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            if not value and ("PASSWORD" in key or "SECRET_KEY" in key):
                line = f"{key}={secrets.token_hex(24)}"
        lines.append(line)
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write("\n".join(lines) + "\n")
    print("Created .env with random local credentials (values not logged).")


if __name__ == "__main__":
    main()
