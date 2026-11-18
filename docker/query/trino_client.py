"""Host entry point for the shared Trino HTTP client (implementation: spark/lib/trino_http.py)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from spark.lib.trino_http import (  # noqa: E402,F401
    DEFAULT_URL,
    Result,
    TrinoQueryError,
    collect,
    run,
)
