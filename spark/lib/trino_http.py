"""Minimal Trino HTTP client (stdlib only) shared by the host tools and Airflow (ADR-0015).

Implements the client protocol: POST /v1/statement, then follow ``nextUri`` until done.
"""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field

DEFAULT_URL = "http://127.0.0.1:8083"


class TrinoQueryError(RuntimeError):
    def __init__(self, error: dict):
        self.error_name = error.get("errorName", "UNKNOWN")
        self.error_type = error.get("errorType", "UNKNOWN")
        self.message = error.get("message", "")
        super().__init__(f"{self.error_name}: {self.message}")

    @property
    def access_denied(self) -> bool:
        return self.error_name == "PERMISSION_DENIED" or "Access Denied" in self.message


@dataclass
class Result:
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)

    def dicts(self) -> list[dict]:
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]


def _request(url: str, method: str, user: str, body: bytes | None, timeout: float) -> dict:
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"X-Trino-User": user, "X-Trino-Source": "omnichannel-query"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def collect(pages) -> Result:
    """Fold protocol pages into a result; raise on the first error page."""
    result = Result()
    for page in pages:
        if "error" in page and page["error"]:
            raise TrinoQueryError(page["error"])
        if page.get("columns") and not result.columns:
            result.columns = [c["name"] for c in page["columns"]]
        result.rows.extend(page.get("data") or [])
    return result


def run(
    sql: str,
    url: str = DEFAULT_URL,
    user: str = "analyst",
    timeout: float = 120.0,
) -> Result:
    deadline = time.monotonic() + timeout

    def pages():
        page = _request(f"{url}/v1/statement", "POST", user, sql.encode(), timeout)
        yield page
        while page.get("nextUri"):
            if time.monotonic() > deadline:
                raise TimeoutError(f"query exceeded {timeout}s")
            page = _request(page["nextUri"], "GET", user, None, timeout)
            yield page

    return collect(pages())
