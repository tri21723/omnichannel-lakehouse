"""Typed, validated parameters for sql/trino files (ADR-0015). Shared by the host runner and
the Spark parity job; pure Python, no pyspark.

Placeholders are ``:name``; values are rendered as typed literals chosen by parameter name.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

PLACEHOLDER = re.compile(r"(?<![\w:]):([a-z][a-z0-9_]*)\b")
RUN_ID = re.compile(r"[0-9a-f]{20}")


class ParamError(ValueError):
    pass


def _timestamp(value: str) -> str:
    text = value.strip().removesuffix(" UTC").removesuffix("Z").removesuffix("+00:00")
    text = text.replace("T", " ")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ParamError(f"invalid UTC timestamp: {value!r}") from None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return f"'{parsed.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC'"


def literal(name: str, value: str) -> str:
    """Typed SQL literal for a parameter, or ParamError."""
    if name == "dataset_run_id":
        if not RUN_ID.fullmatch(value):
            raise ParamError("dataset_run_id must be 20 lowercase hex characters")
        return f"'{value}'"
    if name.endswith("_snapshot"):
        if not re.fullmatch(r"-?\d{1,20}", value):
            raise ParamError(f"{name} must be an integer snapshot id")
        return value
    if name.endswith("_date"):
        try:
            return f"'{date.fromisoformat(value).isoformat()}'"
        except ValueError:
            raise ParamError(f"{name} must be YYYY-MM-DD") from None
    if name.endswith("_ts") or name.startswith("window_"):
        return _timestamp(value)
    raise ParamError(f"unknown parameter: {name}")


def strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0].rstrip() for line in sql.splitlines()).strip()


def render(sql: str, params: dict[str, str]) -> str:
    body = strip_comments(sql)
    needed = set(PLACEHOLDER.findall(body))
    missing = sorted(needed - set(params))
    if missing:
        raise ParamError(f"missing parameters: {missing}")
    rendered = {name: literal(name, params[name]) for name in needed}
    return PLACEHOLDER.sub(lambda m: rendered[m.group(1)], body)


def parse_params(pairs: list[str]) -> dict[str, str]:
    params = {}
    for pair in pairs:
        if "=" not in pair:
            raise ParamError(f"expected name=value, got {pair!r}")
        name, value = pair.split("=", 1)
        params[name.strip()] = value.strip()
    return params
