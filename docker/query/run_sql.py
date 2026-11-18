"""Run a sql/trino file with validated parameters (ADR-0015).

Placeholders are ``:name`` (the same syntax DBeaver prompts for). Values are validated by name
and rendered as typed literals; there is no free-form string interpolation.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from trino_client import DEFAULT_URL, run  # noqa: E402

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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sql_file", type=Path)
    parser.add_argument("--param", action="append", default=[], help="name=value")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--format", choices=("json", "csv"), default="json")
    args = parser.parse_args(argv)
    try:
        sql = render(args.sql_file.read_text(encoding="utf-8"), parse_params(args.param))
    except ParamError as exc:
        parser.error(str(exc))
    result = run(sql, args.url)
    if args.format == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(result.columns)
        writer.writerows(result.rows)
    else:
        print(json.dumps({"columns": result.columns, "rows": result.rows}, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
