"""Run a sql/trino file with validated parameters (ADR-0015).

Placeholders are ``:name`` (the same syntax DBeaver prompts for). Values are validated by name
and rendered as typed literals; there is no free-form string interpolation.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from trino_client import DEFAULT_URL, run  # noqa: E402

from spark.lib.sql_params import (  # noqa: E402,F401
    PLACEHOLDER,
    ParamError,
    literal,
    parse_params,
    render,
    strip_comments,
)


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
