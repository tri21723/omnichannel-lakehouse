import importlib.util
import sys
from pathlib import Path

import pytest

HERE = Path("docker/query")
for name in ("trino_client", "run_sql"):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
run_sql = sys.modules["run_sql"]

RUN = "b1e64afaff0176de180a"


def test_render_substitutes_typed_literals_and_strips_comments():
    sql = (
        "-- Params: dataset_run_id, as_of_date (12:00 is not a param)\n"
        "SELECT 1 FROM t FOR VERSION AS OF :fact_orders_snapshot -- noqa: PRS\n"
        "WHERE r = :dataset_run_id AND d = DATE :as_of_date AND ts < TIMESTAMP :window_end\n"
        "AND x::varchar = 'a'"
    )
    out = run_sql.render(
        sql,
        {
            "dataset_run_id": RUN,
            "fact_orders_snapshot": "2146564618510487487",
            "as_of_date": "2026-01-03",
            "window_end": "2026-01-18T00:00:00+00:00",
            "unused_ts": "2026-01-01",
        },
    )
    assert "--" not in out and "noqa" not in out
    assert f"r = '{RUN}'" in out
    assert "AS OF 2146564618510487487" in out
    assert "DATE '2026-01-03'" in out
    assert "TIMESTAMP '2026-01-18 00:00:00.000000 UTC'" in out
    assert "x::varchar" in out


@pytest.mark.parametrize(
    "params, message",
    [
        ({}, "missing parameters"),
        ({"dataset_run_id": "x' OR '1'='1"}, "20 lowercase hex"),
        ({"dataset_run_id": RUN.upper()}, "20 lowercase hex"),
    ],
)
def test_render_rejects_missing_or_unsafe_run_id(params, message):
    with pytest.raises(run_sql.ParamError, match=message):
        run_sql.render("SELECT :dataset_run_id", params)


def test_typed_parameters_reject_injection():
    for name, value in (
        ("fact_orders_snapshot", "1; DROP TABLE x"),
        ("as_of_date", "2026-01-03' OR 1=1 --"),
        ("as_of_ts", "now()"),
        ("window_start", "2026-13-01"),
        ("free_text", "anything"),
    ):
        with pytest.raises(run_sql.ParamError):
            run_sql.literal(name, value)


def test_parse_params_requires_name_value():
    assert run_sql.parse_params(["a=b=c"]) == {"a": "b=c"}
    with pytest.raises(run_sql.ParamError):
        run_sql.parse_params(["novalue"])


def test_every_sample_file_renders_with_documented_params():
    params = {
        "dataset_run_id": RUN,
        "fact_orders_snapshot": "1",
        "as_of_ts": "2026-11-18 00:00:00",
        "as_of_date": "2026-01-03",
        "window_start": "2025-12-19T00:00:00+00:00",
        "window_end": "2026-01-18T00:00:00+00:00",
    }
    files = sorted(Path("sql/trino").glob("*.sql"))
    assert len(files) == 8
    for path in files:
        rendered = run_sql.render(path.read_text(), params)
        assert not run_sql.PLACEHOLDER.findall(rendered)


def test_timestamp_formats_from_trino_and_iso_are_accepted():
    expected = "'2026-11-18 10:43:42.755000 UTC'"
    for value in (
        "2026-11-18 10:43:42.755 UTC",
        "2026-11-18T10:43:42.755Z",
        "2026-11-18T10:43:42.755+00:00",
    ):
        assert run_sql.literal("as_of_ts", value) == expected
