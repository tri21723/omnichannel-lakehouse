import io
import json
from pathlib import Path

import pytest

from generators.batch.dataset import generate
from generators.common.config import GeneratorConfig
from spark.lib import bronze_input
from spark.lib.bronze import writer_lock
from spark.lib.bronze_input import identity, parse_status, select_files, stage


class Body(io.BytesIO):
    pass


class Client:
    def __init__(self, root: Path):
        self.root = root

    def get_object(self, Bucket, Key):
        return {"Body": Body((self.root / Key).read_bytes())}


def fixture(tmp_path):
    cfg = GeneratorConfig(orders_per_day_per_channel=25, rows_per_file=10)
    root = generate(cfg, tmp_path)
    manifest = json.loads((root / "stream_manifest.json").read_text())
    entries = select_files(manifest, cfg.run_id, "2026-01-01", "amazon")
    return root, cfg, manifest, entries


def test_manifest_and_staging_preserve_duplicates(tmp_path):
    root, cfg, _, entries = fixture(tmp_path)
    output = tmp_path / "stage"
    output.mkdir()
    paths = stage(Client(root), "raw", entries, output, 100000)
    rows = [json.loads(line) for p in paths for line in p.read_text().splitlines()]
    assert len(rows) == 26
    assert len({r["_raw"] for r in rows}) == 25
    assert all(r["_source_file"].startswith("s3://raw/amazon/") for r in rows)
    assert all(r["_parse_status"] == "valid" for r in rows)
    assert identity(cfg.run_id, "2026-01-01", "amazon", "jsonl") != identity(
        cfg.run_id, "2026-01-02", "amazon", "jsonl"
    )


def test_invalid_inventory_and_limits(tmp_path):
    root, cfg, manifest, entries = fixture(tmp_path)
    with pytest.raises(ValueError):
        select_files(manifest, cfg.run_id, "2026-01-04", "amazon")
    manifest["channels"]["amazon"]["files"].append(entries[0])
    with pytest.raises(ValueError):
        select_files(manifest, cfg.run_id, "2026-01-01", "amazon")
    with pytest.raises(ValueError):
        stage(Client(root), "raw", entries, tmp_path, 1)
    entries[0]["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        stage(Client(root), "raw", entries, tmp_path, 100000)


@pytest.mark.parametrize(
    "raw,status", [("{}", "valid"), ("broken", "invalid"), ("  ", "blank"), ("NaN", "invalid")]
)
def test_parse_status(raw, status):
    assert parse_status(raw) == status


def test_lock_released_and_conflict(tmp_path):
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError), writer_lock(tmp_path):
            pass
    with writer_lock(tmp_path):
        pass


def test_malformed_lines_preserved_and_encoding_rejected(tmp_path):
    import hashlib

    key = "raw.jsonl"
    payload = b'broken\n\nNaN\n{"unknown": 1}\n'
    (tmp_path / key).write_bytes(payload)
    entry = {
        "path": key,
        "bytes": len(payload),
        "rows": 4,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    paths = stage(Client(tmp_path), "raw", [entry], tmp_path, 1000)
    rows = [json.loads(line) for line in paths[0].read_text().splitlines()]
    assert [row["_parse_status"] for row in rows] == ["invalid", "blank", "invalid", "valid"]
    assert [row["_raw"] for row in rows] == payload.decode().splitlines()
    payload = b"\xff\n"
    (tmp_path / key).write_bytes(payload)
    entry.update(bytes=len(payload), rows=1, sha256=hashlib.sha256(payload).hexdigest())
    with pytest.raises(UnicodeDecodeError):
        stage(Client(tmp_path), "raw", [entry], tmp_path, 1000)


@pytest.mark.parametrize("path", ["../secret", "/absolute", "s3://other/path"])
def test_reject_manifest_paths_before_io(tmp_path, path):
    _, cfg, manifest, entries = fixture(tmp_path)
    entries[0]["path"] = path
    with pytest.raises(ValueError):
        select_files(manifest, cfg.run_id, "2026-01-01", "amazon")


@pytest.mark.parametrize("extra", [["--channel", "unknown"], ["--source", "wrong"], []])
def test_cli_rejects_missing_or_invalid_selection(monkeypatch, extra):
    import sys

    from spark.jobs.dp1_bronze.main import arguments

    monkeypatch.setattr(sys, "argv", ["dp1_bronze", *extra])
    with pytest.raises(SystemExit):
        arguments()


@pytest.mark.parametrize(
    "message,reason",
    [
        ("Object fingerprint mismatch", "source_fingerprint_mismatch"),
        ("Staging byte/disk limit exceeded", "staging_limit_exceeded"),
        ("private-payload-do-not-log", "runtime_or_validation_failure"),
    ],
)
def test_cli_failure_exit_and_safe_reason(message, reason, caplog):
    from unittest.mock import patch

    from spark.jobs.dp1_bronze.main import run_cli

    with patch("spark.jobs.dp1_bronze.main.main", side_effect=ValueError(message)):
        with caplog.at_level("INFO"), pytest.raises(SystemExit) as failure:
            run_cli()
    assert failure.value.code == 1
    output = json.loads(caplog.records[-1].message)
    assert output["reason"] == reason
    assert "private-payload-do-not-log" not in caplog.text


def test_raw_preserves_bare_cr_without_lf(tmp_path):
    import hashlib

    payload = b'{"a":1}\r\n{"b":2}\r'
    (tmp_path / "lines.jsonl").write_bytes(payload)
    entry = {
        "path": "lines.jsonl",
        "bytes": len(payload),
        "rows": 2,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    paths = stage(Client(tmp_path), "raw", [entry], tmp_path, 1000)
    rows = [json.loads(line)["_raw"] for line in paths[0].read_text().splitlines()]
    assert rows == ['{"a":1}', '{"b":2}\r']


STREAM_RUN = "e7a188e49b1e4042ef15"


def stream_manifest(**overrides):
    def entry(branch, date, rows, n=0):
        return {
            "path": f"stream/amazon/{branch}/run={STREAM_RUN}/date={date}/amazon-x-{n}.jsonl",
            "branch": branch,
            "date": date,
            "rows": rows,
            "bytes": rows * 100,
            "sha256": "a" * 64,
        }

    manifest = {
        "contract": "stream-output-v1",
        "stream_run_id": STREAM_RUN,
        "ground_truth_sha256": "b" * 64,
        "channels": {
            "amazon": {
                "valid_records": 3000,
                "dates": ["2026-02-01", "2026-02-02"],
                "files": [
                    entry("accepted", "2026-02-01", 2000),
                    entry("accepted", "2026-02-02", 940, 1),
                    entry("late", "2026-02-01", 60),
                    entry("windows", "2026-02-01", 100),
                ],
            }
        },
    }
    manifest.update(overrides)
    return manifest


def test_stream_selection_by_date_and_branch_ignores_windows():
    selected = bronze_input.select_stream_files(
        stream_manifest(), STREAM_RUN, "2026-02-01", "amazon"
    )
    assert {b: [e["rows"] for e in files] for b, files in selected.items()} == {
        "accepted": [2000],
        "late": [60],
        "duplicates": [],
    }
    assert bronze_input.identity(STREAM_RUN, "2026-02-01", "amazon", "stream") != (
        bronze_input.identity(STREAM_RUN, "2026-02-01", "amazon", "jsonl")
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda m: m.update(contract="stream-output-v0"), "Manifest identity mismatch"),
        (lambda m: m["channels"]["amazon"]["files"].pop(2), "Ground truth mismatch"),
        (lambda m: m["channels"]["amazon"]["files"].pop(0), "Ground truth mismatch"),
        (
            lambda m: m["channels"]["amazon"]["files"][0].update(
                path="stream/amazon/accepted/x.jsonl"
            ),
            "Invalid/duplicate object key",
        ),
        (lambda m: m["channels"]["amazon"]["files"][1].update(branch="late"), "Invalid/duplicate"),
        (lambda m: m["channels"]["amazon"]["files"][0].update(sha256="nope"), "Invalid checksum"),
    ],
)
def test_stream_selection_rejects_inconsistent_manifests(change, message):
    manifest = stream_manifest()
    change(manifest)
    with pytest.raises(ValueError, match=message):
        bronze_input.select_stream_files(manifest, STREAM_RUN, "2026-02-01", "amazon")


def test_stream_source_only_for_stream_channels():
    with pytest.raises(ValueError, match="Unsupported source"):
        bronze_input.identity(STREAM_RUN, "2026-02-01", "walmart", "stream")
    with pytest.raises(ValueError, match="outside manifest"):
        bronze_input.select_stream_files(stream_manifest(), STREAM_RUN, "2026-02-03", "amazon")
