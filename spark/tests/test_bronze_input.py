import io
import json
from pathlib import Path

import pytest

from generators.batch.dataset import generate
from generators.common.config import GeneratorConfig
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
