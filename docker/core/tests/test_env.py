"""Local credential bootstrap must preserve user configuration."""

import importlib.util
from pathlib import Path

MODULE = Path(__file__).parents[1] / "env.py"
spec = importlib.util.spec_from_file_location("core_env", MODULE)
core_env = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core_env)


def test_generate_unique_secrets_and_preserve_existing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path(".env.example").write_text("POSTGRES_PASSWORD=\nSPARK_S3_SECRET_KEY=\nOTHER=value\n")
    core_env.main()
    first = Path(".env").read_text()
    values = dict(line.split("=", 1) for line in first.splitlines())
    assert len(values["POSTGRES_PASSWORD"]) >= 32
    assert values["POSTGRES_PASSWORD"] != values["SPARK_S3_SECRET_KEY"]
    assert values["OTHER"] == "value"
    core_env.main()
    assert Path(".env").read_text() == first


def test_existing_env_never_rewritten(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path(".env").write_text("custom-existing-config")
    core_env.main()
    assert Path(".env").read_text() == "custom-existing-config"
