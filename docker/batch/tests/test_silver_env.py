import runpy
from pathlib import Path


def test_secret_initialization_preserves_existing_key_and_eof(monkeypatch, tmp_path):
    script = Path("docker/batch/silver_env.py").resolve()
    monkeypatch.chdir(tmp_path)
    path = tmp_path / ".env"
    path.write_text("EXISTING=preserved\nSILVER_PII_KEY=")
    runpy.run_path(str(script))
    first = path.read_text()
    values = dict(line.split("=", 1) for line in first.splitlines())
    assert len(bytes.fromhex(values["SILVER_PII_KEY"])) == 32
    assert values["EXISTING"] == "preserved"
    runpy.run_path(str(script))
    assert path.read_text() == first
