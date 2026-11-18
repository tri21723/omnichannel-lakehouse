import base64
import runpy
from pathlib import Path


def test_airflow_secrets_are_created_once_and_preserved(monkeypatch, tmp_path):
    script = Path("docker/orch/env.py").resolve()
    monkeypatch.chdir(tmp_path)
    path = tmp_path / ".env"
    path.write_text("EXISTING=preserved\nAIRFLOW_ADMIN_PASSWORD=\nAIRFLOW_JWT_SECRET=keep-me")
    runpy.run_path(str(script))
    first = path.read_text()
    values = dict(line.split("=", 1) for line in first.splitlines())
    assert values["EXISTING"] == "preserved"
    assert values["AIRFLOW_JWT_SECRET"] == "keep-me"
    assert len(bytes.fromhex(values["AIRFLOW_ADMIN_PASSWORD"])) == 16
    assert len(base64.urlsafe_b64decode(values["AIRFLOW_FERNET_KEY"])) == 32
    assert values["AIRFLOW_IMAGE"] == "omnichannel/airflow:3.3.2-spark3.5.5"
    runpy.run_path(str(script))
    assert path.read_text() == first
    assert path.stat().st_mode & 0o777 == 0o600
