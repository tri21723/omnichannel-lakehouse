import runpy
from pathlib import Path


def run(script, tmp_path, monkeypatch, content):
    script = Path(script).resolve()  # before chdir
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(content)
    runpy.run_path(str(script))
    return dict(line.split("=", 1) for line in (tmp_path / ".env").read_text().splitlines())


def test_empty_values_from_env_example_are_filled_and_kept(tmp_path, monkeypatch):
    script = Path("docker/governance/env.py").resolve()
    values = run(script, tmp_path, monkeypatch, "DATAHUB_SECRET=\nDATAHUB_ADMIN_PASSWORD=keep-me\n")
    assert len(values["DATAHUB_SECRET"]) == 64  # was empty in .env.example
    assert values["DATAHUB_ADMIN_PASSWORD"] == "keep-me"
    assert values["DATAHUB_VERSION"] == "v1.7.0.1"
    first = (tmp_path / ".env").read_text()
    runpy.run_path(str(script))
    assert (tmp_path / ".env").read_text() == first  # no rotation
    assert (tmp_path / ".local-data/governance/user.props").read_text() == "datahub:keep-me\n"
    assert first.count("DATAHUB_SECRET=") == 1


def test_stream_env_fills_an_empty_cluster_id(tmp_path, monkeypatch):
    values = run(
        "docker/stream/env.py", tmp_path, monkeypatch, "KAFKA_CLUSTER_ID=\nFLINK_S3_SECRET_KEY=x\n"
    )
    assert len(values["KAFKA_CLUSTER_ID"]) == 22
    assert values["FLINK_S3_SECRET_KEY"] == "x"
