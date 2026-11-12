from typer.testing import CliRunner

from generators.cli import app


def test_cli_batch_and_verify_with_overrides(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("orders_per_day_per_channel: 2\nchannels: [walmart]\n")
    common = [
        "--config",
        str(config),
        "--output",
        str(tmp_path / "out"),
        "--seed",
        "7",
        "--days",
        "1",
        "--start-date",
        "2026-02-01",
    ]
    runner = CliRunner()
    result = runner.invoke(app, ["batch", *common])
    assert result.exit_code == 0, result.exception
    result = runner.invoke(app, ["verify", *common])
    assert result.exit_code == 0, result.exception
    result = runner.invoke(app, ["verify", *common, "--seed", "8"])
    assert result.exit_code != 0
