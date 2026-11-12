"""Batch/load CLI. Logs never contain payloads or credentials."""

import json
import logging
import os
from pathlib import Path
from typing import Annotated

import typer

from generators.batch.dataset import generate, verify_local
from generators.common.config import read_config
from generators.common.storage import publish, s3_client, verify_remote
from generators.load.postgres import connect, load

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
log = logging.getLogger("generators")
ConfigPath = Annotated[Path, typer.Option(exists=True, dir_okay=False)]


def event(name: str, **fields: object) -> None:
    log.info(json.dumps({"event": name, **fields}, sort_keys=True))


def dataset(
    config: Path, output: Path, seed: int | None, start_date: str | None, days: int | None
) -> Path:
    cfg = read_config(config, seed=seed, start_date=start_date, days=days)
    root = generate(cfg, output)
    event("dataset_ready", run_id=cfg.run_id)
    return root


@app.command()
def batch(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
    upload: bool = False,
) -> None:
    """Generate JSONL plus manifest; optionally publish to S3."""
    root = dataset(config, output, seed, start_date, days)
    if upload:
        manifest = publish(root, s3_client(), os.environ["S3_RAW_BUCKET"])
        event(
            "s3_published",
            run_id=manifest["run_id"],
            rows={name: data["rows"] for name, data in manifest["channels"].items()},
        )


@app.command("load")
def load_command(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
) -> None:
    """Load unique Walmart entities from the generated JSONL into PostgreSQL."""
    root = dataset(config, output, seed, start_date, days)
    with connect() as connection:
        result = load(root, connection)
    event("walmart_loaded", run_id=root.name, tables=result)


@app.command()
def sources(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
) -> None:
    """Publish batch and load Walmart using the same dataset."""
    batch(config, output, seed, start_date, days, True)
    load_command(config, output, seed, start_date, days)


@app.command()
def verify(
    config: ConfigPath = Path("generators/config.yaml"),
    output: Path = Path(".local-data/generated"),
    seed: int | None = None,
    start_date: str | None = None,
    days: int | None = None,
    remote: bool = False,
    database: bool = False,
) -> None:
    """Reconcile existing data; never regenerate or upsert business records."""
    cfg = read_config(config, seed=seed, start_date=start_date, days=days)
    root = output / cfg.run_id
    verify_local(root)
    if remote:
        verify_remote(root, s3_client(), os.environ["S3_RAW_BUCKET"])
    if database:
        with connect() as connection:
            result = load(root, connection, verify_only=True)
        event("walmart_verified", run_id=cfg.run_id, tables=result)
    event("reconciliation_passed", run_id=cfg.run_id, remote=remote, database=database)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        app()
    except Exception as exc:
        # Driver exception messages may contain connection details; do not log them.
        event("generator_failed", error_type=type(exc).__name__)
        raise SystemExit(1) from None
