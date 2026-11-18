"""Serialize Spark drivers across `make` and Airflow (ADR-0013 §3).

Usage: ``python -m spark.lib.driver_lock [--lock PATH] -- <command> [args...]``

Takes a non-blocking exclusive ``flock`` on a file in the shared ``/runtime`` volume, runs
the command, and returns its exit code. If another driver holds the lock, exits with
``EXIT_BUSY`` (75, EX_TEMPFAIL) without starting the command. The kernel releases the lock
when the holder exits, including on crash.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import subprocess
import sys
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path

DEFAULT_LOCK = Path(os.environ.get("SPARK_DRIVER_LOCK", "/runtime/locks/spark-driver.lock"))
EXIT_BUSY = 75


class DriverBusyError(RuntimeError):
    """Raised when another Spark driver holds the lock."""


@contextmanager
def driver_lock(path: Path = DEFAULT_LOCK):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise DriverBusyError(f"Spark driver lock is held: {path}") from None
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(json.dumps({"pid": os.getpid()}) + "\n")
            handle.flush()
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_locked(command: Sequence[str], lock: Path = DEFAULT_LOCK) -> int:
    """Run ``command`` while holding the driver lock; forward SIGTERM/SIGINT to it."""
    if not command:
        raise ValueError("no command given")
    try:
        with driver_lock(lock):
            child = subprocess.Popen(list(command))

            def forward(signum, _frame):
                child.send_signal(signum)

            previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGTERM, signal.SIGINT)}
            try:
                return child.wait()
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
    except DriverBusyError as exc:
        print(json.dumps({"event": "spark_driver_busy", "lock": str(lock)}), flush=True)
        print(str(exc), file=sys.stderr, flush=True)
        return EXIT_BUSY


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("missing command after --")
    return run_locked(command, args.lock)


if __name__ == "__main__":
    sys.exit(main())
