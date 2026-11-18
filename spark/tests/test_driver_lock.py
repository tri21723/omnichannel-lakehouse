"""Unit tests for the shared Spark driver lock (ADR-0013 §3)."""

import json
import sys

import pytest

from spark.lib.driver_lock import EXIT_BUSY, DriverBusyError, driver_lock, main, run_locked


def test_run_locked_returns_child_exit_code(tmp_path):
    lock = tmp_path / "locks" / "driver.lock"
    assert run_locked([sys.executable, "-c", "raise SystemExit(3)"], lock) == 3
    assert run_locked([sys.executable, "-c", "pass"], lock) == 0
    assert json.loads(lock.read_text())["pid"] > 0


def test_second_driver_is_rejected_without_running(tmp_path, capsys):
    lock = tmp_path / "driver.lock"
    marker = tmp_path / "ran"
    with driver_lock(lock):
        with pytest.raises(DriverBusyError):
            with driver_lock(lock):
                pass
        code = run_locked([sys.executable, "-c", f"open({str(marker)!r}, 'w')"], lock)
    assert code == EXIT_BUSY
    assert not marker.exists()
    assert '"event": "spark_driver_busy"' in capsys.readouterr().out


def test_lock_is_released_after_holder_exits(tmp_path):
    lock = tmp_path / "driver.lock"
    with driver_lock(lock):
        pass
    with driver_lock(lock):
        pass
    # A crashed holder (child process killed) also releases the kernel lock.
    code = run_locked([sys.executable, "-c", "import os; os.kill(os.getpid(), 9)"], lock)
    assert code == -9
    assert run_locked([sys.executable, "-c", "pass"], lock) == 0


def test_cli_requires_command_and_strips_separator(tmp_path):
    lock = tmp_path / "driver.lock"
    assert main(["--lock", str(lock), "--", sys.executable, "-c", "raise SystemExit(5)"]) == 5
    with pytest.raises(SystemExit):
        main(["--lock", str(lock), "--"])


def test_wait_acquires_lock_released_by_another_process(tmp_path):
    import subprocess
    import time

    lock = tmp_path / "driver.lock"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "spark.lib.driver_lock",
            "--lock",
            str(lock),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(1.5)",
        ]
    )
    try:
        time.sleep(0.5)
        started = time.monotonic()
        assert run_locked([sys.executable, "-c", "pass"], lock, wait=10) == 0
        assert time.monotonic() - started >= 0.5
    finally:
        holder.wait()


def test_wait_times_out_with_busy_exit(tmp_path):
    lock = tmp_path / "driver.lock"
    with driver_lock(lock):
        assert run_locked([sys.executable, "-c", "pass"], lock, wait=0.3) == EXIT_BUSY
        assert main(["--lock", str(lock), "--wait", "0.2", "--", sys.executable, "-c", "pass"]) == (
            EXIT_BUSY
        )
