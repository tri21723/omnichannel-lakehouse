import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("ci_events", Path("docker/batch/ci_events.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_failure_output_does_not_copy_credentials_or_payload():
    secret = "password-do-not-copy"
    lines = [
        f"Traceback: {secret}",
        json.dumps(
            {
                "event": "bronze_failed",
                "detail": secret,
                "error_type": "ValueError",
                "tables": {"_raw": secret},
                "reason": secret,
            }
        ),
        json.dumps({"event": "bronze_completed", "metrics": {"rows": 4, "password": secret}}),
    ]
    result = list(module.safe_events(lines))
    assert secret not in json.dumps(result)
    assert result[0] == {"event": "bronze_failed", "error_type": "ValueError"}
    assert result[1]["metrics"] == {"rows": 4}
