#!/bin/bash
# Rerun the whole governance refresh and require an identical DataHub inventory (URN-set hash and
# counts). Prints one JSON line; exits 1 when the inventory changed. Used by make gov-idempotency.
set -uo pipefail
before=$(make -s gov-inventory 2>/dev/null | tail -1)
make -s gov-refresh >/dev/null 2>&1 || { echo '{"event": "gov_idempotency", "ok": false, "stage": "refresh"}'; exit 1; }
after=$(make -s gov-inventory 2>/dev/null | tail -1)
python3 - "$before" "$after" <<'PY'
import json, sys
before, after = (json.loads(x) for x in sys.argv[1:3])
same = before == after
print(json.dumps({"event": "gov_idempotency", "ok": same, "urn_sha256": after.get("urn_sha256"),
                  "datasets": sum(after.get("datasets", {}).values()), "data_jobs": after.get("data_jobs")}))
sys.exit(0 if same else 1)
PY
