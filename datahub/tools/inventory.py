"""Print one JSON line with DataHub dataset/flow/job counts per platform and a hash of the URN set.

Two runs with the same hash show that an ingestion rerun created no new entity. The search index
is updated asynchronously after an ingestion, so the snapshot is taken once two reads 5 s apart
agree.
"""

from __future__ import annotations

import collections
import hashlib
import json
import re
import sys
import time

from tools.gms import all_urns

PLATFORM = re.compile(r"urn:li:dataPlatform:([^,]+)")


def snapshot() -> tuple[list[str], list[str], list[str]]:
    return all_urns("DATASET"), all_urns("DATA_FLOW"), all_urns("DATA_JOB")


def settled(attempts: int = 24, pause: float = 5.0) -> tuple[list[str], list[str], list[str]]:
    previous = snapshot()
    for _ in range(attempts):
        time.sleep(pause)
        current = snapshot()
        if current == previous:
            return current
        previous = current
    raise SystemExit("DataHub search index did not settle")


def main() -> int:
    datasets, flows, jobs = settled()
    per_platform = collections.Counter(PLATFORM.search(urn).group(1) for urn in datasets)
    urn_hash = hashlib.sha256("\n".join(datasets + flows + jobs).encode()).hexdigest()[:16]
    record = {
        "event": "datahub_inventory",
        "datasets": dict(sorted(per_platform.items())),
        "data_flows": len(flows),
        "data_jobs": len(jobs),
        "urn_sha256": urn_hash,
    }
    if "--list" in sys.argv:
        record["urns"] = datasets
    print(json.dumps(record))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
