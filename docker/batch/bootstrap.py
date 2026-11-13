"""Reconcile Lakekeeper v0.9.3 warehouse; never overwrite an existing location."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def request(base, method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(
        base + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urlopen(req, timeout=30) as response:
            body = response.read()
            return json.loads(body) if body else {}
    except HTTPError as error:
        # API response bodies can include storage credentials; never log them.
        raise RuntimeError(f"Catalog {method} {path} returned HTTP {error.code}") from None


def main(runtime=Path("/runtime")):
    base = os.environ["LAKEKEEPER_URI"]
    info = request(base, "GET", "/management/v1/info")
    if not info["bootstrapped"]:
        request(base, "POST", "/management/v1/bootstrap", {"accept-terms-of-use": True})
        info = request(base, "GET", "/management/v1/info")
    project = info["default-project-id"]
    profile = {
        "type": "s3",
        "bucket": os.environ["S3_LAKEHOUSE_BUCKET"],
        "key-prefix": "warehouse",
        "endpoint": os.environ["S3_ENDPOINT"],
        "region": os.environ["AWS_DEFAULT_REGION"],
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": False,
    }
    warehouses = request(base, "GET", f"/management/v1/warehouse?project-id={project}")
    matching = [
        w for w in warehouses["warehouses"] if w["name"] == os.environ["LAKEHOUSE_WAREHOUSE"]
    ]
    if len(matching) > 1:
        raise ValueError("Duplicate warehouse names")
    if not matching:
        created = request(
            base,
            "POST",
            "/management/v1/warehouse",
            {
                "warehouse-name": os.environ["LAKEHOUSE_WAREHOUSE"],
                "project-id": project,
                "storage-profile": profile,
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "aws-access-key-id": os.environ["LAKEKEEPER_S3_ACCESS_KEY"],
                    "aws-secret-access-key": os.environ["LAKEKEEPER_S3_SECRET_KEY"],
                },
            },
        )
        warehouse_id = created["warehouse-id"]
    else:
        warehouse_id = matching[0]["id"]
    actual = request(base, "GET", f"/management/v1/warehouse/{warehouse_id}")["storage-profile"]
    actual["endpoint"] = actual["endpoint"].rstrip("/")
    if any(actual.get(key) != value for key, value in profile.items()):
        raise ValueError("Existing warehouse storage differs; refusing to modify it")
    config = {
        "warehouse_id": warehouse_id,
        "project_id": project,
        "warehouse_name": os.environ["LAKEHOUSE_WAREHOUSE"],
    }
    target = runtime / "catalog.json"
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(config))
    temp.replace(target)
    print(json.dumps({"event": "catalog_ready", **config}))


if __name__ == "__main__":
    main()
