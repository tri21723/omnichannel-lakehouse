"""Reconcile Lakekeeper v0.9.3 warehouse; never overwrite an existing location."""

import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request(base, method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(
        base + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    for attempt in range(3):
        try:
            with urlopen(req, timeout=10) as response:
                body = response.read()
                return json.loads(body) if body else {}
        except HTTPError as error:
            raise CatalogError(error.code) from None
        except (URLError, TimeoutError):
            if method != "GET" or attempt == 2:
                raise RuntimeError("Catalog connection unavailable; retry task") from None
            time.sleep(1)


class CatalogError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"Catalog returned HTTP {status}; response body redacted")


def reconcile(runtime):
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
        "endpoint": os.environ["S3_ENDPOINT"].rstrip("/"),
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


def main(runtime=Path("/runtime")):
    for attempt in range(2):
        try:
            return reconcile(runtime)
        except CatalogError as error:
            if error.status != 409 or attempt == 1:
                raise
            # A concurrent bootstrap/create may have completed. Re-read before writing.


if __name__ == "__main__":
    main()
