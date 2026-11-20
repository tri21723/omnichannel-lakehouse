"""Minimal GraphQL client for the network-internal GMS (ADR-0018 §2: no host port, no auth)."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


def graphql(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    url = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080") + "/api/graphql"
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")
    return payload["data"]


SCROLL = """
query scroll($types: [EntityType!], $scrollId: String) {
  scrollAcrossEntities(input: {types: $types, query: "*", count: 200, scrollId: $scrollId}) {
    nextScrollId
    searchResults { entity { urn } }
  }
}
"""


def all_urns(entity_type: str) -> list[str]:
    urns: list[str] = []
    scroll_id = None
    while True:
        variables = {"types": [entity_type], "scrollId": scroll_id}
        page = graphql(SCROLL, variables)["scrollAcrossEntities"]
        urns += [result["entity"]["urn"] for result in page["searchResults"]]
        scroll_id = page["nextScrollId"]
        if not scroll_id:
            return sorted(set(urns))
