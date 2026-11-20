"""Apply datahub/metadata/metadata.yml to DataHub (ADR-0018 §5).

`plan()` is pure: it maps every catalogued dataset (`<platform>:<name>`) to tags, glossary terms,
domain, description and column descriptions. `apply()` upserts tag/domain/glossary entities and,
per dataset, the aspects owned by metadata-as-code (globalTags, glossaryTerms, domains, ownership,
editableDatasetProperties, editableSchemaMetadata). Ingestion recipes never write those aspects, and
every value is deterministic (sorted, fixed audit time), so a rerun writes identical aspects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

import yaml

AUDIT_TIME = 0
ACTOR = "urn:li:corpuser:datahub"
METADATA = Path(__file__).with_name("metadata.yml")


def key(urn: str) -> str:
    """Dataset URN → `<platform>:<name>`, e.g. `iceberg:gold.fact_orders`."""
    platform, name = urn.split("urn:li:dataPlatform:", 1)[1].split(",")[:2]
    return f"{platform}:{name}"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def term_urn(term: str) -> str:
    return f"urn:li:glossaryTerm:omnichannel.{slug(term)}"


def load(path: Path = METADATA) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def plan(metadata: dict[str, Any], keys: list[str]) -> dict[str, dict[str, Any]]:
    result = {}
    for name in sorted(keys):
        entry: dict[str, Any] = {"tags": set(), "terms": set(), "domain": None, "description": None}
        for rule in metadata["rules"]:
            patterns = rule["match"] if isinstance(rule["match"], list) else [rule["match"]]
            if not any(re.fullmatch(pattern, name) for pattern in patterns):
                continue
            entry["tags"].update(rule.get("tags", []))
            entry["terms"].update(rule.get("terms", []))
            entry["domain"] = entry["domain"] or rule.get("domain")
            entry["description"] = entry["description"] or rule.get("description")
        columns: dict[str, str] = {}
        for pattern, fields in metadata.get("columns", {}).items():
            if re.fullmatch(pattern, name):
                columns.update(fields)
        unknown = (entry["tags"] - set(metadata["tags"])) | (
            entry["terms"] - set(metadata["glossary"]["terms"])
        )
        if unknown or (entry["domain"] and entry["domain"] not in metadata["domains"]):
            raise ValueError(f"{name}: undefined tag/term/domain {sorted(unknown)}")
        result[name] = {
            "tags": sorted(entry["tags"]),
            "terms": sorted(entry["terms"]),
            "domain": entry["domain"],
            "description": entry["description"],
            "columns": dict(sorted(columns.items())),
        }
    return result


def digest(planned: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(planned, sort_keys=True).encode()).hexdigest()[:16]


def apply(
    metadata: dict[str, Any], urns: list[str], fields: dict[str, list[str]], server: str
) -> None:
    from tools import aspects

    from datahub.emitter.mcp import MetadataChangeProposalWrapper as Mcp
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata import schema_classes as s

    stamp = s.AuditStampClass(time=AUDIT_TIME, actor=ACTOR)
    owner = f"urn:li:corpuser:{metadata['owner']}"
    node = "urn:li:glossaryNode:omnichannel"
    proposals = [
        Mcp(
            entityUrn=node,
            aspect=s.GlossaryNodeInfoClass(
                name=metadata["glossary"]["node"]["name"],
                definition=metadata["glossary"]["node"]["description"],
            ),
        )
    ]
    for name, description in sorted(metadata["tags"].items()):
        proposals.append(
            Mcp(
                entityUrn=f"urn:li:tag:{name}",
                aspect=s.TagPropertiesClass(name=name, description=description),
            )
        )
    for domain_id, domain in sorted(metadata["domains"].items()):
        proposals.append(
            Mcp(
                entityUrn=f"urn:li:domain:{domain_id}",
                aspect=s.DomainPropertiesClass(
                    name=domain["name"], description=domain["description"]
                ),
            )
        )
    for term, definition in sorted(metadata["glossary"]["terms"].items()):
        proposals.append(
            Mcp(
                entityUrn=term_urn(term),
                aspect=s.GlossaryTermInfoClass(
                    name=term, definition=definition, termSource="INTERNAL", parentNode=node
                ),
            )
        )
    by_key = {key(urn): urn for urn in urns}
    for name, item in plan(metadata, list(by_key)).items():
        urn = by_key[name]
        proposals += [
            Mcp(
                entityUrn=urn,
                aspect=s.GlobalTagsClass(
                    tags=[s.TagAssociationClass(tag=f"urn:li:tag:{t}") for t in item["tags"]]
                ),
            ),
            Mcp(
                entityUrn=urn,
                aspect=s.GlossaryTermsClass(
                    terms=[s.GlossaryTermAssociationClass(urn=term_urn(t)) for t in item["terms"]],
                    auditStamp=stamp,
                ),
            ),
            Mcp(
                entityUrn=urn,
                aspect=aspects.ownership(owner),
            ),
            Mcp(
                entityUrn=urn,
                aspect=aspects.domains(
                    [f"urn:li:domain:{item['domain']}"] if item["domain"] else []
                ),
            ),
        ]
        if item["description"]:
            proposals.append(
                Mcp(
                    entityUrn=urn,
                    aspect=s.EditableDatasetPropertiesClass(
                        description=item["description"], created=stamp, lastModified=stamp
                    ),
                )
            )
        paths = {path.rsplit(".", 1)[-1]: path for path in fields.get(urn, [])}
        columns = [
            s.EditableSchemaFieldInfoClass(fieldPath=paths[col], description=text)
            for col, text in item["columns"].items()
            if col in paths
        ]
        if columns:
            proposals.append(
                Mcp(
                    entityUrn=urn,
                    aspect=s.EditableSchemaMetadataClass(
                        editableSchemaFieldInfo=columns, created=stamp, lastModified=stamp
                    ),
                )
            )
    emitter = DatahubRestEmitter(server)
    for proposal in proposals:
        emitter.emit_mcp(proposal)
    emitter.flush()


FIELDS = (
    """query f($urn: String!) { dataset(urn: $urn) { schemaMetadata { fields { fieldPath } } } }"""
)
LIVE = """query d($urn: String!) { dataset(urn: $urn) {
  tags { tags { tag { urn } } }
  glossaryTerms { terms { term { urn } } }
  domain { domain { urn } }
  ownership { owners { owner { ... on CorpUser { urn } } } }
  editableProperties { description }
  editableSchemaMetadata { editableSchemaFieldInfo { fieldPath description } }
} }"""


def live_state(urn: str, graphql) -> dict[str, Any]:
    d = graphql(LIVE, {"urn": urn})["dataset"]
    domain = (d.get("domain") or {}).get("domain") or {}
    columns = (d.get("editableSchemaMetadata") or {}).get("editableSchemaFieldInfo") or []
    tags = (d.get("tags") or {}).get("tags") or []
    terms = (d.get("glossaryTerms") or {}).get("terms") or []
    owners = (d.get("ownership") or {}).get("owners") or []
    return {
        "tags": sorted(t["tag"]["urn"].removeprefix("urn:li:tag:") for t in tags),
        "terms": sorted(t["term"]["urn"] for t in terms),
        "domain": domain.get("urn", "").removeprefix("urn:li:domain:") or None,
        "owners": sorted(o["owner"]["urn"] for o in owners),
        "description": (d.get("editableProperties") or {}).get("description"),
        "columns": {c["fieldPath"].rsplit(".", 1)[-1]: c["description"] for c in columns},
    }


def drift(metadata: dict[str, Any], planned: dict[str, Any], urns: list[str], graphql) -> list[str]:
    """Datasets whose live aspects differ from the plan (empty list = in sync)."""
    owner = f"urn:li:corpuser:{metadata['owner']}"
    problems = []
    for urn in urns:
        want = planned[key(urn)]
        have = live_state(urn, graphql)
        expected = {
            "tags": want["tags"],
            "terms": sorted(term_urn(t) for t in want["terms"]),
            "domain": want["domain"],
            "owners": [owner],
            "description": want["description"],
        }
        diff = [field for field, value in expected.items() if have[field] != value]
        known = {c: text for c, text in want["columns"].items() if c in have["columns"]}
        if any(have["columns"][c] != text for c, text in known.items()):
            diff.append("columns")
        if diff:
            problems.append(f"{key(urn)}: {','.join(diff)}")
    return problems


def main() -> int:
    from tools.gms import all_urns, graphql

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--verify", action="store_true", help="compare live aspects; exit 1 on drift"
    )
    args = parser.parse_args()
    metadata = load()
    urns = all_urns("DATASET")
    planned = plan(metadata, [key(u) for u in urns])
    summary = {
        "event": "metadata_planned",
        "datasets": len(planned),
        "pii": sorted(k for k, v in planned.items() if "PII" in v["tags"]),
        "without_domain": sorted(k for k, v in planned.items() if not v["domain"]),
        "metadata_sha256": digest(planned),
    }
    if args.verify:
        problems = drift(metadata, planned, urns, graphql)
        summary.update(event="metadata_verified", ok=not problems, drift=problems)
        print(json.dumps(summary))
        return 0 if not problems else 1
    if not args.dry_run:
        fields = {}
        for urn in urns:
            if any(key(urn) == k and v["columns"] for k, v in planned.items()):
                schema = graphql(FIELDS, {"urn": urn})["dataset"]["schemaMetadata"] or {
                    "fields": []
                }
                fields[urn] = [f["fieldPath"] for f in schema["fields"]]
        apply(metadata, urns, fields, os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080"))
        summary["event"] = "metadata_applied"
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
