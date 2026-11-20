"""Aspects in the normalized form GMS stores, so an unchanged upsert creates no new version.

GMS enriches `ownership` (ownerTypes, lastModified) and `domains` (domainAssociations) on write;
sending the bare form makes every rerun look like a change. Requires the DataHub SDK.
"""

from __future__ import annotations

from typing import Any

TECHNICAL_OWNER_TYPE = "urn:li:ownershipType:__system__technical_owner"
UNKNOWN_ACTOR = "urn:li:corpuser:unknown"


def ownership(owner: str) -> Any:
    from datahub.metadata import schema_classes as s

    return s.OwnershipClass(
        owners=[s.OwnerClass(owner=owner, type=s.OwnershipTypeClass.TECHNICAL_OWNER)],
        ownerTypes={TECHNICAL_OWNER_TYPE: [owner]},
        lastModified=s.AuditStampClass(time=0, actor=UNKNOWN_ACTOR),
    )


def domains(urns: list[str]) -> Any:
    from datahub.metadata import schema_classes as s

    return s.DomainsClass(
        domains=urns, domainAssociations=[s.DomainAssociationClass(domain=u) for u in urns]
    )
