"""
Canonical multiplicity scope contract.

Defines the scope keys and grouping logic for cross-campaign / campaign-lineage
FDR adjustment. Every promoted artifact must have an explicit multiplicity scope.

Phase 1 invariant:
    No promoted, seed-promoted, or deployment-exported thesis may exist without
    an explicit multiplicity scope and statistical-regime stamp.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


SCOPE_VERSION_PHASE1_V1 = "phase1_v1"


@dataclass(frozen=True)
class MultiplicityScope:
    run_id: str
    campaign_id: str
    program_id: str
    concept_lineage_key: str
    family_id: str
    side_policy: str
    scope_version: str = SCOPE_VERSION_PHASE1_V1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "campaign_id": self.campaign_id,
            "program_id": self.program_id,
            "concept_lineage_key": self.concept_lineage_key,
            "family_id": self.family_id,
            "side_policy": self.side_policy,
            "scope_version": self.scope_version,
        }


def resolve_campaign_scope_key(row: Dict[str, Any]) -> str:
    campaign_id = str(row.get("campaign_id", "") or "").strip()
    program_id = str(row.get("program_id", "") or "").strip()
    run_id = str(row.get("run_id", "") or "").strip()
    
    if campaign_id:
        return f"campaign::{campaign_id}"
    if program_id:
        return f"program::{program_id}"
    return f"run::{run_id}"


def resolve_lineage_scope_key(row: Dict[str, Any]) -> str:
    campaign_id = str(row.get("campaign_id", "") or "").strip()
    program_id = str(row.get("program_id", "") or "").strip()
    lineage_key = str(row.get("concept_lineage_key", "") or row.get("hypothesis_id", "") or "").strip()
    run_id = str(row.get("run_id", "") or "").strip()
    
    parts = []
    if campaign_id:
        parts.append(f"campaign::{campaign_id}")
    elif program_id:
        parts.append(f"program::{program_id}")
    else:
        parts.append(f"run::{run_id}")
    
    if lineage_key:
        parts.append(f"lineage::{lineage_key}")
    
    return "/".join(parts) if parts else "unknown_scope"


def resolve_effective_scope_key(
    row: Dict[str, Any],
    mode: str = "campaign_lineage",
) -> str:
    mode = str(mode or "campaign_lineage").strip().lower()
    
    if mode == "run":
        return f"run::{row.get('run_id', 'unknown')}"
    
    if mode == "campaign":
        return resolve_campaign_scope_key(row)
    
    if mode == "program":
        program_id = str(row.get("program_id", "") or "").strip()
        if program_id:
            return f"program::{program_id}"
        campaign_id = str(row.get("campaign_id", "") or "").strip()
        if campaign_id:
            return f"campaign::{campaign_id}"
        return f"run::{row.get('run_id', 'unknown')}"
    
    if mode in ("campaign_lineage", "lineage"):
        return resolve_lineage_scope_key(row)
    
    return resolve_campaign_scope_key(row)


def infer_multiplicity_scope(
    row: Dict[str, Any],
    scope_version: str = SCOPE_VERSION_PHASE1_V1,
) -> MultiplicityScope:
    return MultiplicityScope(
        run_id=str(row.get("run_id", "") or "").strip(),
        campaign_id=str(row.get("campaign_id", "") or "").strip(),
        program_id=str(row.get("program_id", "") or "").strip(),
        concept_lineage_key=str(
            row.get("concept_lineage_key", "") or row.get("hypothesis_id", "") or ""
        ).strip(),
        family_id=str(row.get("family_id", "") or "").strip(),
        side_policy=str(row.get("side_policy", "directional") or "directional").strip().lower(),
        scope_version=scope_version,
    )


__all__ = [
    "MultiplicityScope",
    "SCOPE_VERSION_PHASE1_V1",
    "infer_multiplicity_scope",
    "resolve_campaign_scope_key",
    "resolve_lineage_scope_key",
    "resolve_effective_scope_key",
]