from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from project.events.event_aliases import resolve_event_alias
from project.events.registry import get_event_definition
from project.research.analyzers.overlap import OverlapAnalyzer


AUDITED_PROXY_EVENTS = {}


@dataclass(frozen=True)
class CanonicalAuditRow:
    source_event_type: str
    canonical_event_type: str
    prior_status: str
    recommended_status: str
    evidence_tier: str
    reason: str


def audit_canonical_event_types(event_types: Iterable[str]) -> list[CanonicalAuditRow]:
    rows: list[CanonicalAuditRow] = []
    for event_type in event_types:
        source = str(event_type).strip().upper()
        canonical = resolve_event_alias(source)
        definition = get_event_definition(source) or {}
        evidence_tier = str(definition.get("evidence_tier", "proxy"))
        prior_status = str(definition.get("status", "prototype"))
        if source in AUDITED_PROXY_EVENTS:
            rows.append(
                CanonicalAuditRow(
                    source,
                    canonical,
                    prior_status,
                    "deprecated",
                    evidence_tier,
                    "legacy heuristic renamed to explicit proxy taxonomy",
                )
            )
        else:
            rows.append(
                CanonicalAuditRow(
                    source,
                    canonical,
                    prior_status,
                    prior_status,
                    evidence_tier,
                    "no canonical remap required",
                )
            )
    return rows


def build_canonical_audit_frame(event_types: Iterable[str]) -> pd.DataFrame:
    rows = [row.__dict__ for row in audit_canonical_event_types(event_types)]
    return pd.DataFrame(
        rows,
        columns=[
            "source_event_type",
            "canonical_event_type",
            "prior_status",
            "recommended_status",
            "evidence_tier",
            "reason",
        ],
    )


def redundancy_report(
    events: pd.DataFrame,
    reference_events: pd.DataFrame | None = None,
    *,
    overlap_threshold: float = 0.8,
) -> dict[str, float | bool | int]:
    if (
        reference_events is None
        or events is None
        or getattr(events, "empty", True)
        or getattr(reference_events, "empty", True)
    ):
        return {"jaccard_overlap": 0.0, "exact_overlap_count": 0, "redundant": False}
    result = OverlapAnalyzer().analyze(events, reference_events=reference_events)
    jaccard = float(result.summary.get("jaccard_overlap", 0.0) or 0.0)
    return {
        "jaccard_overlap": jaccard,
        "exact_overlap_count": int(result.summary.get("exact_overlap_count", 0) or 0),
        "redundant": bool(jaccard >= float(overlap_threshold)),
    }
