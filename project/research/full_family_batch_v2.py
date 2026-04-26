from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.approval_registry_v2 import write_registry_status_artifacts
from project.research.approval_workflow_v2 import (
    ApprovalDecision,
    evaluate_approval_workflow,
)


@dataclass(frozen=True)
class FamilyBatchSpec:
    family_name: str
    event_type: str
    analyze_callable: Callable[..., tuple[pd.DataFrame, dict[str, Any]]]
    args: Sequence[Any] = field(default_factory=tuple)
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    current_status: str = "prototype"
    reference_events: pd.DataFrame | None = None
    overlap_threshold: float = 0.8
    min_events: int = 20


@dataclass(frozen=True)
class FamilyBatchResult:
    family_name: str
    event_type: str
    events: pd.DataFrame
    analyzer_results: dict[str, Any]
    approval_decision: ApprovalDecision


def run_family_approval_batch(
    specs: Sequence[FamilyBatchSpec],
    *,
    output_dir: str | Path | None = None,
) -> dict[str, FamilyBatchResult]:
    results: dict[str, FamilyBatchResult] = {}
    decisions: dict[str, ApprovalDecision] = {}
    for spec in specs:
        events, analyzer_results = spec.analyze_callable(*tuple(spec.args), **dict(spec.kwargs))
        decision = evaluate_approval_workflow(
            analyzer_results,
            current_status=spec.current_status,
            events=events,
            reference_events=spec.reference_events,
            overlap_threshold=spec.overlap_threshold,
            min_events=spec.min_events,
        )
        event_type = str(spec.event_type).strip().upper()
        results[event_type] = FamilyBatchResult(
            family_name=str(spec.family_name),
            event_type=event_type,
            events=events.copy() if isinstance(events, pd.DataFrame) else pd.DataFrame(),
            analyzer_results=dict(analyzer_results or {}),
            approval_decision=decision,
        )
        decisions[event_type] = decision
    if output_dir is not None:
        write_registry_status_artifacts(decisions, output_dir=output_dir)
    return results
