from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from project.events.canonical_audit import redundancy_report
from project.research.promotion_gates_v2 import evaluate_family_promotion


@dataclass(frozen=True)
class ApprovalDecision:
    current_status: str
    recommended_status: str
    approved: bool
    reasons: list[str]
    metrics: dict[str, Any]


def evaluate_approval_workflow(
    analyzer_results: dict[str, Any],
    *,
    current_status: str = "prototype",
    events=None,
    reference_events=None,
    overlap_threshold: float = 0.8,
    min_events: int = 20,
) -> ApprovalDecision:
    promotion = evaluate_family_promotion(analyzer_results, min_events=min_events)
    overlap_metrics = redundancy_report(
        events, reference_events, overlap_threshold=overlap_threshold
    )
    reasons = list(promotion.reasons)
    metrics = dict(promotion.metrics)
    metrics.update(overlap_metrics)
    recommended = (
        "approved"
        if promotion.passed
        else "validated"
        if metrics.get("pit_ok", False) and metrics.get("n_events", 0) >= max(5, min_events // 2)
        else "prototype"
    )
    if overlap_metrics.get("redundant"):
        recommended = "deprecated"
        reasons.append(
            f"redundant with reference family: jaccard {float(overlap_metrics['jaccard_overlap']):.3f} >= {float(overlap_threshold):.3f}"
        )
    approved = recommended == "approved"
    return ApprovalDecision(
        current_status=str(current_status),
        recommended_status=recommended,
        approved=approved,
        reasons=reasons,
        metrics=metrics,
    )
