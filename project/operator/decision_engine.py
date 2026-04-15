from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DecisionResult:
    action: str
    reason: str
    confidence: float
    classification: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "confidence": float(self.confidence),
            "classification": self.classification,
        }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def decide_next_action(*, run_summary: dict[str, Any], diagnostics: dict[str, Any] | None = None) -> DecisionResult:
    diagnostics = diagnostics or {}
    terminal_status = str(run_summary.get("terminal_status", "") or "").strip().lower()
    mechanical_outcome = str(run_summary.get("mechanical_outcome", "") or "").strip().lower()
    verdict = str(run_summary.get("verdict", "") or "").strip().upper()
    promoted_count = int(run_summary.get("promoted_count", 0) or 0)
    candidate_count = int(run_summary.get("candidate_count", 0) or 0)
    metric_value = _safe_float(((run_summary.get("top_candidate", {}) or {}).get("metric_value")), 0.0)
    diagnosis = str(diagnostics.get("diagnosis", "") or "").strip().lower()
    regime_classification = str((run_summary.get("regime_split_report", {}) or {}).get("classification", "") or "").strip().lower()

    if verdict == "PROMOTE" or promoted_count > 0:
        return DecisionResult(
            action="PROMOTE",
            reason="Run produced at least one promoted candidate.",
            confidence=0.95,
            classification="pass",
        )

    if (
        terminal_status in {"failed_mechanical", "failed_data_quality", "failed_runtime_invariants"}
        or mechanical_outcome in {"mechanical_failure", "artifact_contract_failure", "data_quality_failure"}
        or diagnosis == "mechanical_artifact_gap"
    ):
        return DecisionResult(
            action="REPAIR",
            reason="Run failed for mechanical or artifact-contract reasons.",
            confidence=0.9,
            classification="mechanical",
        )

    if diagnosis in {"regime_instability"} or regime_classification == "regime_instability":
        return DecisionResult(
            action="MODIFY",
            reason="Signal appears regime-sensitive; next step should be one bounded regime-aware mutation.",
            confidence=0.8,
            classification="near_miss",
        )

    if diagnosis in {"low_sample_power"}:
        return DecisionResult(
            action="MODIFY",
            reason="Evidence is inconclusive because sample size is too small.",
            confidence=0.7,
            classification="near_miss",
        )

    if abs(metric_value) >= 1.5 and candidate_count > 0:
        return DecisionResult(
            action="MODIFY",
            reason="Leading metric is non-trivial but below promotion quality; continue with one bounded refinement.",
            confidence=0.7,
            classification="near_miss",
        )

    return DecisionResult(
        action="STOP",
        reason="Run did not produce strong enough evidence to justify another mutation on this branch.",
        confidence=0.85,
        classification="fail",
    )
