from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set


@dataclass(frozen=True)
class AdmissionResult:
    admissible: bool
    reason: str
    winner_id: str | None = None


class PortfolioAdmissionPolicy:
    """Shared thesis/family admission rules used by engine and live runtime."""

    def __init__(self, family_budgets: Dict[str, float] | None = None):
        self.family_budgets = family_budgets or {}

    def resolve_overlap_winners(
        self,
        candidates: List[Dict[str, Any]],
        active_groups: Set[str],
    ) -> List[Dict[str, Any]]:
        def ranking_key(candidate: Dict[str, Any]) -> tuple[float, int, str]:
            return (
                float(candidate.get("support_score", 0.0))
                - float(candidate.get("contradiction_penalty", 0.0)),
                int(candidate.get("sample_size", 0)),
                str(candidate.get("thesis_id", "")),
            )

        eligible = [
            candidate
            for candidate in candidates
            if str(candidate.get("overlap_group_id", "")).strip() not in active_groups
        ]

        if not eligible:
            return []

        sorted_candidates = sorted(eligible, key=ranking_key, reverse=True)
        winners: List[Dict[str, Any]] = []
        seen_in_batch: set[str] = set()

        for candidate in sorted_candidates:
            group_id = str(candidate.get("overlap_group_id", "")).strip()
            if not group_id or group_id not in seen_in_batch:
                if group_id:
                    seen_in_batch.add(group_id)
                winners.append(candidate)
        return winners

    def is_thesis_admissible(
        self,
        thesis_id: str,
        overlap_group_id: str,
        active_groups: Set[str],
    ) -> AdmissionResult:
        group_id = str(overlap_group_id).strip()
        if group_id and group_id in active_groups:
            return AdmissionResult(False, "blocked_by_active_group_member")
        return AdmissionResult(True, "selected_as_group_winner", winner_id=thesis_id)

    def is_family_admissible(
        self,
        family: str,
        family_exposures: Dict[str, float],
    ) -> AdmissionResult:
        budget = self.family_budgets.get(family, 0.0)
        if budget <= 0.0:
            return AdmissionResult(True, "no_family_budget_limit")

        current_exposure = abs(family_exposures.get(family, 0.0))
        if current_exposure >= budget:
            return AdmissionResult(
                False,
                f"family_budget_exhausted:{family}:{current_exposure:.2f}>={budget:.2f}",
            )

        return AdmissionResult(True, "family_budget_available")
