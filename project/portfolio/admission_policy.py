from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AdmissionResult:
    admissible: bool
    reason: str
    winner_id: str | None = None


@dataclass(frozen=True)
class ConflictResolution:
    """Portfolio-level arbitration result for overlapping/opposing thesis signals."""

    decision: str  # accept, reject, reduce_size, net_exposure, defer_until_confirmation
    reason: str
    accepted_ids: list[str]
    rejected_ids: list[str]
    size_scalars: dict[str, float]


class PortfolioAdmissionPolicy:
    """Shared thesis/family admission rules used by engine and live runtime."""

    def __init__(self, family_budgets: dict[str, float] | None = None):
        self.family_budgets = family_budgets or {}

    def resolve_overlap_winners(
        self,
        candidates: list[dict[str, Any]],
        active_groups: set[str],
    ) -> list[dict[str, Any]]:
        def ranking_key(candidate: dict[str, Any]) -> tuple[float, int, str]:
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
        winners: list[dict[str, Any]] = []
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
        active_groups: set[str],
    ) -> AdmissionResult:
        group_id = str(overlap_group_id).strip()
        if group_id and group_id in active_groups:
            return AdmissionResult(False, "blocked_by_active_group_member")
        return AdmissionResult(True, "selected_as_group_winner", winner_id=thesis_id)

    def is_family_admissible(
        self,
        family: str,
        family_exposures: dict[str, float],
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

    def resolve_signal_conflicts(
        self,
        candidates: list[dict[str, Any]],
        *,
        active_groups: set[str] | None = None,
        guard_veto_families: set[str] | None = None,
    ) -> ConflictResolution:
        """Resolve same-symbol/timeframe/horizon conflicts before order admission.

        Candidate dictionaries may contain:
        thesis_id, symbol, timeframe, direction, horizon_bars, family, support_score,
        contradiction_penalty, overlap_group_id, guard_veto.

        Policy:
        - any explicit guard_veto rejects all alpha candidates for that symbol/timeframe.
        - active overlap groups are rejected.
        - opposing directions inside the same symbol/timeframe/horizon bucket choose the
          highest risk-adjusted support candidate and reject the rest.
        - same-side candidates in a bucket are accepted with concentration downscaling.
        """
        active_groups = active_groups or set()
        guard_veto_families = guard_veto_families or {"EXECUTION_FRICTION", "LIQUIDITY_STRESS"}

        if not candidates:
            return ConflictResolution("accept", "no_candidates", [], [], {})

        guard_veto = any(
            bool(c.get("guard_veto", False))
            or str(c.get("family", "")).strip().upper() in guard_veto_families
            and str(c.get("direction", "")).strip().lower() in {"block", "veto", "no_trade"}
            for c in candidates
        )
        if guard_veto:
            ids = [str(c.get("thesis_id", "")) for c in candidates if c.get("thesis_id")]
            return ConflictResolution("reject", "guard_veto_present", [], ids, {})

        eligible = self.resolve_overlap_winners(candidates, active_groups)
        rejected_by_overlap = [
            str(c.get("thesis_id", ""))
            for c in candidates
            if c not in eligible and c.get("thesis_id")
        ]
        if not eligible:
            return ConflictResolution(
                "reject",
                "all_candidates_blocked_by_overlap",
                [],
                rejected_by_overlap,
                {},
            )

        def score(c: dict[str, Any]) -> float:
            return float(c.get("support_score", 0.0)) - float(c.get("contradiction_penalty", 0.0))

        def bucket_key(c: dict[str, Any]) -> tuple[str, str, int]:
            return (
                str(c.get("symbol", "")).strip().upper(),
                str(c.get("timeframe", "")).strip(),
                int(c.get("horizon_bars", 0) or 0),
            )

        buckets: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
        for c in eligible:
            buckets.setdefault(bucket_key(c), []).append(c)

        accepted: list[str] = []
        rejected: list[str] = list(rejected_by_overlap)
        scalars: dict[str, float] = {}
        any_conflict = False
        any_downscale = False

        for bucket in buckets.values():
            directions = {str(c.get("direction", "")).strip().lower() for c in bucket}
            has_long = bool(directions & {"long", "buy", "bullish"})
            has_short = bool(directions & {"short", "sell", "bearish"})
            if has_long and has_short:
                any_conflict = True
                winner = max(bucket, key=score)
                winner_id = str(winner.get("thesis_id", ""))
                if winner_id:
                    accepted.append(winner_id)
                    scalars[winner_id] = 1.0
                for c in bucket:
                    cid = str(c.get("thesis_id", ""))
                    if cid and cid != winner_id:
                        rejected.append(cid)
                continue

            # Same-side concentration: allow but downscale if multiple theses compete in
            # the same bucket.
            n = max(1, len(bucket))
            scalar = 1.0 if n == 1 else 1.0 / n
            if n > 1:
                any_downscale = True
            for c in bucket:
                cid = str(c.get("thesis_id", ""))
                if cid:
                    accepted.append(cid)
                    scalars[cid] = scalar

        if any_conflict:
            return ConflictResolution("net_exposure", "opposing_direction_conflict_resolved", accepted, rejected, scalars)
        if any_downscale:
            return ConflictResolution("reduce_size", "same_bucket_concentration_downscaled", accepted, rejected, scalars)
        return ConflictResolution("accept", "no_conflict", accepted, rejected, scalars)



def build_admission_trace(
    candidates: list[dict[str, Any]],
    resolution: ConflictResolution,
) -> list[dict[str, Any]]:
    """Build deterministic portfolio-admission trace rows.

    The runtime/paper engine can persist these rows without re-deriving conflict
    reasons.  The function is deliberately side-effect free so tests and audit
    tools can call it directly.
    """
    accepted = set(resolution.accepted_ids)
    rejected = set(resolution.rejected_ids)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        thesis_id = str(candidate.get("thesis_id", candidate.get("signal_id", "")) or "")
        decision = "accept" if thesis_id in accepted else "reject" if thesis_id in rejected else resolution.decision
        rows.append({
            "signal_id": str(candidate.get("signal_id", thesis_id)),
            "thesis_id": thesis_id,
            "symbol": str(candidate.get("symbol", "")),
            "timeframe": str(candidate.get("timeframe", "")),
            "horizon_bars": int(candidate.get("horizon_bars", 0) or 0),
            "event_id": str(candidate.get("event_id", candidate.get("primary_event_id", ""))),
            "event_family": str(candidate.get("event_family", candidate.get("family", ""))),
            "anchor_role": str(candidate.get("anchor_role", "")),
            "polarity_semantics": str(candidate.get("polarity_semantics", "")),
            "template_id": str(candidate.get("template_id", candidate.get("template", ""))),
            "direction": str(candidate.get("direction", "")),
            "overlap_group": str(candidate.get("overlap_group_id", candidate.get("overlap_group", ""))),
            "candidate_score": float(candidate.get("support_score", candidate.get("candidate_score", 0.0)) or 0.0),
            "risk_adjusted_support": float(candidate.get("support_score", 0.0) or 0.0) - float(candidate.get("contradiction_penalty", 0.0) or 0.0),
            "decision": decision,
            "decision_reason": resolution.reason,
            "conflicting_signal_ids": "|".join(x for x in resolution.rejected_ids if x != thesis_id),
            "size_multiplier": float(resolution.size_scalars.get(thesis_id, 0.0 if decision == "reject" else 1.0)),
            "veto_source": str(candidate.get("veto_source", "guard_veto" if bool(candidate.get("guard_veto", False)) else "")),
        })
    return rows
