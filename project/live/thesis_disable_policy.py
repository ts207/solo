from __future__ import annotations

from dataclasses import dataclass

from project.live.live_quality_gate import LiveQualityGateResult
from project.live.thesis_state import ThesisStateManager


@dataclass(frozen=True)
class ThesisDisableDecision:
    thesis_id: str
    action: str
    risk_scale: float
    reason: str


def decide_thesis_disable_policy(gate: LiveQualityGateResult) -> ThesisDisableDecision:
    reason = ",".join(gate.reason_codes) if gate.reason_codes else "live_quality_ok"
    if gate.should_disable:
        return ThesisDisableDecision(gate.thesis_id, "disable", 0.0, reason)
    if gate.should_downscale:
        return ThesisDisableDecision(gate.thesis_id, "downscale", gate.risk_scale, reason)
    return ThesisDisableDecision(gate.thesis_id, "allow", 1.0, reason)


def apply_thesis_disable_decision(
    manager: ThesisStateManager,
    decision: ThesisDisableDecision,
) -> None:
    state = manager.get_state(decision.thesis_id)
    if state is None:
        manager.register_thesis(
            decision.thesis_id,
            promotion_class="unknown",
            deployment_mode="paper_only",
        )
    manager.apply_live_quality_decision(
        decision.thesis_id,
        action=decision.action,
        risk_scale=decision.risk_scale,
        reason=decision.reason,
    )
