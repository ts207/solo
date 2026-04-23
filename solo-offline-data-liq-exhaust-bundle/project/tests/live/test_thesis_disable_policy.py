from __future__ import annotations

from project.live.live_quality_gate import evaluate_live_quality_gate
from project.live.thesis_disable_policy import (
    apply_thesis_disable_decision,
    decide_thesis_disable_policy,
)
from project.live.thesis_state import ThesisStateManager


def test_thesis_disable_policy_downscales_degraded_thesis() -> None:
    manager = ThesisStateManager()
    manager.register_thesis("T1", promotion_class="paper_eligible", deployment_mode="paper_only")
    gate = evaluate_live_quality_gate(
        "T1",
        {
            "sample_count": 20,
            "slippage_drift_bps": 10.0,
            "fill_rate": 0.60,
        },
    )

    decision = decide_thesis_disable_policy(gate)
    apply_thesis_disable_decision(manager, decision)

    state = manager.get_state("T1")
    assert state is not None
    assert state.state == "degraded"
    assert 0.0 < state.size_scalar < 1.0


def test_thesis_disable_policy_disables_bad_live_behavior() -> None:
    manager = ThesisStateManager()
    manager.register_thesis("T1", promotion_class="live_eligible", deployment_mode="live_enabled")
    gate = evaluate_live_quality_gate(
        "T1",
        {
            "sample_count": 20,
            "edge_divergence_bps": 40.0,
            "thesis_decay_rate": 0.70,
        },
    )

    decision = decide_thesis_disable_policy(gate)
    apply_thesis_disable_decision(manager, decision)

    state = manager.get_state("T1")
    assert state is not None
    assert state.state == "disabled"
    assert state.size_scalar == 0.0
    assert "live_quality" in state.disable_reason
