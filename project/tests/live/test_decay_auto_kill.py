from __future__ import annotations

from project.live.decay import DecayMonitor, DecayRule, default_decay_rules
from project.live.thesis_state import ThesisStateManager


def test_default_edge_decay_auto_disables_after_persistent_breach() -> None:
    monitor = DecayMonitor(default_decay_rules())
    expected = {"net_expectancy_bps": 10.0, "hit_rate": 0.55}
    realized = {
        "sample_count": 10,
        "avg_realized_net_edge_bps": 2.0,
        "hit_rate": 0.50,
        "avg_realized_slippage_bps": 0.0,
    }

    first = monitor.assess_thesis_health("thesis_a", realized, expected)
    second = monitor.assess_thesis_health("thesis_a", realized, expected)

    assert first.health_state == "degraded"
    assert second.health_state == "disabled"
    assert "decay_edge_persistent" in second.reason_codes
    assert "disable" in second.actions_taken


def test_brief_breach_resets_before_auto_disable() -> None:
    monitor = DecayMonitor(default_decay_rules())
    expected = {"net_expectancy_bps": 10.0, "hit_rate": 0.55}
    breached = {
        "sample_count": 10,
        "avg_realized_net_edge_bps": 2.0,
        "hit_rate": 0.50,
        "avg_realized_slippage_bps": 0.0,
    }
    recovered = {
        "sample_count": 10,
        "avg_realized_net_edge_bps": 8.0,
        "hit_rate": 0.55,
        "avg_realized_slippage_bps": 0.0,
    }

    assert monitor.assess_thesis_health("thesis_b", breached, expected).health_state == "degraded"
    assert monitor.assess_thesis_health("thesis_b", recovered, expected).health_state == "healthy"
    assert monitor.assess_thesis_health("thesis_b", breached, expected).health_state == "degraded"


def test_explicit_breach_samples_can_auto_disable_immediately() -> None:
    monitor = DecayMonitor(
        [
            DecayRule(
                rule_id="edge_decay_custom",
                metric="edge",
                threshold=0.50,
                window_samples=10,
                action="downsize",
                disable_threshold_samples=20,
            )
        ]
    )
    snapshot = monitor.assess_thesis_health(
        "thesis_c",
        {
            "sample_count": 20,
            "edge_breach_samples": 20,
            "avg_realized_net_edge_bps": 1.0,
        },
        {"net_expectancy_bps": 10.0},
    )
    assert snapshot.health_state == "disabled"
    assert "decay_edge_persistent" in snapshot.reason_codes


def test_thesis_state_manager_sets_size_zero_on_decay_disable() -> None:
    manager = ThesisStateManager()
    manager.register_thesis("thesis_d", "research", "live_enabled")
    manager.update_health("thesis_d", "disabled", ["disable"])

    state = manager.get_state("thesis_d")
    assert state is not None
    assert state.state == "disabled"
    assert state.size_scalar == 0.0
