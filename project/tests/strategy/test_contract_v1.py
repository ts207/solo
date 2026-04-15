from __future__ import annotations

import pytest

from project.strategy.dsl.contract_v1 import (
    NonExecutableActionError,
    NonExecutableConditionError,
    _is_allowed_feature_name,
    action_to_overlays,
    derive_action_delay,
    is_executable_action,
    is_executable_condition,
    normalize_entry_condition,
    resolve_trigger_column,
    validate_action,
    validate_feature_references,
)


def test_action_validation_and_delay_rules() -> None:
    assert is_executable_action("delay_8")
    assert validate_action("delay_8", event_type="E", candidate_id="c1") == "delay_8"
    assert derive_action_delay("reenable_at_half_life", robustness=0.9, time_stop_bars=9) == 4
    assert action_to_overlays("entry_gate_skip")[0].params["size_scale"] == 0.0
    with pytest.raises(NonExecutableActionError):
        validate_action("unsupported", event_type="E", candidate_id="c1")


def test_condition_normalization_and_blueprint_validation() -> None:
    canonical, nodes, symbol = normalize_entry_condition(
        "basis_bps>=12.5", event_type="E", candidate_id="c1"
    )
    assert canonical == "basis_bps>=12.5"
    assert nodes[0].feature == "basis_bps"
    assert nodes[0].value == 12.5
    assert symbol is None

    canonical, nodes, symbol = normalize_entry_condition(
        "symbol_btc", event_type="E", candidate_id="c1"
    )
    assert canonical == "symbol_BTC"
    assert symbol == "BTC"
    assert nodes == []
    assert is_executable_condition("vol_regime_high")
    assert not is_executable_condition("severity_bucket_high")

    with pytest.raises(NonExecutableConditionError):
        normalize_entry_condition("all__legacy", event_type="E", candidate_id="c1")

    validate_feature_references(
        {
            "entry": {
                "condition_nodes": [{"feature": "basis_bps"}],
                "conditions": ["vol_regime_high >= 1"],
            }
        }
    )
    with pytest.raises(ValueError):
        validate_feature_references(
            {"entry": {"condition_nodes": [{"feature": "forward_return"}]}}
        )



def test_trigger_resolution_and_feature_allowlist() -> None:
    assert resolve_trigger_column("event_signal", ["signal_flag", "event_signal"]) == "event_signal"
    assert resolve_trigger_column("signal", ["signal_flag", "event_signal"]) == "event_signal"
    assert _is_allowed_feature_name("basis_bps")
    assert not _is_allowed_feature_name("forward_return")
