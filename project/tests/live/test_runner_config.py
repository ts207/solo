from pathlib import Path

from project.live.runner_config import (
    expected_slippage_bps,
    resolve_execution_model_config,
    resolve_live_quality_thresholds,
    resolve_memory_root,
    serialize_live_quality_thresholds,
)


def test_resolve_memory_root_returns_none_for_empty_config() -> None:
    assert resolve_memory_root({}) is None
    assert resolve_memory_root({"memory_root": "   "}) is None


def test_resolve_memory_root_returns_path_for_configured_value() -> None:
    assert resolve_memory_root({"memory_root": "data/live/memory"}) == Path("data/live/memory")


def test_execution_model_defaults_to_simulator_for_implemented_runtime() -> None:
    observed = resolve_execution_model_config({"implemented": True})
    assert observed["cost_model"] == "execution_simulator_v2"


def test_execution_model_preserves_explicit_cost_model() -> None:
    observed = resolve_execution_model_config(
        {"implemented": True, "execution_model": {"cost_model": "custom_model"}}
    )
    assert observed["cost_model"] == "custom_model"


def test_expected_slippage_prefers_execution_model_override() -> None:
    assert (
        expected_slippage_bps({"base_slippage_bps": 3.25}, {"expected_slippage_bps": 1.0})
        == 3.25
    )


def test_live_quality_threshold_serialization_includes_kill_policy() -> None:
    thresholds = resolve_live_quality_thresholds(
        {"live_quality_gate": {"min_samples": 9, "min_fill_rate": 0.8}}
    )
    payload = serialize_live_quality_thresholds(thresholds, kill_on_disable=True)
    assert payload["min_samples"] == 9
    assert payload["min_fill_rate"] == 0.8
    assert payload["kill_on_disable"] is True
