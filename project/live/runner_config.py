"""Configuration helpers for :mod:`project.live.runner`.

The live runner is intentionally an orchestrator.  Keep parsing/defaulting logic
for runtime configuration in this module so admission and runtime tests can cover
it without constructing exchange clients or websocket managers.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from project.live.live_quality_gate import LiveQualityThresholds


def resolve_memory_root(strategy_runtime: Mapping[str, Any]) -> Path | None:
    memory_root = str(strategy_runtime.get("memory_root", "")).strip()
    if not memory_root:
        return None
    return Path(memory_root)


def resolve_execution_model_config(strategy_runtime: Mapping[str, Any]) -> dict[str, Any]:
    configured = strategy_runtime.get("execution_model", {})
    config = dict(configured) if isinstance(configured, Mapping) else {}
    if bool(strategy_runtime.get("implemented", False)):
        config.setdefault("cost_model", "execution_simulator_v2")
    return config


def resolve_live_quality_thresholds(strategy_runtime: Mapping[str, Any]) -> LiveQualityThresholds:
    configured = strategy_runtime.get("live_quality_gate", {})
    values = dict(configured) if isinstance(configured, Mapping) else {}
    return LiveQualityThresholds(
        min_samples=int(values.get("min_samples", 5) or 5),
        max_slippage_drift_bps=float(values.get("max_slippage_drift_bps", 5.0) or 5.0),
        disable_slippage_drift_bps=float(
            values.get("disable_slippage_drift_bps", 15.0) or 15.0
        ),
        min_fill_rate=float(values.get("min_fill_rate", 0.70) or 0.70),
        disable_fill_rate=float(values.get("disable_fill_rate", 0.40) or 0.40),
        max_edge_divergence_bps=float(values.get("max_edge_divergence_bps", 10.0) or 10.0),
        disable_edge_divergence_bps=float(
            values.get("disable_edge_divergence_bps", 25.0) or 25.0
        ),
        max_stale_data_frequency=float(values.get("max_stale_data_frequency", 0.05) or 0.05),
        disable_stale_data_frequency=float(
            values.get("disable_stale_data_frequency", 0.20) or 0.20
        ),
        max_thesis_decay_rate=float(values.get("max_thesis_decay_rate", 0.25) or 0.25),
        disable_thesis_decay_rate=float(
            values.get("disable_thesis_decay_rate", 0.60) or 0.60
        ),
        min_risk_scale=float(values.get("min_risk_scale", 0.10) or 0.10),
    )


def expected_slippage_bps(
    execution_model_config: Mapping[str, Any],
    strategy_runtime: Mapping[str, Any],
) -> float:
    return float(
        execution_model_config.get(
            "base_slippage_bps",
            strategy_runtime.get("expected_slippage_bps", 0.0),
        )
        or 0.0
    )


def serialize_live_quality_thresholds(
    thresholds: LiveQualityThresholds,
    *,
    kill_on_disable: bool,
) -> dict[str, Any]:
    return {
        "min_samples": int(thresholds.min_samples),
        "max_slippage_drift_bps": float(thresholds.max_slippage_drift_bps),
        "disable_slippage_drift_bps": float(thresholds.disable_slippage_drift_bps),
        "min_fill_rate": float(thresholds.min_fill_rate),
        "disable_fill_rate": float(thresholds.disable_fill_rate),
        "max_edge_divergence_bps": float(thresholds.max_edge_divergence_bps),
        "disable_edge_divergence_bps": float(thresholds.disable_edge_divergence_bps),
        "max_stale_data_frequency": float(thresholds.max_stale_data_frequency),
        "disable_stale_data_frequency": float(thresholds.disable_stale_data_frequency),
        "max_thesis_decay_rate": float(thresholds.max_thesis_decay_rate),
        "disable_thesis_decay_rate": float(thresholds.disable_thesis_decay_rate),
        "min_risk_scale": float(thresholds.min_risk_scale),
        "kill_on_disable": bool(kill_on_disable),
    }
