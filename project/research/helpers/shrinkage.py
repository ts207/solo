"""
Hierarchical James-Stein shrinkage and time-decay weighting.
Public API facade with backward compatibility for legacy private-prefix imports.
"""

from __future__ import annotations

from project.research.helpers.diagnostics import (
    _refresh_phase2_metrics_after_shrinkage,
)
from project.research.helpers.estimation_kernels import (
    _aggregate_effect_units,
    _apply_hierarchical_shrinkage,
    _compute_loso_stability,
    _effective_sample_size,
    _estimate_adaptive_lambda,
    _time_decay_weights,
)
from project.research.helpers.parameter_normalization import (
    _asymmetric_tau_days,
    _direction_sign,
    _ensure_shrinkage_parameters_loaded,
    _event_direction_from_joined_row,
    _normalize_liquidity_state,
    _normalize_vol_regime,
    _optional_token,
    _regime_conditioned_tau_days,
    _resolve_tau_days,
    update_shrinkage_parameters_from_spec,
)

# New Public API Names
ensure_shrinkage_parameters_loaded = _ensure_shrinkage_parameters_loaded
resolve_tau_days = _resolve_tau_days
regime_conditioned_tau_days = _regime_conditioned_tau_days
direction_sign = _direction_sign
compute_time_decay_weights = _time_decay_weights
compute_effective_sample_size = _effective_sample_size
estimate_adaptive_lambda = _estimate_adaptive_lambda
compute_loso_stability = _compute_loso_stability
apply_hierarchical_shrinkage = _apply_hierarchical_shrinkage
refresh_phase2_metrics = _refresh_phase2_metrics_after_shrinkage

__all__ = [
    "update_shrinkage_parameters_from_spec",
    "ensure_shrinkage_parameters_loaded",
    "resolve_tau_days",
    "regime_conditioned_tau_days",
    "direction_sign",
    "compute_time_decay_weights",
    "compute_effective_sample_size",
    "estimate_adaptive_lambda",
    "compute_loso_stability",
    "apply_hierarchical_shrinkage",
    "refresh_phase2_metrics",
    # Legacy Compatibility
    "_ensure_shrinkage_parameters_loaded",
    "_resolve_tau_days",
    "_normalize_vol_regime",
    "_normalize_liquidity_state",
    "_regime_conditioned_tau_days",
    "_direction_sign",
    "_optional_token",
    "_event_direction_from_joined_row",
    "_asymmetric_tau_days",
    "_time_decay_weights",
    "_effective_sample_size",
    "_aggregate_effect_units",
    "_estimate_adaptive_lambda",
    "_compute_loso_stability",
    "_apply_hierarchical_shrinkage",
    "_refresh_phase2_metrics_after_shrinkage",
]
