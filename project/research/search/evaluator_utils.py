# project/research/search/evaluator_utils.py
"""
Shared utilities for hypothesis evaluation.
Broken out from evaluator.py to avoid circular imports.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from project.core.constants import parse_horizon_bars
from project.research.direction_semantics import normalize_side_policy, resolve_effect_sign
from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.core.column_registry import ColumnRegistry
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.context_labels import canonicalize_context_label, expand_dimension_values

log = logging.getLogger(__name__)

_CONTEXT_CONFIDENCE_COLUMN_BY_FAMILY = {
    "vol": "ms_vol_confidence",
    "vol_regime": "ms_vol_confidence",
    "liq": "ms_liq_confidence",
    "liquidity": "ms_liq_confidence",
    "oi": "ms_oi_confidence",
    "funding": "ms_funding_confidence",
    "trend": "ms_trend_confidence",
    "spread": "ms_spread_confidence",
}

_CONTEXT_ENTROPY_COLUMN_BY_FAMILY = {
    "vol": "ms_vol_entropy",
    "vol_regime": "ms_vol_entropy",
    "liq": "ms_liq_entropy",
    "liquidity": "ms_liq_entropy",
    "oi": "ms_oi_entropy",
    "funding": "ms_funding_entropy",
    "trend": "ms_trend_entropy",
    "spread": "ms_spread_entropy",
}

_DEFAULT_CONTEXT_MIN_CONFIDENCE = 0.55
_DEFAULT_CONTEXT_MAX_ENTROPY = 0.90


def horizon_bars(horizon: str) -> int:
    return parse_horizon_bars(horizon)


def forward_log_returns(close: pd.Series, horizon_bars: int) -> pd.Series:
    """
    Calculate forward log returns in Basis Points (BPS).
    """
    log_close = np.log(close.clip(lower=1e-12))
    return (log_close.shift(-horizon_bars) - log_close) * 10_000.0


def excursion_stats(
    close: pd.Series, mask: pd.Series, horizon_bars: int, direction_sign: float
) -> Tuple[pd.Series, pd.Series]:
    """
    Calculate Max Adverse Excursion (MAE) and Max Favorable Excursion (MFE)
    for each trigger event in the mask.
    """
    if not mask.any():
        return pd.Series(dtype=float), pd.Series(dtype=float)

    indices = np.where(mask)[0]
    maes = []
    mfes = []

    close_vals = close.values
    for idx in indices:
        if idx + horizon_bars >= len(close_vals):
            maes.append(np.nan)
            mfes.append(np.nan)
            continue

        window = close_vals[idx : idx + horizon_bars + 1]
        returns = np.log(window / close_vals[idx])
        signed_returns = returns * direction_sign

        maes.append(np.nanmin(signed_returns))
        mfes.append(np.nanmax(signed_returns))

    return pd.Series(maes, index=mask[mask].index), pd.Series(mfes, index=mask[mask].index)


def trigger_mask(spec: HypothesisSpec, features: pd.DataFrame) -> pd.Series:
    """
    Resolve a trigger to a boolean mask over the feature table rows.
    Returns a Series[bool] aligned to features.index.
    """
    t = spec.trigger
    ttype = t.trigger_type
    false_mask = pd.Series(False, index=features.index)

    if ttype == TriggerType.EVENT:
        eid = t.event_id or ""
        spec_event = EVENT_REGISTRY_SPECS.get(eid.upper())
        signal_col = spec_event.signal_column if spec_event else None
        cols = ColumnRegistry.event_cols(eid, signal_col=signal_col)
        for col in cols:
            if col in features.columns:
                vals = features[col]
                mask = vals.where(vals.notna(), False).astype(bool)
                if not t.event_direction:
                    return mask
                direction_col = next(
                    (candidate for candidate in ColumnRegistry.event_direction_cols(eid) if candidate in features.columns),
                    None,
                )
                if direction_col is None:
                    log.debug("Event direction column for %r not found in features", eid)
                    return false_mask
                direction_vals = pd.to_numeric(features[direction_col], errors="coerce")
                if t.event_direction == "up":
                    direction_mask = direction_vals > 0
                elif t.event_direction == "down":
                    direction_mask = direction_vals < 0
                else:
                    direction_mask = direction_vals == 0
                return mask & direction_mask.fillna(False)
        log.debug("Event column for %r (signal_col=%r) not found in features", eid, signal_col)
        return false_mask

    if ttype == TriggerType.STATE:
        cols = ColumnRegistry.state_cols(t.state_id or "")
        for col in cols:
            if col in features.columns:
                vals = pd.to_numeric(features[col], errors="coerce")
                vals = vals.where(vals.notna(), 0)
                return (vals == 1) if t.state_active else (vals == 0)
        log.debug("State column for %r not found in features", t.state_id)
        return false_mask

    if ttype == TriggerType.TRANSITION:
        from_cols = ColumnRegistry.state_cols(t.from_state or "")
        to_cols = ColumnRegistry.state_cols(t.to_state or "")
        from_col = next((c for c in from_cols if c in features.columns), None)
        to_col = next((c for c in to_cols if c in features.columns), None)
        if from_col and to_col:
            was_from_vals = pd.to_numeric(features[from_col], errors="coerce")
            is_to_vals = pd.to_numeric(features[to_col], errors="coerce")
            
            # Sprint 2: Strict transition semantics.
            # Require both previous and current state data to exist (no implicit fallback).
            # was_from.shift(1) will have NaN at index 0. 
            # We must ensure we don't treat NaN as "not from_state" or anything else.
            
            was_from_raw = (was_from_vals == 1)
            is_to_raw = (is_to_vals == 1)
            
            # shift(1) makes the first element NaN. 
            # In Sprint 2, we must NOT fire at index 0 because history is missing.
            # .fillna(False) on the result of & with shifted values is okay IF 
            # we are sure shift(1) correctly represents the lack of history.
            
            was_from_shifted = was_from_raw.shift(1)
            
            # Explicitly require history: first bar can never be a transition onset.
            return (was_from_shifted == True) & (is_to_raw == True)
        
        log.debug(
            "Transition columns for %r→%r not found in features",
            t.from_state,
            t.to_state,
        )
        return false_mask

    if ttype == TriggerType.FEATURE_PREDICATE:
        feat_name = t.feature or ""
        cols = ColumnRegistry.feature_cols(feat_name)
        feat = next((c for c in cols if c in features.columns), None)
        if not feat:
            log.debug("Feature %r not found in features", feat_name)
            return false_mask
        vals = pd.to_numeric(features[feat], errors="coerce")
        op, thr = t.operator, t.threshold
        if op == ">=":
            return vals >= thr
        if op == "<=":
            return vals <= thr
        if op == ">":
            return vals > thr
        if op == "<":
            return vals < thr
        if op == "==":
            return vals == thr
        return false_mask

    if ttype == TriggerType.SEQUENCE:
        cols = ColumnRegistry.sequence_cols(t.sequence_id or "")
        for col in cols:
            if col in features.columns:
                vals = features[col]
                return vals.where(vals.notna(), False).astype(bool)
        log.debug("Sequence column for %r not found in features", t.sequence_id)
        return false_mask

    if ttype == TriggerType.INTERACTION:
        cols = ColumnRegistry.interaction_cols(t.interaction_id or "")
        for col in cols:
            if col in features.columns:
                vals = features[col]
                return vals.where(vals.notna(), False).astype(bool)
        log.debug("Interaction column for %r not found in features", t.interaction_id)
        return false_mask

    return false_mask


def load_context_state_map() -> Dict[Tuple[str, str], str]:
    """
    Load the compiled context state map and return a flat mapping of
    (family, label) -> state_id. Raises FileNotFoundError if the compiled map
    is missing.
    """
    registry = get_domain_registry()
    if not registry.context_state_map:
        raise FileNotFoundError("context_state_map is missing from compiled domain registry")
    return dict(registry.context_state_map)


def _normalize_context_scalar(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _series_matches_values(series: pd.Series, values: list[object]) -> pd.Series:
    if series.empty:
        return pd.Series(False, index=series.index, dtype=bool)
    numeric_targets: list[float] = []
    all_numeric = True
    for value in values:
        try:
            numeric_targets.append(float(str(value).strip()))
        except Exception:
            all_numeric = False
            break
    coerced = pd.to_numeric(series, errors="coerce")
    if all_numeric and coerced.notna().any():
        return coerced.isin(numeric_targets)
    return series.map(_normalize_context_scalar).isin(
        {_normalize_context_scalar(value) for value in values}
    )


# Cache for the context state map to avoid repeated file I/O
_CACHED_CONTEXT_MAP: Optional[Dict[Tuple[str, str], str]] = None


def context_mask(
    context: Dict[str, str],
    features: pd.DataFrame,
    *,
    use_context_quality: bool = True,
) -> Optional[pd.Series]:
    """
    Build a boolean mask from a context dict (e.g. {vol_regime: "high", carry_state: "funding_pos"}).
    Returns None if ANY context key cannot be resolved to a feature column (context is unresolvable).
    Returns a combined AND mask when all keys resolve.
    """
    global _CACHED_CONTEXT_MAP
    if _CACHED_CONTEXT_MAP is None:
        try:
            _CACHED_CONTEXT_MAP = load_context_state_map()
        except Exception as e:
            log.error("Failed to load context state map: %s", e)
            return None

    combined = pd.Series(True, index=features.index)
    for family, label in context.items():
        canonical_label = canonicalize_context_label(family, label)
        expanded_values = expand_dimension_values(family, [canonical_label])
        if family in features.columns:
            vals = _series_matches_values(features[family], expanded_values)
        else:
            vals = None
        state_id = _CACHED_CONTEXT_MAP.get((family, canonical_label))
        if vals is None and state_id is None:
            log.debug("No state mapping for context (%r, %r) — context unresolvable", family, label)
            return None
        if vals is None:
            cols = ColumnRegistry.state_cols(state_id)
            col = next((c for c in cols if c in features.columns), None)
            if col is None:
                log.debug(
                    "Context state column %r not found in features — context unresolvable", state_id
                )
                return None
            vals = pd.to_numeric(features[col], errors="coerce").fillna(0) == 1
        quality_mask = pd.Series(True, index=features.index)
        if use_context_quality:
            family_key = str(family).strip().lower()

            confidence_col = _CONTEXT_CONFIDENCE_COLUMN_BY_FAMILY.get(family_key)
            if confidence_col and confidence_col in features.columns:
                confidence = pd.to_numeric(features[confidence_col], errors="coerce")
                quality_mask = quality_mask & (
                    confidence >= _DEFAULT_CONTEXT_MIN_CONFIDENCE
                ).fillna(False)

            entropy_col = _CONTEXT_ENTROPY_COLUMN_BY_FAMILY.get(family_key)
            if entropy_col and entropy_col in features.columns:
                entropy = pd.to_numeric(features[entropy_col], errors="coerce")
                quality_mask = quality_mask & (entropy <= _DEFAULT_CONTEXT_MAX_ENTROPY).fillna(
                    False
                )

        combined = combined & vals.fillna(False) & quality_mask
    return combined


def trigger_key(spec: HypothesisSpec) -> str:
    return spec.trigger.label()


def event_direction_series(spec: HypothesisSpec, features: pd.DataFrame) -> Optional[pd.Series]:
    if spec.trigger.trigger_type != TriggerType.EVENT or features.empty:
        return None
    event_id = str(spec.trigger.event_id or "").upper()
    for col in ColumnRegistry.event_direction_cols(event_id):
        if col in features.columns:
            return pd.to_numeric(features[col], errors="coerce")
    return None


def operator_semantics(spec: HypothesisSpec) -> Optional[Dict[str, Any]]:
    operator = get_domain_registry().get_operator(spec.template_id)
    if operator is None:
        return None
    raw = dict(operator.raw)
    side_policy = normalize_side_policy(str(raw.get("side_policy", "both")))
    label_target = str(raw.get("label_target", "fwd_return_h")).strip().lower() or "fwd_return_h"
    requires_direction = bool(raw.get("requires_direction", True))
    return {
        "side_policy": side_policy,
        "label_target": label_target,
        "requires_direction": requires_direction,
    }


def signed_returns_for_spec(
    spec: HypothesisSpec,
    features: pd.DataFrame,
    returns: pd.Series,
) -> tuple[Optional[pd.Series], Optional[str]]:
    semantics = operator_semantics(spec)
    if semantics is None:
        return None, "unknown_template_operator"
    label_target = str(semantics["label_target"]).strip().lower()
    if label_target == "gate":
        return None, "gate_template_unsupported"
    if label_target != "fwd_return_h":
        return None, "unsupported_label_target"

    if spec.trigger.trigger_type == TriggerType.EVENT and not semantics["requires_direction"]:
        direction_series = event_direction_series(spec, features)
        if direction_series is None:
            return None, "missing_event_direction"
        if spec.entry_lag > 0:
            direction_series = direction_series.shift(int(spec.entry_lag))
        aligned = direction_series.loc[returns.index].dropna()
        if aligned.empty or len(aligned) != len(returns):
            return None, "missing_event_direction"
        _fallback = 1 if spec.direction == "long" else -1 if spec.direction == "short" else 1
        sign_values = aligned.apply(
            lambda value: resolve_effect_sign(
                template_verb=spec.template_id,
                side_policy=str(semantics["side_policy"]),
                event_direction=value,
                label_target=label_target,
                fallback_sign=_fallback,
            )
        ).astype(float)
        return returns * sign_values, None

    direction_sign = 1.0 if spec.direction == "long" else -1.0 if spec.direction == "short" else 1.0
    return returns * float(direction_sign), None
