"""
Configuration and parameter handling for shrinkage estimation.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_SHRINKAGE_LOCK = threading.Lock()

_TAU_BY_FAMILY_DAYS: Dict[str, float] = {}
_VOL_REGIME_MULTIPLIER: Dict[str, float] = {}
_LIQUIDITY_STATE_MULTIPLIER: Dict[str, float] = {}
_DIRECTIONAL_ASYMMETRY_BY_FAMILY: Dict[str, Tuple[float, float]] = {}

_EVENT_DIRECTION_NUMERIC_COLS: Tuple[str, ...] = (
    "evt_event_direction",
    "evt_direction",
    "evt_signal_direction",
    "evt_flow_direction",
    "evt_breakout_direction",
    "evt_shock_direction",
    "evt_move_direction",
    "evt_leader_direction",
    "evt_return_1",
    "evt_return_sign",
    "evt_sign",
    "evt_polarity",
    "evt_funding_z",
    "evt_basis_z",
)

_EVENT_DIRECTION_TEXT_COLS: Tuple[str, ...] = (
    "evt_side",
    "evt_trade_side",
    "evt_signal_side",
    "evt_direction_label",
)


def update_shrinkage_parameters_from_spec(repo_root: Optional[Path] = None) -> None:
    """
    Load statistical parameters from spec/gates.yaml and update local constants.
    Ensures 'Spec is Truth' for statistical shrinkage logic.
    Raises RuntimeError if the spec or required parameters are missing.
    """
    from project.spec_registry import load_gates_spec

    try:
        spec = load_gates_spec()
        if not spec:
            raise FileNotFoundError("spec/gates.yaml not found or empty.")

        params = spec.get("shrinkage_parameters", {})
        if not params:
            raise ValueError("Required 'shrinkage_parameters' section missing in spec/gates.yaml")

        with _SHRINKAGE_LOCK:
            if "tau_by_family_days" in params:
                _TAU_BY_FAMILY_DAYS.clear()
                _TAU_BY_FAMILY_DAYS.update(
                    {
                        str(k).strip().upper(): float(v)
                        for k, v in params["tau_by_family_days"].items()
                    }
                )
            else:
                raise ValueError("Missing 'tau_by_family_days' in shrinkage_parameters")

            if "vol_regime_multiplier" in params:
                _VOL_REGIME_MULTIPLIER.clear()
                _VOL_REGIME_MULTIPLIER.update(
                    {
                        str(k).strip().upper(): float(v)
                        for k, v in params["vol_regime_multiplier"].items()
                    }
                )

            if "liquidity_state_multiplier" in params:
                _LIQUIDITY_STATE_MULTIPLIER.clear()
                _LIQUIDITY_STATE_MULTIPLIER.update(
                    {
                        str(k).strip().upper(): float(v)
                        for k, v in params["liquidity_state_multiplier"].items()
                    }
                )

            if "directional_asymmetry_by_family" in params:
                _DIRECTIONAL_ASYMMETRY_BY_FAMILY.clear()
                for family, values in params["directional_asymmetry_by_family"].items():
                    if isinstance(values, list) and len(values) == 2:
                        _DIRECTIONAL_ASYMMETRY_BY_FAMILY[str(family).strip().upper()] = (
                            float(values[0]),
                            float(values[1]),
                        )

        log.info("Successfully initialized shrinkage parameters from spec/gates.yaml")
    except Exception as e:
        log.error("CRITICAL: Failed to load mandatory shrinkage parameters: %s", e)
        raise RuntimeError(f"Spec-driven shrinkage initialization failed: {e}") from e


def _ensure_shrinkage_parameters_loaded() -> None:
    if _TAU_BY_FAMILY_DAYS and _VOL_REGIME_MULTIPLIER and _LIQUIDITY_STATE_MULTIPLIER:
        return
    update_shrinkage_parameters_from_spec()


def _resolve_tau_days(canonical_family: str, override_days: Optional[float]) -> float:
    _ensure_shrinkage_parameters_loaded()
    if override_days is not None and float(override_days) > 0.0:
        return float(override_days)
    key = str(canonical_family or "").strip().upper()
    return float(_TAU_BY_FAMILY_DAYS.get(key, 60.0))


def _normalize_vol_regime(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "MID"
    if "shock" in text:
        return "SHOCK"
    if "high" in text:
        return "HIGH"
    if "low" in text:
        return "LOW"
    if "mid" in text or "normal" in text:
        return "MID"
    return "MID"


def _normalize_liquidity_state(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "NORMAL"
    if "recovery" in text:
        return "RECOVERY"
    if any(token in text for token in ("low", "absence", "illiquid", "collapse", "vacuum")):
        return "LOW"
    return "NORMAL"


def _regime_conditioned_tau_days(
    *,
    canonical_family: str,
    vol_regime: Any,
    liquidity_state: Any,
    base_tau_days_override: Optional[float],
) -> float:
    _ensure_shrinkage_parameters_loaded()
    tau = _resolve_tau_days(canonical_family, base_tau_days_override)
    vol_key = _normalize_vol_regime(vol_regime)
    liq_key = _normalize_liquidity_state(liquidity_state)
    tau *= float(_VOL_REGIME_MULTIPLIER.get(vol_key, 1.0))
    tau *= float(_LIQUIDITY_STATE_MULTIPLIER.get(liq_key, 1.0))
    return float(tau)


def _direction_sign(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        num = float(value)
        if np.isfinite(num) and num != 0.0:
            return 1 if num > 0.0 else -1
    except (TypeError, ValueError):
        pass

    token = str(value).strip().lower()
    if not token:
        return None
    if token in {"1", "+1", "up", "long", "buy", "bull", "positive", "pos"}:
        return 1
    if token in {"-1", "down", "short", "sell", "bear", "negative", "neg"}:
        return -1
    return None


def _optional_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    token = str(value).strip()
    if not token:
        return None
    if token.lower() in {"none", "null", "nan", "na"}:
        return None
    return token


def _event_direction_from_joined_row(
    row: pd.Series,
    *,
    canonical_family: str,
    fallback_direction: int,
) -> int:
    family_key = str(canonical_family or "").strip().upper()
    for col in _EVENT_DIRECTION_NUMERIC_COLS:
        if col not in row:
            continue
        val = row.get(col)
        sign = _direction_sign(val)
        if sign is None:
            continue
        if family_key == "POSITIONING_EXTREMES" and col == "evt_funding_z":
            return -sign
        return sign
    for col in _EVENT_DIRECTION_TEXT_COLS:
        if col not in row:
            continue
        val = row.get(col)
        sign = _direction_sign(val)
        if sign is not None:
            return sign
    return 1 if int(fallback_direction) >= 0 else -1


def _asymmetric_tau_days(
    *,
    base_tau_days: float,
    canonical_family: str,
    direction: int,
    default_up_mult: float,
    default_down_mult: float,
    min_ratio: float,
    max_ratio: float,
) -> Tuple[float, float, float, float]:
    _ensure_shrinkage_parameters_loaded()
    family_key = str(canonical_family or "").strip().upper()
    up_mult, down_mult = _DIRECTIONAL_ASYMMETRY_BY_FAMILY.get(
        family_key,
        (float(default_up_mult), float(default_down_mult)),
    )
    up_mult = float(up_mult if up_mult > 0.0 else max(default_up_mult, 1e-6))
    down_mult = float(down_mult if down_mult > 0.0 else max(default_down_mult, 1e-6))
    ratio = up_mult / max(down_mult, 1e-9)
    min_r = max(1.0, float(min_ratio))
    max_r = max(min_r, float(max_ratio))
    if ratio < min_r:
        down_mult = up_mult / min_r
    elif ratio > max_r:
        down_mult = up_mult / max_r
    ratio = up_mult / max(down_mult, 1e-9)

    tau_up = float(base_tau_days) * up_mult
    tau_down = float(base_tau_days) * down_mult
    tau_eff = tau_up if int(direction) >= 0 else tau_down
    return float(tau_eff), float(tau_up), float(tau_down), float(ratio)


# Initialize constants at module load time
try:
    update_shrinkage_parameters_from_spec()
except Exception:
    pass
