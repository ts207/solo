import logging
from typing import Any

import numpy as np
import pandas as pd

from project.domain.compiled_registry import get_domain_registry

log = logging.getLogger(__name__)

def compute_discovery_prechecks(
    spec: Any,
    features: pd.DataFrame,
    mask: pd.Series,
    fwd: pd.Series,
    event_weights: pd.Series,
    signed: pd.Series,
    regime_evals: pd.DataFrame,
    n: int,
    cost_bps: float,
    mean_bps: float,
    t_stat: float
) -> dict[str, Any]:
    """
    Computes cheap additive precheck metrics to support Discovery v2 candidate scoring.
    Fails safely returning defaults.
    """
    out = {
        "placebo_shift_effect": np.nan,
        "placebo_random_entry_effect": np.nan,
        "direction_reversal_effect": np.nan,
        "regime_support_ratio": np.nan,
        "null_strength_ratio": np.nan,
        "after_cost_expectancy_bps": np.nan,
        "cost_survival_ratio": np.nan,
        "coverage_ratio": np.nan,
        "turnover_proxy": np.nan,
        "microstructure_stress_proxy": np.nan,
        "event_family_key": "UNKNOWN",
        "template_family_key": "UNKNOWN",
        "direction_key": "UNKNOWN",
        "horizon_bucket": "UNKNOWN",
        "context_signature": "UNKNOWN",
        "symbol_timeframe_key": "UNKNOWN",
    }

    try:
        if features.empty or len(mask) == 0:
            return out

        direction_sign = 1.0 if str(spec.direction).lower() == "long" else -1.0

        # Falsification
        # 1. Placebo shift: shift mask forward 1 bar (entry lag + 1 conceptually)
        shifted_mask = mask.shift(1, fill_value=False)
        shifted_returns = fwd[shifted_mask].dropna()
        if not shifted_returns.empty:
            out["placebo_shift_effect"] = float(shifted_returns.mean() * direction_sign * 1e4)

        # 2. Placebo random: sample n random bars
        if n > 0 and len(fwd) > n:
            random_returns = fwd.sample(n=n, random_state=42).dropna()
            if not random_returns.empty:
                out["placebo_random_entry_effect"] = float(random_returns.mean() * direction_sign * 1e4)

        # 3. Direction reversal (assuming symmetrical pricing)
        out["direction_reversal_effect"] = -float(mean_bps) if not np.isnan(mean_bps) else np.nan

        # 4. Null strength ratio (compare absolute mean diff vs its standard error proxy)
        if not np.isnan(out["placebo_shift_effect"]) and abs(out["placebo_shift_effect"]) > 1e-8:
            out["null_strength_ratio"] = float(abs(mean_bps) / abs(out["placebo_shift_effect"]))

        # 5. Regime support
        if not regime_evals.empty and "t_stat" in regime_evals.columns:
            valid_regimes = regime_evals[regime_evals["valid"] == True]
            if not valid_regimes.empty:
                supported = (valid_regimes["t_stat"] > 0).sum() if t_stat >= 0 else (valid_regimes["t_stat"] < 0).sum()
                out["regime_support_ratio"] = float(supported / len(valid_regimes))

        # Tradability
        out["after_cost_expectancy_bps"] = mean_bps - cost_bps
        out["cost_survival_ratio"] = mean_bps / max(abs(cost_bps), 1e-4) if not np.isnan(mean_bps) else np.nan

        total_bars = len(features)
        out["coverage_ratio"] = float(n / total_bars) if total_bars > 0 else 0.0

        # Turnover proxy: sum of active mask fraction over time
        out["turnover_proxy"] = float(n / total_bars) if total_bars > 0 else 0.0

        # Microstructure stress proxy: difference between VWAP/open (if available) and close
        out["microstructure_stress_proxy"] = float(abs(mean_bps * 0.1)) # Cheap proxy offset for phase 1

        # Overlap / Novelty hints
        trigger = getattr(spec, "trigger", None)
        trigger_type = str(getattr(trigger, "trigger_type", "")).lower()

        event_id = "UNKNOWN"
        if trigger_type == "event":
            event_id = str(getattr(trigger, "event_id", "UNKNOWN")).upper()
        elif trigger_type == "state":
            event_id = f"STATE:{str(getattr(trigger, 'state_id', '')).upper()}"
        elif trigger_type == "transition":
            event_id = f"TRANS:{getattr(trigger, 'from_state', '')!s}_{getattr(trigger, 'to_state', '')!s}".upper()
        elif trigger_type in {"sequence", "interaction"}:
            event_id = str(getattr(trigger, "label", lambda: trigger_type)()).upper()

        # Resolve canonical family if possible
        canonical_family = event_id
        registry = get_domain_registry()
        try:
            event_spec = registry.get_event(event_id)
            if event_spec is not None:
                canonical_family = (
                    event_spec.research_family
                    or event_spec.canonical_family
                    or event_spec.canonical_regime
                    or event_spec.event_type
                )
        except Exception:
            pass

        out["event_family_key"] = canonical_family
        out["template_family_key"] = str(getattr(spec, "template_id", "UNKNOWN")).upper()
        out["direction_key"] = str(getattr(spec, "direction", "UNKNOWN")).upper()
        out["horizon_bucket"] = str(getattr(spec, "horizon", "UNKNOWN")).upper()
        out["context_signature"] = str(getattr(spec, "context", "NO_CTX")).upper()
        out["symbol_timeframe_key"] = "DYNAMIC_RESOLVED_BY_SERVICES"

    except Exception as e:
        log.warning(f"Error computing discovery prechecks: {e}")

    return out
