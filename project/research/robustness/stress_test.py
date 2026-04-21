# project/research/robustness/stress_test.py
"""
Stress test evaluator.

Evaluates a hypothesis during predefined market stress scenarios.
Each scenario is a feature condition (feature_predicate) that identifies
"stressed" bars. Hypothesis is evaluated only on stressed bars.

A hypothesis that survives (positive t-stat) across multiple stress scenarios
is more robust than one that relies entirely on benign market conditions.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.evaluator_utils import (
    trigger_mask,
    forward_log_returns,
    signed_returns_for_spec,
)
from project.core.column_registry import ColumnRegistry

log = logging.getLogger(__name__)
_BPS = 10_000.0


# Predefined stress scenarios as feature_predicate conditions.
# Features listed here must be present in the wide features table.
# If a feature column is missing, the scenario is skipped (valid=False).
def _load_stress_scenarios() -> list[dict]:
    try:
        return get_domain_registry().stress_scenario_rows()
    except Exception as e:
        log.warning("Failed to load stress scenarios from compiled registry: %s", e)
        return []


STRESS_SCENARIOS: list[dict] = _load_stress_scenarios()


def _apply_stress_mask(scenario: dict, features: pd.DataFrame) -> pd.Series | None:
    """Return boolean mask for scenario, or None if feature column not found."""
    feat_name = scenario["feature"]
    # Try ColumnRegistry first, then direct name
    cols = ColumnRegistry.feature_cols(feat_name)
    col = next((c for c in cols if c in features.columns), None)
    if col is None and feat_name in features.columns:
        col = feat_name
    if col is None:
        return None

    vals = pd.to_numeric(features[col], errors="coerce")
    op, thr = scenario["operator"], scenario["threshold"]
    if op == ">":
        return vals > thr
    if op == ">=":
        return vals >= thr
    if op == "<":
        return vals < thr
    if op == "<=":
        return vals <= thr
    if op == "==":
        return vals == thr
    return None


def evaluate_stress_scenarios(
    spec: HypothesisSpec,
    features: pd.DataFrame,
    *,
    horizon_bars: int = 12,
    min_n: int = 10,
    scenarios: list[dict] | None = None,
    stress_masks: dict[str, pd.Series | None] | None = None,
    event_mask: pd.Series | None = None,
    forward_returns: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Evaluate spec within each stress scenario.

    Parameters
    ----------
    spec : HypothesisSpec to evaluate
    features : wide feature DataFrame
    horizon_bars : forward return horizon in bars
    min_n : stress periods with fewer fires marked valid=False
    scenarios : override default STRESS_SCENARIOS list
    stress_masks : Pre-calculated scenario masks keyed by scenario name

    Returns
    -------
    DataFrame with one row per scenario:
        scenario, description, n, mean_return_bps, t_stat, hit_rate, valid,
        pct_of_base_n (share of total event fires in this stress period)
    """
    if scenarios is None:
        scenarios = STRESS_SCENARIOS

    if stress_masks is None:
        stress_masks = {s["name"]: _apply_stress_mask(s, features) for s in scenarios}

    if features.empty or "close" not in features.columns:
        return pd.DataFrame()

    if event_mask is None:
        mask_raw = trigger_mask(spec, features)
        if spec.feature_condition is not None:
            fc_spec = HypothesisSpec(
                trigger=spec.feature_condition,
                direction=spec.direction,
                horizon=spec.horizon,
                template_id=spec.template_id,
            )
            mask_raw = mask_raw & trigger_mask(fc_spec, features)

        # Trigger mask with entry lag
        if spec.entry_lag > 0:
            mask = mask_raw.astype("boolean").shift(spec.entry_lag, fill_value=False).astype(bool)
        else:
            mask = mask_raw
    else:
        mask = event_mask.astype(bool)

    base_n = int(mask.sum())
    if base_n < min_n:
        return pd.DataFrame()

    fwd = (
        forward_returns
        if forward_returns is not None
        else forward_log_returns(features["close"], horizon_bars)
    )

    rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        stress_mask = stress_masks.get(scenario["name"])

        if stress_mask is None:
            rows.append(
                {
                    "scenario": scenario["name"],
                    "description": scenario.get("description", ""),
                    "n": 0,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "pct_of_base_n": float("nan"),
                    "valid": False,
                    "skip_reason": "feature_column_missing",
                }
            )
            continue

        combined = mask & stress_mask

        event_returns = fwd[combined].dropna()
        n = len(event_returns)

        if n < min_n:
            rows.append(
                {
                    "scenario": scenario["name"],
                    "description": scenario.get("description", ""),
                    "n": n,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "pct_of_base_n": round(n / base_n, 4) if base_n > 0 else float("nan"),
                    "valid": False,
                    "skip_reason": f"n={n} < min_n={min_n}",
                }
            )
            continue

        signed, reason = signed_returns_for_spec(spec, features, event_returns)
        if signed is None:
            rows.append(
                {
                    "scenario": scenario["name"],
                    "description": scenario.get("description", ""),
                    "n": n,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "pct_of_base_n": round(n / base_n, 4) if base_n > 0 else float("nan"),
                    "valid": False,
                    "skip_reason": reason or "direction_resolution_failed",
                }
            )
            continue
        mean_r = float(signed.mean())
        std_r = float(signed.std(ddof=1))
        t = mean_r / (std_r / np.sqrt(n)) if std_r > 1e-10 else 0.0

        rows.append(
            {
                "scenario": scenario["name"],
                "description": scenario.get("description", ""),
                "n": n,
                "mean_return_bps": round(mean_r * _BPS, 4),
                "t_stat": round(t, 4),
                "hit_rate": round(float((signed > 0).mean()), 4),
                "pct_of_base_n": round(n / base_n, 4) if base_n > 0 else float("nan"),
                "valid": True,
                "skip_reason": None,
            }
        )

    return pd.DataFrame(rows) if rows else pd.DataFrame()
