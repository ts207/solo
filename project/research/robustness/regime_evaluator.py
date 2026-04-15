# project/research/robustness/regime_evaluator.py
"""
Per-regime hypothesis evaluator.

Evaluates a single HypothesisSpec within each discrete regime segment,
returning per-regime performance metrics. Used as the foundation for
the regime-aware robustness score and stress tests.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from project.domain.hypotheses import HypothesisSpec
from project.research.search.evaluator_utils import (
    trigger_mask,
    forward_log_returns,
    signed_returns_for_spec,
)
from project.research.robustness.regime_labeler import label_regimes

log = logging.getLogger(__name__)

_BPS = 10_000.0


def evaluate_by_regime(
    spec: HypothesisSpec,
    features: pd.DataFrame,
    *,
    horizon_bars: int = 12,
    min_n_per_regime: int = 10,
    regime_labels: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Evaluate spec within each distinct regime in features.

    Parameters
    ----------
    spec : HypothesisSpec to evaluate
    features : wide feature DataFrame with 'close' and state_* columns
    horizon_bars : forward return horizon in bars
    min_n_per_regime : regimes with fewer event fires are marked valid=False
    regime_labels : Pre-calculated regime labels to save computation

    Returns
    -------
    DataFrame with one row per observed regime:
        regime, n, mean_return_bps, t_stat, hit_rate, valid
    """
    if features.empty or "close" not in features.columns:
        return pd.DataFrame()

    mask_raw = trigger_mask(spec, features)
    if spec.feature_condition is not None:
        fc_spec = HypothesisSpec(
            trigger=spec.feature_condition,
            direction=spec.direction,
            horizon=spec.horizon,
            template_id=spec.template_id,
        )
        fc_mask = trigger_mask(fc_spec, features)
        mask_raw = mask_raw & fc_mask

    # Trigger mask with explicit entry-lag guardrail
    if spec.entry_lag < 1:
        raise ValueError("entry_lag must be >= 1 to prevent same-bar entry leakage")
    mask = mask_raw.astype("boolean").shift(spec.entry_lag, fill_value=False).astype(bool)

    # Forward returns
    fwd = forward_log_returns(features["close"], horizon_bars)

    # Regime labels
    if regime_labels is None:
        regime_labels = label_regimes(features)

    rows: list[dict[str, Any]] = []
    for regime in sorted(regime_labels.unique()):
        regime_mask = regime_labels == regime
        combined = mask & regime_mask

        fire_indices = np.where(combined)[0]
        n = len(fire_indices)

        if n < min_n_per_regime:
            rows.append(
                {
                    "regime": regime,
                    "n": n,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "valid": False,
                }
            )
            continue

        # Extract forward returns at fire bars
        event_returns = fwd[combined].dropna()
        n_valid = len(event_returns)
        if n_valid < min_n_per_regime:
            rows.append(
                {
                    "regime": regime,
                    "n": n_valid,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "valid": False,
                }
            )
            continue

        signed, reason = signed_returns_for_spec(spec, features, event_returns)
        if signed is None:
            rows.append(
                {
                    "regime": regime,
                    "n": n_valid,
                    "mean_return_bps": float("nan"),
                    "t_stat": float("nan"),
                    "hit_rate": float("nan"),
                    "valid": False,
                    "skip_reason": reason or "direction_resolution_failed",
                }
            )
            continue
        mean_r = float(signed.mean())
        std_r = float(signed.std(ddof=1))
        t = mean_r / (std_r / np.sqrt(n_valid)) if std_r > 1e-10 else 0.0
        hit_rate = float((signed > 0).mean())

        rows.append(
            {
                "regime": regime,
                "n": n_valid,
                "mean_return_bps": round(mean_r, 4),
                "t_stat": round(t, 4),
                "hit_rate": round(hit_rate, 4),
                "valid": True,
            }
        )

    return pd.DataFrame(rows) if rows else pd.DataFrame()
