import math
from typing import Any
import pandas as pd

from project.research.regime_baselines import (
    _context_mask,
    _cost_series,
    _suppress_overlap,
    _year_stats,
    regime_id,
)

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def classify_failure(
    *,
    effective_n: int,
    mean_gross_bps: float,
    mean_net_bps: float,
    entry_lag_1_net: float,
    entry_lag_2_net: float,
    max_year_pnl_share: float | None,
    positive_year_count: int,
) -> str:
    if effective_n < 50:
        return "insufficient_data"
    
    if mean_net_bps > 0:
        if (max_year_pnl_share is not None and max_year_pnl_share > 0.50) or positive_year_count < 2:
            return "one_year_artifact"
        return "positive_edge_exists"

    if mean_gross_bps <= 0:
        return "no_gross_edge"
        
    if mean_gross_bps > 0 and mean_net_bps <= 0:
        if entry_lag_1_net > 0 or entry_lag_2_net > 0:
            return "adverse_timing"
        return "cost_killed"
        
    return "unknown"


def analyze_failure_regime(
    features: pd.DataFrame,
    filters: dict[str, str],
    symbol: str,
    direction: str,
    horizon_bars: int
) -> dict[str, Any] | None:
    mask, _ = _context_mask(features, filters)
    if mask is None:
        return None
        
    costs, _ = _cost_series(features)
    if costs is None:
        return None

    working = features.copy()
    close = pd.to_numeric(working["close"], errors="coerce")
    direction_sign = 1.0 if direction == "long" else -1.0
    
    future_close = close.shift(-int(horizon_bars))
    gross_bps = direction_sign * ((future_close / close) - 1.0) * 10_000.0
    
    close_lag1 = close.shift(-1)
    future_close_lag1 = close.shift(-(int(horizon_bars) + 1))
    gross_bps_lag1 = direction_sign * ((future_close_lag1 / close_lag1) - 1.0) * 10_000.0
    cost_bps_lag1 = costs.shift(-1)
    
    close_lag2 = close.shift(-2)
    future_close_lag2 = close.shift(-(int(horizon_bars) + 2))
    gross_bps_lag2 = direction_sign * ((future_close_lag2 / close_lag2) - 1.0) * 10_000.0
    cost_bps_lag2 = costs.shift(-2)

    working["_pos"] = range(len(working))
    working["gross_bps"] = gross_bps
    working["cost_bps"] = costs
    working["net_bps"] = gross_bps - costs
    working["net_bps_lag1"] = gross_bps_lag1 - cost_bps_lag1
    working["net_bps_lag2"] = gross_bps_lag2 - cost_bps_lag2
    
    eligible = working[mask & working["gross_bps"].notna() & working["cost_bps"].notna()].copy()
    sampled = _suppress_overlap(eligible, horizon_bars)
    
    effective_n = len(sampled)
    if effective_n == 0:
        return None
        
    mean_gross = _safe_float(sampled["gross_bps"].mean()) or 0.0
    mean_cost = _safe_float(sampled["cost_bps"].mean()) or 0.0
    mean_net = _safe_float(sampled["net_bps"].mean()) or 0.0
    
    net_lag1 = _safe_float(sampled["net_bps_lag1"].mean()) or 0.0
    net_lag2 = _safe_float(sampled["net_bps_lag2"].mean()) or 0.0

    cost_share = (mean_cost / abs(mean_gross)) if mean_gross != 0 else 0.0
    
    year_stats, max_share, positive_count = _year_stats(sampled)
    
    classification = classify_failure(
        effective_n=effective_n,
        mean_gross_bps=mean_gross,
        mean_net_bps=mean_net,
        entry_lag_1_net=net_lag1,
        entry_lag_2_net=net_lag2,
        max_year_pnl_share=max_share,
        positive_year_count=positive_count
    )

    year_pnl_str = ", ".join(f"{yr}:{s['mean_net_bps']:.1f}" for yr, s in year_stats.items() if s.get('mean_net_bps') is not None)

    return {
        "regime_id": regime_id(filters),
        "symbol": symbol,
        "direction": direction,
        "horizon": horizon_bars,
        "mean_gross_bps": mean_gross,
        "mean_cost_bps": mean_cost,
        "mean_net_bps": mean_net,
        "cost_share_of_gross": cost_share,
        "entry_lag_0_net": mean_net,
        "entry_lag_1_net": net_lag1,
        "entry_lag_2_net": net_lag2,
        "year_stats": year_pnl_str,
        "classification": classification
    }
