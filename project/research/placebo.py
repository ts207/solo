"""Placebo evaluation functions (M4).

Tests specificity: that the apparent edge attaches to the *event*, not just
to the regime in which the event tends to occur.

Key function:
    build_placebo_series   — construct regime-matched random timestamps
    evaluate_specificity_lift — gate: U(h) - U_placebo_p95 >= min_lift
"""
from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)
_DEFAULT_RANDOM_SEED = 0


def _aligned_returns_positions(
    returns: pd.Series, positions: pd.Series
) -> tuple[pd.Series, pd.Series]:
    df = pd.concat(
        [
            pd.to_numeric(returns, errors="coerce").rename("returns"),
            pd.to_numeric(positions, errors="coerce").rename("positions"),
        ],
        axis=1,
    ).dropna()
    return df["returns"], df["positions"]


def _position_run_lengths(positions: pd.Series) -> list[int]:
    active = positions.fillna(0.0).ne(0.0)
    lengths: list[int] = []
    run = 0
    for flag in active:
        if flag:
            run += 1
        elif run:
            lengths.append(run)
            run = 0
    if run:
        lengths.append(run)
    return lengths


def evaluate_shift_placebo(
    returns: pd.Series,
    positions: pd.Series,
    shift_bars: int = 5,
) -> dict[str, Any]:
    """
    Shift signals by +/- shift_bars and compare PnL.
    If actual PnL is not significantly better than shifted PnL, it may be spurious.
    """
    returns_aligned, positions_aligned = _aligned_returns_positions(returns, positions)
    actual_pnl = (positions_aligned * returns_aligned).dropna()
    actual_mean = actual_pnl.mean()

    shifts = [shift_bars, -shift_bars]
    shift_means = []
    for s in shifts:
        shifted_pos = positions_aligned.shift(s).fillna(0.0)
        shifted_pnl = (shifted_pos * returns_aligned).dropna()
        shift_means.append(float(shifted_pnl.mean()))

    max_shift_mean = max(shift_means) if shift_means else 0.0
    lift = actual_mean / max(1e-9, max_shift_mean) if max_shift_mean > 0 else 10.0

    return {
        "actual_mean_ret": float(actual_mean),
        "max_shifted_mean_ret": float(max_shift_mean),
        "shift_lift_ratio": float(lift),
        "pass": bool(actual_mean > max_shift_mean * 1.5),
    }


def evaluate_random_entry_placebo(
    returns: pd.Series,
    states: pd.Series,
    actual_positions: pd.Series,
    n_iterations: int = 100,
    random_seed: int = _DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    """
    Compare actual performance against random entries within the same market state.
    Random placements preserve observed trade-count and approximate holding durations.
    """
    df = pd.concat(
        [
            pd.to_numeric(returns, errors="coerce").rename("returns"),
            pd.Series(states).rename("state"),
            pd.to_numeric(actual_positions, errors="coerce").rename("positions"),
        ],
        axis=1,
    ).dropna(subset=["returns", "positions"])
    if df.empty:
        return {"pass": True, "percentile": 1.0}

    actual_pnl = (df["positions"] * df["returns"]).dropna()
    actual_total = float(actual_pnl.sum())
    trade_lengths = _position_run_lengths(df["positions"])
    n_trades = len(trade_lengths)
    if n_trades == 0:
        return {"pass": True, "percentile": 1.0}

    state_values = (
        df["state"].astype("object")
        if "state" in df
        else pd.Series(["all"] * len(df), index=df.index)
    )
    active_mask = df["positions"].fillna(0.0).ne(0.0)
    state_counts = state_values.loc[active_mask].value_counts(dropna=False)
    eligible_by_state = {
        state: np.flatnonzero((state_values == state).to_numpy()) for state in state_counts.index
    }
    rng = np.random.default_rng(random_seed)
    random_totals: list[float] = []
    lengths = trade_lengths if trade_lengths else [10]

    for _ in range(n_iterations):
        random_pos = np.zeros(len(df), dtype=float)
        for trade_idx in range(n_trades):
            duration = int(lengths[min(trade_idx, len(lengths) - 1)])
            if duration <= 0:
                continue
            sampled_state = rng.choice(
                state_counts.index.to_list(),
                p=(state_counts / state_counts.sum()).to_numpy(dtype=float),
            )
            eligible = eligible_by_state.get(sampled_state, np.array([], dtype=int))
            if eligible.size == 0:
                continue
            start = int(rng.choice(eligible))
            stop = min(len(random_pos), start + duration)
            random_pos[start:stop] = 1.0
        rand_pnl = pd.Series(random_pos, index=df.index).mul(df["returns"]).sum()
        random_totals.append(float(rand_pnl))

    percentile = float(np.mean([actual_total > r for r in random_totals])) if random_totals else 1.0
    return {
        "actual_total_pnl": actual_total,
        "random_pnl_median": float(np.median(random_totals)) if random_totals else 0.0,
        "percentile": percentile,
        "pass": bool(percentile > 0.8),
        "random_seed": int(random_seed),
    }


def evaluate_direction_reversal_placebo(
    returns: pd.Series,
    positions: pd.Series,
) -> dict[str, Any]:
    """
    Check if reversing the trade direction results in negative expectancy.
    """
    returns_aligned, positions_aligned = _aligned_returns_positions(returns, positions)
    actual_pnl = (positions_aligned * returns_aligned).dropna()
    actual_mean = actual_pnl.mean()

    reversed_pos = -positions_aligned
    reversed_pnl = (reversed_pos * returns_aligned).dropna()
    reversed_mean = reversed_pnl.mean()

    return {
        "actual_mean": float(actual_mean),
        "reversed_mean": float(reversed_mean),
        "pass": bool(actual_mean > 0 and reversed_mean < 0),
    }


def build_placebo_series(
    events: pd.Series,
    regime_state: pd.Series,
    *,
    density_match: bool = True,
    regime_match: bool = True,
    n: int = 500,
    random_seed: int = _DEFAULT_RANDOM_SEED,
) -> list[pd.Series]:
    """Build n regime-matched random timestamp series (M4).

    Each returned series has the same density and regime distribution as
    the real event series but with randomised timing within regime windows.
    Used to test whether the edge is event-driven or regime-driven.

    Args:
        events: boolean or 0/1 Series indexed by bar, True where event fires.
        regime_state: categorical Series with the same index.
        density_match: if True, preserve overall event density.
        regime_match: if True, preserve regime-conditional density.
        n: number of placebo draws to generate.
        random_seed: for reproducibility.
    """
    rng = np.random.default_rng(random_seed)
    idx = events.index
    total_bars = len(idx)
    if total_bars == 0:
        return []

    event_mask = events.fillna(False).astype(bool)
    n_events = int(event_mask.sum())
    if n_events == 0:
        return [pd.Series(False, index=idx, dtype=bool) for _ in range(n)]

    if regime_match and regime_state is not None and len(regime_state) == total_bars:
        regime_arr = regime_state.reindex(idx).fillna("unknown").astype(str).to_numpy()
        # Compute event counts per regime
        real_regimes = regime_arr[event_mask.to_numpy()]
        unique_regimes, regime_counts = np.unique(real_regimes, return_counts=True)
        regime_positions: dict[str, np.ndarray] = {}
        for r in unique_regimes:
            regime_positions[r] = np.where(regime_arr == r)[0]
    else:
        regime_match = False

    placebo_series_list = []
    for _ in range(n):
        placebo = np.zeros(total_bars, dtype=bool)
        if regime_match:
            for regime, count in zip(unique_regimes, regime_counts):
                positions = regime_positions.get(regime, np.array([], dtype=int))
                if len(positions) == 0 or count == 0:
                    continue
                chosen = rng.choice(positions, size=min(int(count), len(positions)), replace=False)
                placebo[chosen] = True
        else:
            chosen = rng.choice(total_bars, size=n_events, replace=False)
            placebo[chosen] = True
        placebo_series_list.append(pd.Series(placebo, index=idx))

    return placebo_series_list


def _quick_lwc(returns_series: pd.Series, kelly_fraction: float = 0.5) -> float:
    """Approximate expected log-wealth contribution in bps (M1 formula)."""
    r = pd.to_numeric(returns_series, errors="coerce").dropna()
    if len(r) < 4:
        return 0.0
    mu = float(r.mean())
    std = float(r.std(ddof=1))
    if std < 1e-12:
        return 0.0
    f = min(kelly_fraction, abs(mu) / (std**2))
    lwc = f * mu - 0.5 * f**2 * std**2
    return lwc * 1e4


def evaluate_specificity_lift(
    real_returns: pd.Series,
    placebo_return_series: list[pd.Series],
    *,
    kelly_fraction: float = 0.5,
    min_specificity_lift_bps: float = 0.3,
    percentile: int = 95,
) -> dict[str, Any]:
    """Gate: U(h) - U_placebo_p95 >= min_specificity_lift (M4).

    Args:
        real_returns: per-event net returns for the actual hypothesis.
        placebo_return_series: list of per-event return series for placebos.
        kelly_fraction: fractional Kelly for LWC computation.
        min_specificity_lift_bps: minimum required lift over placebo p95.
        percentile: placebo quantile to use (95 by default).
    """
    real_lwc = _quick_lwc(real_returns, kelly_fraction)

    if not placebo_return_series:
        return {
            "real_lwc_bps": real_lwc,
            "placebo_p95_lwc_bps": 0.0,
            "specificity_lift_bps": real_lwc,
            "pass": real_lwc >= min_specificity_lift_bps,
            "n_placebo": 0,
        }

    placebo_lwcs = [_quick_lwc(s, kelly_fraction) for s in placebo_return_series]
    placebo_p95 = float(np.percentile(placebo_lwcs, percentile))
    lift = real_lwc - placebo_p95

    return {
        "real_lwc_bps": round(real_lwc, 4),
        "placebo_p95_lwc_bps": round(placebo_p95, 4),
        "specificity_lift_bps": round(lift, 4),
        "pass": bool(lift >= min_specificity_lift_bps),
        "n_placebo": len(placebo_return_series),
    }
