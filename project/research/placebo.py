from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)
_DEFAULT_RANDOM_SEED = 0


def _aligned_returns_positions(
    returns: pd.Series, positions: pd.Series
) -> Tuple[pd.Series, pd.Series]:
    df = pd.concat(
        [
            pd.to_numeric(returns, errors="coerce").rename("returns"),
            pd.to_numeric(positions, errors="coerce").rename("positions"),
        ],
        axis=1,
    ).dropna()
    return df["returns"], df["positions"]


def _position_run_lengths(positions: pd.Series) -> List[int]:
    active = positions.fillna(0.0).ne(0.0)
    lengths: List[int] = []
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
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
    random_totals: List[float] = []
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
) -> Dict[str, Any]:
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
