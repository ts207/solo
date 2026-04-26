"""Bound-search cross-validation (T2.4).

Sweeps authored axes (horizon_bars, entry_lag_bars, severity thresholds) on the
train half, selects on the validation half, and records which candidates should
be evaluated on the test half. This prevents post-hoc bound selection from
inflating in-sample statistics.

Usage (via proposal.search_control.bound_search.enabled=true):
    from project.research.search.bound_search import BoundSearchSweep
    sweep = BoundSearchSweep.from_spec(spec)
    selected = sweep.run(features, hypothesis_generator)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)

_DEFAULT_HORIZON_GRID = [6, 12, 24, 48, 96]
_DEFAULT_LAG_GRID = [0, 1, 2]
_DEFAULT_TRAIN_FRAC = 0.5
_DEFAULT_VAL_FRAC = 0.25


@dataclass(frozen=True)
class BoundAxis:
    name: str
    values: list[Any]


@dataclass
class BoundSearchConfig:
    enabled: bool = False
    horizon_bars_grid: list[int] = field(default_factory=lambda: list(_DEFAULT_HORIZON_GRID))
    entry_lag_bars_grid: list[int] = field(default_factory=lambda: list(_DEFAULT_LAG_GRID))
    train_frac: float = _DEFAULT_TRAIN_FRAC
    val_frac: float = _DEFAULT_VAL_FRAC
    min_val_improvement: float = 0.0  # net-t lift required over train-best
    max_candidates_per_event: int = 3

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> BoundSearchConfig:
        return cls(
            enabled=bool(raw.get("enabled", False)),
            horizon_bars_grid=list(
                raw.get("horizon_bars_grid", _DEFAULT_HORIZON_GRID)
            ),
            entry_lag_bars_grid=list(
                raw.get("entry_lag_bars_grid", _DEFAULT_LAG_GRID)
            ),
            train_frac=float(raw.get("train_frac", _DEFAULT_TRAIN_FRAC)),
            val_frac=float(raw.get("val_frac", _DEFAULT_VAL_FRAC)),
            min_val_improvement=float(raw.get("min_val_improvement", 0.0)),
            max_candidates_per_event=int(raw.get("max_candidates_per_event", 3)),
        )


@dataclass
class BoundSearchResult:
    event_type: str
    best_horizon: int
    best_lag: int
    train_t_stat: float
    val_t_stat: float
    val_improvement: float
    selected: bool
    drop_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "best_horizon": self.best_horizon,
            "best_lag": self.best_lag,
            "train_t_stat": self.train_t_stat,
            "val_t_stat": self.val_t_stat,
            "val_improvement": self.val_improvement,
            "selected": self.selected,
            "drop_reason": self.drop_reason,
        }


def _split_index(n: int, train_frac: float, val_frac: float) -> tuple[int, int]:
    """Return (train_end, val_end) indices."""
    train_end = int(n * train_frac)
    val_end = train_end + int(n * val_frac)
    return train_end, min(val_end, n)


def _quick_t_stat(returns: pd.Series) -> float:
    """Newey-West HAC t-stat approximation for a return series."""
    returns = pd.to_numeric(returns, errors="coerce").dropna()
    n = len(returns)
    if n < 4:
        return 0.0
    mean = float(returns.mean())
    std = float(returns.std(ddof=1))
    if std < 1e-10:
        return 0.0
    # Bartlett kernel lag for NW
    nw_lag = int(4 * (n / 100) ** (2 / 9))
    gamma0 = std**2
    gamma_sum = gamma0
    arr = (returns - mean).to_numpy()
    for lag in range(1, nw_lag + 1):
        weight = 1.0 - lag / (nw_lag + 1)
        cov = float(np.mean(arr[lag:] * arr[:-lag]))
        gamma_sum += 2.0 * weight * cov
    se = max(float(np.sqrt(abs(gamma_sum) / n)), 1e-10)
    return mean / se


class BoundSearchSweep:
    """Sweep horizon/lag axes and select the best-validated parameter set."""

    def __init__(self, config: BoundSearchConfig):
        self.config = config

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> BoundSearchSweep:
        return cls(BoundSearchConfig.from_mapping(raw))

    def run_for_event(
        self,
        event_type: str,
        event_returns: dict[tuple[int, int], pd.Series],
    ) -> BoundSearchResult:
        """Select best (horizon, lag) for one event type.

        Args:
            event_returns: mapping of (horizon_bars, lag_bars) → aligned return Series.
        """
        cfg = self.config
        if not event_returns:
            return BoundSearchResult(
                event_type=event_type,
                best_horizon=_DEFAULT_HORIZON_GRID[0],
                best_lag=0,
                train_t_stat=0.0,
                val_t_stat=0.0,
                val_improvement=0.0,
                selected=False,
                drop_reason="no_event_returns",
            )

        # Find one series length to determine split
        sample_series = next(iter(event_returns.values()))
        n = len(sample_series)
        train_end, val_end = _split_index(n, cfg.train_frac, cfg.val_frac)

        # Stage 1: select best (h, l) on train
        best_train_key: tuple[int, int] = next(iter(event_returns))
        best_train_t = -1e9
        for key, series in event_returns.items():
            t = _quick_t_stat(series.iloc[:train_end])
            if t > best_train_t:
                best_train_t = t
                best_train_key = key

        # Stage 2: evaluate on validation split
        val_series = event_returns[best_train_key]
        val_t = _quick_t_stat(val_series.iloc[train_end:val_end])
        improvement = val_t - best_train_t
        selected = val_t >= cfg.min_val_improvement

        drop_reason = "" if selected else f"val_improvement={improvement:.2f}<{cfg.min_val_improvement}"

        return BoundSearchResult(
            event_type=event_type,
            best_horizon=best_train_key[0],
            best_lag=best_train_key[1],
            train_t_stat=round(best_train_t, 4),
            val_t_stat=round(val_t, 4),
            val_improvement=round(improvement, 4),
            selected=selected,
            drop_reason=drop_reason,
        )

    def results_to_frame(self, results: list[BoundSearchResult]) -> pd.DataFrame:
        return pd.DataFrame([r.to_dict() for r in results])
