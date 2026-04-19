from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DeflatedSharpeResult:
    sharpe_ratio: float
    deflated_sharpe_ratio: float
    probability_positive: float
    benchmark_sharpe: float
    n_trials: int
    n_obs: int
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "sharpe_ratio": self.sharpe_ratio,
            "deflated_sharpe_ratio": self.deflated_sharpe_ratio,
            "probability_positive": self.probability_positive,
            "benchmark_sharpe": self.benchmark_sharpe,
            "n_trials": self.n_trials,
            "n_obs": self.n_obs,
            "passed": self.passed,
        }


def compute_deflated_sharpe(
    returns: pd.Series | np.ndarray | list[float],
    *,
    periods_per_year: int = 365 * 24 * 12,
    n_trials: int = 1,
    min_probability: float = 0.95,
) -> DeflatedSharpeResult:
    values = pd.to_numeric(pd.Series(returns), errors="coerce").dropna().to_numpy(dtype=float)
    values = values[np.isfinite(values)]
    n_obs = int(values.size)
    if n_obs < 3 or float(np.std(values, ddof=1)) <= 0.0:
        return DeflatedSharpeResult(0.0, 0.0, 0.0, 0.0, int(n_trials), n_obs, False)
    periods = max(1, int(periods_per_year))
    sharpe = float(np.mean(values) / np.std(values, ddof=1) * math.sqrt(periods))
    trials = max(1, int(n_trials))
    benchmark = NormalDist().inv_cdf(1.0 - 0.5 / trials) / math.sqrt(max(1, n_obs - 1))
    benchmark *= math.sqrt(periods)
    standard_error = math.sqrt(max(1e-12, (1.0 + 0.5 * sharpe * sharpe) / max(1, n_obs - 1)))
    dsr = (sharpe - benchmark) / standard_error
    probability = NormalDist().cdf(dsr)
    return DeflatedSharpeResult(
        sharpe_ratio=float(sharpe),
        deflated_sharpe_ratio=float(dsr),
        probability_positive=float(probability),
        benchmark_sharpe=float(benchmark),
        n_trials=trials,
        n_obs=n_obs,
        passed=bool(probability >= float(min_probability)),
    )
