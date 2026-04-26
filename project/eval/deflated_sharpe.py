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
    raw_sharpe_ratio: float = 0.0
    raw_benchmark_sharpe: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "sharpe_ratio": self.sharpe_ratio,
            "deflated_sharpe_ratio": self.deflated_sharpe_ratio,
            "probability_positive": self.probability_positive,
            "benchmark_sharpe": self.benchmark_sharpe,
            "n_trials": self.n_trials,
            "n_obs": self.n_obs,
            "passed": self.passed,
            "raw_sharpe_ratio": self.raw_sharpe_ratio,
            "raw_benchmark_sharpe": self.raw_benchmark_sharpe,
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
    trials = max(1, int(n_trials))

    # Keep statistical testing in per-observation Sharpe units. Annualizing before
    # computing the DSR standard error distorts high-frequency series because the
    # standard-error formula is for the raw sample Sharpe estimator.
    raw_sharpe = float(np.mean(values) / np.std(values, ddof=1))
    annualized_sharpe = float(raw_sharpe * math.sqrt(periods))
    raw_benchmark = NormalDist().inv_cdf(1.0 - 0.5 / trials) / math.sqrt(max(1, n_obs - 1))
    annualized_benchmark = float(raw_benchmark * math.sqrt(periods))
    standard_error = math.sqrt(
        max(1e-12, (1.0 + 0.5 * raw_sharpe * raw_sharpe) / max(1, n_obs - 1))
    )
    dsr = (raw_sharpe - raw_benchmark) / standard_error
    probability = NormalDist().cdf(dsr)
    return DeflatedSharpeResult(
        sharpe_ratio=float(annualized_sharpe),
        deflated_sharpe_ratio=float(dsr),
        probability_positive=float(probability),
        benchmark_sharpe=float(annualized_benchmark),
        n_trials=trials,
        n_obs=n_obs,
        passed=bool(probability >= float(min_probability)),
        raw_sharpe_ratio=float(raw_sharpe),
        raw_benchmark_sharpe=float(raw_benchmark),
    )
