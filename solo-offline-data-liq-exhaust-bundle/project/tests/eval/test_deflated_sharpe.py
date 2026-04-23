from __future__ import annotations

import numpy as np

from project.eval.deflated_sharpe import compute_deflated_sharpe


def test_deflated_sharpe_penalizes_multiple_trials() -> None:
    returns = np.array([0.002] * 80 + [-0.0005] * 20)

    one_trial = compute_deflated_sharpe(returns, periods_per_year=365, n_trials=1)
    many_trials = compute_deflated_sharpe(returns, periods_per_year=365, n_trials=100)

    assert one_trial.sharpe_ratio > 0.0
    assert many_trials.benchmark_sharpe > one_trial.benchmark_sharpe
    assert many_trials.deflated_sharpe_ratio < one_trial.deflated_sharpe_ratio
