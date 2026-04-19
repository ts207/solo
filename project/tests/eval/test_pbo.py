from __future__ import annotations

import pandas as pd

from project.eval.pbo import estimate_probability_of_backtest_overfitting


def test_pbo_detects_in_sample_winner_that_loses_oos() -> None:
    frame = pd.DataFrame(
        [
            {"fold_id": 1, "strategy_id": "overfit", "is_score": 10.0, "oos_score": -1.0},
            {"fold_id": 1, "strategy_id": "robust", "is_score": 5.0, "oos_score": 3.0},
            {"fold_id": 2, "strategy_id": "overfit", "is_score": 9.0, "oos_score": -2.0},
            {"fold_id": 2, "strategy_id": "robust", "is_score": 4.0, "oos_score": 2.0},
        ]
    )

    result = estimate_probability_of_backtest_overfitting(frame, max_pbo=0.20)

    assert result.pbo == 1.0
    assert result.passed is False
