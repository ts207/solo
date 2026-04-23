from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PBOResult:
    pbo: float
    logits: list[float]
    fold_count: int
    passed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "pbo": self.pbo,
            "logits": list(self.logits),
            "fold_count": self.fold_count,
            "passed": self.passed,
        }


def estimate_probability_of_backtest_overfitting(
    performance: pd.DataFrame,
    *,
    strategy_col: str = "strategy_id",
    fold_col: str = "fold_id",
    in_sample_col: str = "is_score",
    out_sample_col: str = "oos_score",
    max_pbo: float = 0.20,
) -> PBOResult:
    if performance.empty:
        return PBOResult(1.0, [], 0, False)
    logits: list[float] = []
    for _, group in performance.groupby(fold_col):
        frame = group[[strategy_col, in_sample_col, out_sample_col]].dropna()
        if len(frame) < 2:
            continue
        winner = frame.sort_values(in_sample_col, ascending=False).iloc[0]
        oos_rank = int(
            frame[out_sample_col].rank(method="first", ascending=True).loc[winner.name]
        )
        quantile = oos_rank / (len(frame) + 1.0)
        quantile = float(np.clip(quantile, 1e-6, 1.0 - 1e-6))
        logits.append(float(np.log(quantile / (1.0 - quantile))))
    if not logits:
        return PBOResult(1.0, [], 0, False)
    pbo = float(np.mean([value < 0.0 for value in logits]))
    return PBOResult(pbo=pbo, logits=logits, fold_count=len(logits), passed=bool(pbo <= max_pbo))
