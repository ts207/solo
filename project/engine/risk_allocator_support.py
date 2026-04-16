from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd


def _equity_curve_from_pnl(
    pnl: pd.Series, pnl_mode: Literal["dollar", "return"] = "dollar"
) -> pd.Series:
    clean = pd.to_numeric(pnl, errors="coerce").fillna(0.0).astype(float)
    if clean.empty:
        return pd.Series(dtype=float)
    if pnl_mode == "dollar":
        return 1.0 + clean.cumsum()
    return (1.0 + clean).cumprod()


def _clamp_positions_py(raw: np.ndarray, max_new: float) -> np.ndarray:
    n = raw.size
    if n == 0:
        return raw

    first_ok = abs(raw[0]) <= max_new
    if first_ok and n > 1:
        if np.abs(np.diff(raw)).max() <= max_new:
            return raw
    elif first_ok and n == 1:
        return raw

    out = np.empty_like(raw)
    prior = 0.0
    for i in range(n):
        target = raw[i]
        delta = target - prior
        if delta > max_new:
            delta = max_new
        elif delta < -max_new:
            delta = -max_new
        out[i] = prior + delta
        prior = out[i]
    return out
