from __future__ import annotations

import numpy as np
import pandas as pd


def extract_funding_event_indices(
    df: pd.DataFrame,
    extreme_pct: float,
    accel_pct: float,
    accel_lookback: int,
    persistence_pct: float,
    persistence_bars: int,
    normalization_pct: float,
    normalization_lookback: int,
    min_event_spacing: int,
) -> dict[str, list[int]]:
    """
    Core logic for identifying funding-related events from feature data.
    """
    f_pct = df["funding_abs_pct"].astype(float)
    f_abs = df["funding_abs"].astype(float)

    # 1. Onset
    extreme_raw = ((f_pct >= extreme_pct) & (f_pct.shift(1) < extreme_pct)).fillna(False)

    # 2. Acceleration
    accel = f_abs - f_abs.shift(accel_lookback)
    accel_pos = accel.where(accel > 0)
    # Using a local rolling percentile if not provided
    from project.research.helpers.events import rolling_percentile

    accel_rank = rolling_percentile(accel_pos.astype(float), window=2880)
    accel_raw = ((accel_rank >= accel_pct) & (accel_rank.shift(1) < accel_pct)).fillna(False)

    # 3. Persistence
    high = f_pct.ge(persistence_pct).astype(int)
    run_len = high.groupby((high == 0).cumsum()).cumsum()
    persistence_raw = ((high == 1) & (run_len == persistence_bars)).fillna(False)

    # 4. Normalization
    recent_extreme = (
        (f_pct >= extreme_pct)
        .rolling(window=normalization_lookback, min_periods=1)
        .max()
        .fillna(0)
        .astype(bool)
    )
    normalization_raw = (
        (f_pct <= normalization_pct) & (f_pct.shift(1) > normalization_pct) & recent_extreme
    ).fillna(False)

    persistence_trigger_raw = (accel_raw | persistence_raw).fillna(False)

    raw_map = {
        "FUNDING_EXTREME_ONSET": np.flatnonzero(extreme_raw.values).tolist(),
        "FUNDING_PERSISTENCE_TRIGGER": np.flatnonzero(persistence_trigger_raw.values).tolist(),
        "FUNDING_NORMALIZATION_TRIGGER": np.flatnonzero(normalization_raw.values).tolist(),
    }

    from project.research.helpers.events import sparsify_event_mask

    return {
        k: sparsify_event_mask(
            pd.Series(False, index=df.index).set_axis(v, True), min_event_spacing
        )
        for k, v in raw_map.items()
    }
