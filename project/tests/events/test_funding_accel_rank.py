import pandas as pd

from project.events.thresholding import percentile_rank_historical, rolling_percentile_rank


def test_rolling_accel_rank_matches_historical_percentile_alignment():
    funding_abs = pd.Series(
        [0.0001, 0.0001, 0.0002, 0.0002, 0.0003, 0.0005, 0.0004, 0.0006],
        dtype=float,
    )
    accel = (funding_abs - funding_abs.shift(2)).clip(lower=0.0)

    historical = percentile_rank_historical(accel, window=3, min_periods=2)
    rolling = rolling_percentile_rank(
        accel,
        window=3,
        min_periods=2,
        shift=0,
        scale=100.0,
    )

    pd.testing.assert_series_equal(rolling, historical)
