import pandas as pd

from project.events.families.statistical import BandBreakDetector


def test_band_break_honors_runtime_threshold_override():
    ts = pd.date_range("2024-01-01", periods=80, freq="5min", tz="UTC")
    close = [100.0] * 70 + [
        101.0,
        102.0,
        103.0,
        104.0,
        105.0,
        106.0,
        107.0,
        108.0,
        109.0,
        110.0,
    ]
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "high": [x + 0.5 for x in close],
            "low": [x - 0.5 for x in close],
            "rv_96": [0.01] * len(close),
        }
    )

    detector = BandBreakDetector()
    loose = detector.prepare_features(df, lookback_window=24, band_z_threshold=2.0)
    strict = detector.prepare_features(df, lookback_window=24, band_z_threshold=3.5)

    assert loose["mult"].dropna().unique().tolist() == [2.0]
    assert strict["mult"].dropna().unique().tolist() == [3.5]
    assert detector.compute_raw_mask(df, features=loose).sum() >= detector.compute_raw_mask(
        df, features=strict
    ).sum()
