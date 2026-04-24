
import logging

import pandas as pd
import pytest

from project.features.alignment import align_funding_to_bars, assert_complete_funding_series


def test_graceful_missing_handling(caplog):
    bars = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 00:00", "2023-01-01 01:00"]).tz_localize("UTC")
    })
    funding = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 01:00"]).tz_localize("UTC"),
        "funding_rate_scaled": [0.01]
    })

    aligned = align_funding_to_bars(bars, funding)

    # Expect logging
    with caplog.at_level(logging.WARNING):
        series = assert_complete_funding_series(aligned, symbol="BTC", on_missing="warn", fill_value=999.0)

    assert "Funding alignment gaps found for BTC" in caplog.text
    assert series.iloc[0] == 999.0
    assert series.iloc[1] == 0.01

def test_alignment_gaps():
    # Bars: 0, 1, 2, 3, 4 (hours)
    # Funding: 0, 4 (missing at 1, 2, 3)
    # Staleness: 2h.

    # t=0: match funding t=0. (diff=0) -> OK
    # t=1: match funding t=0. (diff=1h) -> OK
    # t=2: match funding t=0. (diff=2h) -> OK (tolerance is 2h inclusive?)
    # t=3: match funding t=0. (diff=3h) -> Fail (NaN)
    # t=4: match funding t=4. (diff=0) -> OK

    bars = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 00:00", "2023-01-01 01:00",
                                     "2023-01-01 02:00", "2023-01-01 03:00",
                                     "2023-01-01 04:00"]).tz_localize("UTC")
    })

    funding = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 00:00", "2023-01-01 04:00"]).tz_localize("UTC"),
        "funding_rate_scaled": [0.01, 0.02]
    })

    # 2 hour tolerance
    aligned = align_funding_to_bars(bars, funding, max_staleness=pd.Timedelta("2h"))

    # t=0 (idx 0): filled
    assert not pd.isna(aligned.iloc[0]["funding_rate_scaled"])
    # t=1 (idx 1): filled (age 1h)
    assert not pd.isna(aligned.iloc[1]["funding_rate_scaled"])
    # t=2 (idx 2): filled (age 2h)
    assert not pd.isna(aligned.iloc[2]["funding_rate_scaled"])
    # t=3 (idx 3): NaN (age 3h > 2h)
    assert pd.isna(aligned.iloc[3]["funding_rate_scaled"])

    # assert_complete_funding_series should fail
    with pytest.raises(ValueError, match="Funding alignment gaps found"):
        assert_complete_funding_series(aligned)

def test_startup_gap():
    # Bar starts at t=0. Funding starts at t=1.
    bars = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 00:00", "2023-01-01 01:00"]).tz_localize("UTC")
    })
    funding = pd.DataFrame({
        "timestamp": pd.to_datetime(["2023-01-01 01:00"]).tz_localize("UTC"),
        "funding_rate_scaled": [0.01]
    })

    aligned = align_funding_to_bars(bars, funding)

    # t=0: NaN (no prior funding)
    assert pd.isna(aligned.iloc[0]["funding_rate_scaled"])

    with pytest.raises(ValueError):
        assert_complete_funding_series(aligned)
