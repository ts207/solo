"""Regression: ScheduledNewsDetector must apply spec windows even when news columns exist."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd

from project.events.families.temporal import ScheduledNewsDetector


def _make_df(n: int = 100, add_news_col: bool = False) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01 12:00", periods=n, freq="5min", tz="UTC")
    df = pd.DataFrame({"timestamp": ts, "close": np.ones(n)})
    if add_news_col:
        # Column present but all-False — should NOT mask out spec windows
        df["scheduled_news_event"] = False
    return df


def test_spec_windows_applied_when_news_col_all_false():
    """When news column exists but is all-False, spec windows should still trigger."""
    df = _make_df(add_news_col=True)

    fake_spec = {"parameters": {"windows_utc": [{"hour": 12, "minute_start": 0, "minute_end": 10}]}}
    det = ScheduledNewsDetector()
    with patch("project.events.families.temporal.load_event_spec", return_value=fake_spec):
        events = det.detect(df, symbol="BTC")

    # bars 0–2 are in 12:00–12:10 UTC window
    assert len(events) > 0, (
        "Detector returned no events even though spec window covers 12:00-12:10 "
        "and news column is all-False. LT-004 not fixed."
    )


def test_news_col_true_takes_precedence():
    """When news column has True entries, those bars should still fire."""
    df = _make_df(add_news_col=False)
    df["scheduled_news_event"] = False
    df.loc[5, "scheduled_news_event"] = True

    det = ScheduledNewsDetector()
    with patch(
        "project.events.families.temporal.load_event_spec",
        return_value={"parameters": {"windows_utc": []}},
    ):
        events = det.detect(df, symbol="BTC")

    fire_times = {pd.to_datetime(row["timestamp"]) for row in events.to_dict(orient="records")}
    # Signal is emitted on the NEXT bar after detection (12:25 -> 12:30)
    expected = df["timestamp"].iloc[5] + pd.Timedelta(minutes=5)
    assert expected in fire_times, (
        "Bar with news_col=True should fire even with empty spec windows."
    )
