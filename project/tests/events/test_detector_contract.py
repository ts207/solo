import pandas as pd
import pytest
from project.events.detector_contract import DetectorContract, DetectorContractError


def _minimal_df():
    ts = pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": [100.0 + i for i in range(10)],
            "high": [101.0 + i for i in range(10)],
            "low": [99.0 + i for i in range(10)],
            "volume": [1000.0] * 10,
        }
    )


class _GoodDetector(DetectorContract):
    required_columns = ["close", "high", "low", "volume"]
    lookback_bars = 5
    warmup_bars = 5
    bar_type = "bar_close"

    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        return (df["close"].pct_change().abs() > 0.001).astype(float)

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        sig = self.compute_signal(df)
        rows = df[sig > 0].copy()
        rows["event_type"] = "TEST"
        return rows

    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        pass


def test_good_detector_runs():
    d = _GoodDetector()
    df = _minimal_df()
    sig = d.compute_signal(df)
    assert isinstance(sig, pd.Series)
    assert len(sig) == len(df)


def test_missing_required_column_raises():
    d = _GoodDetector()
    df = _minimal_df().drop(columns=["volume"])
    with pytest.raises(DetectorContractError, match="volume"):
        d.check_required_columns(df)


def test_detect_events_returns_dataframe():
    d = _GoodDetector()
    df = _minimal_df()
    result = d.detect_events(df, {})
    assert isinstance(result, pd.DataFrame)


def test_bar_type_is_valid():
    d = _GoodDetector()
    assert d.bar_type in DetectorContract._VALID_BAR_TYPES
