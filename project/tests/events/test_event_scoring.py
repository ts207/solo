import pandas as pd
import numpy as np
from project.events.scoring import score_event_frame, EventScoreColumns


def _make_event_frame(n: int = 20) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    ts = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    return pd.DataFrame(
        {
            "event_type": ["LIQUIDITY_VACUUM"] * n,
            "event_id": [f"lv_BTCUSDT_{i:08d}_000" for i in range(n)],
            "symbol": ["BTCUSDT"] * n,
            "timestamp": ts,
            "enter_ts": ts,
            "exit_ts": ts + pd.Timedelta(hours=1),
            "evt_signal_intensity": rng.uniform(0.001, 0.05, n),
            "severity_bucket": rng.choice(["low", "moderate", "high"], n),
            "spread_z": rng.uniform(0.5, 4.0, n),
            "basis_z": rng.uniform(-2.0, 2.0, n),
            "direction": ["non_directional"] * n,
        }
    )


def test_score_event_frame_returns_expected_columns():
    df = _make_event_frame()
    result = score_event_frame(df)
    for col in EventScoreColumns:
        assert col in result.columns, f"Missing score column: {col}"


def test_score_values_bounded():
    df = _make_event_frame()
    result = score_event_frame(df)
    for col in EventScoreColumns:
        assert result[col].between(0.0, 1.0).all(), f"{col} has values outside [0, 1]"


def test_score_frame_preserves_input_columns():
    df = _make_event_frame()
    result = score_event_frame(df)
    for col in df.columns:
        assert col in result.columns, f"Input column {col} missing from result"


def test_score_no_lookahead_side_effects():
    df = _make_event_frame()
    original_intensity = df["evt_signal_intensity"].copy()
    _ = score_event_frame(df)
    pd.testing.assert_series_equal(df["evt_signal_intensity"], original_intensity)


def test_empty_frame_returns_empty_with_columns():
    df = _make_event_frame(0)
    result = score_event_frame(df)
    assert result.empty
    for col in EventScoreColumns:
        assert col in result.columns


def test_tradeability_score_calculation():
    df = _make_event_frame(5)
    result = score_event_frame(df)

    base_viability = (
        result["cleanliness_score"]
        * result["crowding_score"]
        * result["execution_score"]
        * result["microstructure_score"]
    ).apply(lambda x: np.power(x, 0.25))

    expected = (
        (base_viability * result["severity_score"] * result["novelty_score"])
        .apply(np.sqrt)
        .clip(0.0, 1.0)
    )

    pd.testing.assert_series_equal(
        result["event_tradeability_score"].round(6),
        expected.round(6),
        check_names=False,
    )
