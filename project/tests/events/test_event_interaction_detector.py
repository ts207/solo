import pandas as pd

from project.events.detectors.interaction import EventInteractionDetector


def test_event_interaction_detector():
    df = pd.DataFrame(
        {
            "symbol": ["BTC", "BTC", "BTC"],
            "event_type": ["E1", "E2", "E3"],
            "signal_ts": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-01 10:15", "2024-01-01 12:00"]
            ),
        }
    )

    det = EventInteractionDetector(
        interaction_name="INT_E1_E2",
        left_id="E1",
        right_id="E2",
        op="confirm",
        lag=pd.Timedelta("30min"),
    )

    res = det.detect(df, "BTC")

    assert len(res) == 1
    assert res.iloc[0]["interaction_id"] == "INT_E1_E2"
    assert res.iloc[0]["signal_ts"] == pd.Timestamp("2024-01-01 10:15")
