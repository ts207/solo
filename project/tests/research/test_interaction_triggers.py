import pandas as pd

from project.events.interaction_analyzer import InteractionOp, detect_interactions


def test_detect_interactions_confirm():
    data = {
        "symbol": ["BTCUSDT"] * 4,
        "event_type": ["A", "B", "A", "C"],
        "signal_ts": [1, 3, 10, 11],
    }
    df = pd.DataFrame(data)

    # A followed by B within lag 5
    res = detect_interactions(df, "A", "B", InteractionOp.CONFIRM, 5, "test_int")
    assert len(res) == 1
    assert res.iloc[0]["interaction_id"] == "test_int"
    assert res.iloc[0]["signal_ts"] == 3


def test_detect_interactions_exclude():
    data = {"symbol": ["BTCUSDT"] * 3, "event_type": ["A", "B", "A"], "signal_ts": [1, 2, 10]}
    df = pd.DataFrame(data)

    # A WITHOUT B within lag 5
    # First A has B at ts=2 (lag 1) -> excluded
    # Second A has no B within [5, 15] -> included
    res = detect_interactions(df, "A", "B", InteractionOp.EXCLUDE, 5, "test_int_ex")
    assert len(res) == 1
    assert res.iloc[0]["signal_ts"] == 10
