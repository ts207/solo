import pandas as pd

from project.events.sequence_analyzer import detect_sequences


def test_detect_sequences():
    data = {
        "symbol": ["BTCUSDT"] * 5,
        "event_type": ["A", "B", "C", "A", "B"],
        "signal_ts": [1, 2, 3, 10, 15],
    }
    df = pd.DataFrame(data)

    # Sequence A -> B -> C with gap 2
    res = detect_sequences(df, ["A", "B", "C"], [2, 2], "test_seq")

    assert len(res) == 1
    assert res.iloc[0]["sequence_name"] == "test_seq"
    assert res.iloc[0]["enter_ts"] == 1
    assert res.iloc[0]["signal_ts"] == 3


def test_detect_sequences_gap_violation():
    data = {
        "symbol": ["BTCUSDT"] * 3,
        "event_type": ["A", "B", "C"],
        "signal_ts": [1, 10, 11],  # A-B gap is 9
    }
    df = pd.DataFrame(data)

    # Gap [2, 2]
    res = detect_sequences(df, ["A", "B", "C"], [2, 2], "test_seq")
    assert len(res) == 0
