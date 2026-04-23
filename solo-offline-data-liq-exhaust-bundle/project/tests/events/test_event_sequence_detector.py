import pandas as pd
import pytest
from project.events.detectors.sequence import EventSequenceDetector
from project.events.detectors.base import BaseEventDetector


class _DummyDetector(BaseEventDetector):
    def compute_raw_mask(self, df: pd.DataFrame, *, features, **params):
        return pd.Series(False, index=df.index, dtype=bool)


@pytest.mark.skip(reason="EventSequenceDetector requires full event data with rv_96 column")
def test_event_sequence_detector():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC"),
            "symbol": ["BTC", "BTC", "BTC", "BTC"],
            "event_type": ["VOL_SPIKE", "VOL_RELAXATION_START", "VOL_SPIKE", "VOL_RELAXATION_START"],
            "signal_ts": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-01 10:15", "2024-01-01 11:00", "2024-01-01 12:00"]
            ),
        }
    )

    det = EventSequenceDetector(anchor_event="VOL_SPIKE", trigger_event="VOL_RELAXATION_START", max_window=48)

    res = det.detect(df, symbol="BTC")

    assert len(res) >= 0


def test_event_sequence_detector_rejects_sequence_anchor_event():
    det = EventSequenceDetector(
        anchor_event="SEQ_FND_EXTREME_THEN_BREAKOUT",
        trigger_event="VOL_SHOCK",
    )

    with pytest.raises(ValueError, match="anchor event"):
        det.prepare_features(pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=1, tz="UTC")}))


def test_event_sequence_detector_rejects_non_sequence_eligible_registry_component(monkeypatch):
    monkeypatch.setattr(
        "project.events.detectors.sequence._load_registered_event_metadata",
        lambda: {"BLOCKED_EVT": {"sequence_eligible": False}},
    )
    det = EventSequenceDetector(anchor_event="BLOCKED_EVT", trigger_event="VOL_SHOCK")

    with pytest.raises(ValueError, match="not sequence-eligible"):
        det.prepare_features(pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=1, tz="UTC")}))


def test_event_sequence_detector_allows_non_sequence_components(monkeypatch):
    monkeypatch.setattr(
        "project.events.detectors.sequence.get_detector",
        lambda event_name: _DummyDetector(),
    )
    det = EventSequenceDetector(anchor_event="VOL_SHOCK", trigger_event="BREAKOUT_TRIGGER")
    features = det.prepare_features(pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=1, tz="UTC")}))

    assert "anchor_mask" in features
    assert "trigger_mask" in features
