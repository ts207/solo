from __future__ import annotations

import pandas as pd

from project.events.registry import EVENT_REGISTRY_SPECS, normalize_phase1_events


def _base_events() -> pd.DataFrame:
    # normalize_phase1_events requires an explicit PIT-safe signal timestamp column.
    # For these tests, we treat the event timestamp itself as the detected/signal time.
    return pd.DataFrame(
        {
            "event_type": [
                "FUNDING_EXTREME_ONSET",
                "FUNDING_PERSISTENCE_TRIGGER",
                "FUNDING_NORMALIZATION_TRIGGER",
                "OI_SPIKE_POSITIVE",
            ],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "signal_ts": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "symbol": ["BTCUSDT"] * 4,
            "event_id": ["e0", "e1", "e2", "e3"],
        }
    )


def test_normalize_phase1_events_subtype_filters_to_exact_event_type():
    events = _base_events()
    spec = EVENT_REGISTRY_SPECS["FUNDING_EXTREME_ONSET"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == 1
    assert normalized["event_type"].iloc[0] == "FUNDING_EXTREME_ONSET"
    assert normalized["event_id"].iloc[0] == "e0"


def test_normalize_phase1_events_without_event_type_column_keeps_rows():
    events = _base_events().drop(columns=["event_type"])
    spec = EVENT_REGISTRY_SPECS["FUNDING_EXTREME_ONSET"]
    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")
    assert len(normalized) == len(events)


def test_normalize_phase1_events_falls_back_from_null_timestamp_columns():
    events = pd.DataFrame(
        {
            "event_type": ["DEPTH_COLLAPSE"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "anchor_ts": [pd.NaT],
            "enter_ts": [pd.NaT],
            "signal_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
            "detected_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "exit_ts": [pd.NaT],
            "symbol": ["BTCUSDT"],
        }
    )
    spec = EVENT_REGISTRY_SPECS["DEPTH_COLLAPSE"]

    normalized = normalize_phase1_events(events=events, spec=spec, run_id="r1")

    assert len(normalized) == 1
    row = normalized.iloc[0]
    assert row["phenom_enter_ts"] == events.loc[0, "timestamp"]
    assert row["enter_ts"] == events.loc[0, "signal_ts"]
    assert row["exit_ts"] == events.loc[0, "signal_ts"]


def test_registry_uses_canonical_funding_specs():
    assert "FUNDING_PERSISTENCE_TRIGGER" in EVENT_REGISTRY_SPECS
