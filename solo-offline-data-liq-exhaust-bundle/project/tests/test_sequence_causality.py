import pandas as pd
import pytest
from project.events.sequence_analyzer import detect_sequences


def test_sequence_causality_and_earliest_match():
    # Construct synthetic events A and B where B occurs after A.

    # Event A fires at 10:00 (enter) and is tradable at 10:05 (signal_ts)
    df_A = pd.DataFrame(
        {
            "event_id": ["A1"],
            "enter_ts": [pd.to_datetime("2024-01-01 10:00:00", utc=True)],
            "signal_ts": [pd.to_datetime("2024-01-01 10:05:00", utc=True)],
            "symbol": ["BTCUSDT"],
        }
    )

    # Event B fires multiple times:
    # B1: 10:03 (Before A's signal_ts, should NOT match if causality is strictly signal_ts >= signal_ts)
    # B2: 10:10 (Valid match within gap)
    # B3: 10:15 (Also valid, but earliest-match should pick B2)
    # B4: 11:00 (Outside gap window)
    df_B = pd.DataFrame(
        {
            "event_id": ["B1", "B2", "B3", "B4"],
            "enter_ts": pd.to_datetime(
                [
                    "2024-01-01 10:03:00",
                    "2024-01-01 10:10:00",
                    "2024-01-01 10:15:00",
                    "2024-01-01 11:00:00",
                ],
                utc=True,
            ),
            "signal_ts": pd.to_datetime(
                [
                    "2024-01-01 10:03:00",  # actually violates causality if matched to A
                    "2024-01-01 10:10:00",
                    "2024-01-01 10:15:00",
                    "2024-01-01 11:00:00",
                ],
                utc=True,
            ),
            "symbol": ["BTCUSDT"] * 4,
        }
    )

    df_A["event_type"] = "EVENT_A"
    df_B["event_type"] = "EVENT_B"
    events_df = pd.concat([df_A, df_B]).rename(columns={"event_id": "id"})

    seq_df = detect_sequences(events_df, ["EVENT_A", "EVENT_B"], [pd.Timedelta("15m")], "test_seq")

    assert not seq_df.empty, "Failed to detect valid sequence"
    assert len(seq_df) == 1, f"Expected 1 sequence due to earliest-match, got {len(seq_df)}"

    row = seq_df.iloc[0]

    # 1) Sequence emits only when B is within gap window (B2 is at 10:10, A is at 10:05. Diff is 5m <= 15m)
    # 2) Sequence signal_ts equals B's signal_ts
    assert row["signal_ts"] == pd.to_datetime("2024-01-01 10:10:00", utc=True), (
        "signal_ts must map to last event in chain"
    )

    # 3) Sequence enter_ts equals A's signal_ts (lineage)
    assert row["enter_ts"] == pd.to_datetime("2024-01-01 10:05:00", utc=True), (
        "enter_ts must map to first event in chain"
    )


def test_sequence_out_of_window():
    df_A = pd.DataFrame(
        {
            "event_id": ["A1"],
            "enter_ts": [pd.to_datetime("2024-01-01 10:00:00", utc=True)],
            "signal_ts": [pd.to_datetime("2024-01-01 10:05:00", utc=True)],
            "symbol": ["BTCUSDT"],
        }
    )
    df_B = pd.DataFrame(
        {
            "event_id": ["B1"],
            "enter_ts": [pd.to_datetime("2024-01-01 11:00:00", utc=True)],
            "signal_ts": [pd.to_datetime("2024-01-01 11:05:00", utc=True)],
            "symbol": ["BTCUSDT"],
        }
    )

    df_A["event_type"] = "EVENT_A"
    df_B["event_type"] = "EVENT_B"
    events_df = pd.concat([df_A, df_B]).rename(columns={"event_id": "id"})

    seq_df = detect_sequences(events_df, ["EVENT_A", "EVENT_B"], [pd.Timedelta("10m")], "test_seq")

    assert seq_df.empty, "Sequence emitted despite B occurring outside the allowed window"
