from __future__ import annotations

import pandas as pd

from project.core.data_quality import summarize_frame_quality


def test_summarize_frame_quality_reports_gaps_duplicates_and_missing_ratio():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00:00",
                    "2026-01-01 00:05:00",
                    "2026-01-01 00:05:00",
                    "2026-01-01 00:20:00",
                ],
                utc=True,
            ),
            "open": [100.0, None, 101.0, 102.0],
            "high": [101.0, 102.0, 102.0, 103.0],
            "low": [99.0, 100.0, 100.0, 101.0],
            "close": [100.5, 101.5, 101.2, 102.4],
            "volume": [10.0, 11.0, 11.0, 12.0],
            "is_gap": [False, True, True, False],
            "gap_len": [0, 2, 2, 0],
        }
    )

    summary = summarize_frame_quality(
        frame,
        expected_minutes=5,
        numeric_cols=["open", "high", "low", "close", "volume"],
        coerced_value_count=3,
    )

    assert summary.duplicate_timestamp_count == 1
    assert summary.timestamp_gap_count >= 1
    assert summary.max_gap_len == 2
    assert summary.gap_ratio == 0.5
    assert summary.coerced_value_count == 3
    assert summary.missing_ratio > 0.0
