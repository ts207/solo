import pandas as pd
import numpy as np
import pytest
from unittest import mock
from pathlib import Path
from project.research.compile_strategy_blueprints import _event_stats


@mock.patch("project.research.compile_strategy_blueprints.pd.read_parquet")
@mock.patch("project.research.compile_strategy_blueprints.Path.exists")
def test_event_stats_train_only(mock_exists, mock_read_parquet):
    # Mock data setup
    mock_exists.return_value = True

    # Create mock dataframe with some timestamps before and after train_end_date
    df = pd.DataFrame(
        {
            "timestamp": [
                "2023-01-01T00:00:00Z",
                "2023-01-02T00:00:00Z",
                "2023-01-03T00:00:00Z",
                "2023-01-04T00:00:00Z",
            ],
            "event_type": ["TEST_EVENT"] * 4,
            "adverse_move": [0.01, 0.02, 0.05, 0.10],  # the latter two are post-train
            "favorable_move": [0.02, 0.03, 0.08, 0.12],
            "half_life_bars": [10, 15, 30, 40],
        }
    )

    # Needs to match filter_phase1_rows_for_event_type if real spec exists
    # But since we pass a bogus event type without spec, it bypasses spec filter or we just mock.
    mock_read_parquet.return_value = df

    train_end = pd.to_datetime("2023-01-02T12:00:00Z", utc=True)

    stats = _event_stats(run_id="test_run", event_type="UNKNOWN_EVENT", train_end_date=train_end)

    # Should only include the first two rows
    assert len(stats["adverse"]) == 2
    assert stats["adverse"][0] == 0.01
    assert stats["adverse"][1] == 0.02

    assert len(stats["favorable"]) == 2
    assert stats["favorable"][0] == 0.02
    assert stats["favorable"][1] == 0.03

    # If no train_end_date passed, all should be there
    stats_all = _event_stats(run_id="test_run", event_type="UNKNOWN_EVENT")
    assert len(stats_all["adverse"]) == 4
    assert stats_all["adverse"][3] == 0.10
