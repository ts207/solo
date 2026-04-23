from __future__ import annotations

import pandas as pd
import pytest

from project.research.holdout_integrity import (
    assert_holdout_split_integrity,
    assert_no_lookahead_join,
)


def test_assert_holdout_split_integrity_passes_ordered_splits():
    events = pd.DataFrame(
        {
            "enter_ts": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "split_label": ["train", "validation", "test"],
        }
    )

    summary = assert_holdout_split_integrity(events)

    assert summary["status"] == "ok"
    assert summary["counts"]["train"] == 1
    assert summary["counts"]["validation"] == 1
    assert summary["counts"]["test"] == 1


def test_assert_holdout_split_integrity_rejects_invalid_label():
    events = pd.DataFrame(
        {
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "split_label": ["dev"],
        }
    )
    with pytest.raises(ValueError, match="invalid split labels"):
        assert_holdout_split_integrity(events)


def test_assert_holdout_split_integrity_rejects_overlap():
    events = pd.DataFrame(
        {
            "enter_ts": pd.to_datetime(
                [
                    "2026-01-01T00:10:00Z",  # train ends after validation starts
                    "2026-01-01T00:05:00Z",
                ],
                utc=True,
            ),
            "split_label": ["train", "validation"],
        }
    )
    with pytest.raises(ValueError, match="train/validation overlap"):
        assert_holdout_split_integrity(events)


def test_assert_no_lookahead_join_rejects_future_feature_timestamp():
    merged = pd.DataFrame(
        {
            "event_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "feature_ts": pd.to_datetime(["2026-01-01T00:05:00Z"], utc=True),
        }
    )
    with pytest.raises(ValueError, match="Lookahead sentinel failed"):
        assert_no_lookahead_join(merged, context="unit_test")
