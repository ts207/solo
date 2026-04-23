from __future__ import annotations

import pandas as pd

from project.research.validation.splits import assign_row_purged_split_labels


def test_row_purge_excludes_exact_rows_on_irregular_timestamps() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01 00:00",
                    "2026-01-01 00:05",
                    "2026-01-01 12:00",
                    "2026-01-02 00:00",
                    "2026-01-05 00:00",
                    "2026-01-05 00:05",
                    "2026-01-10 00:00",
                    "2026-01-10 00:05",
                    "2026-01-11 00:00",
                    "2026-01-12 00:00",
                ],
                utc=True,
            ),
            "value": range(10),
        }
    )

    labeled = assign_row_purged_split_labels(
        frame,
        sort_col="timestamp",
        train_frac=0.5,
        validation_frac=0.2,
        purge_bars=1,
        embargo_bars=1,
    )

    labels_by_value = dict(zip(labeled["value"], labeled["split_label"], strict=True))
    assert labels_by_value == {
        0: "train",
        1: "train",
        2: "train",
        3: "train",
        6: "validation",
        9: "test",
    }
