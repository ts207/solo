from __future__ import annotations

from typing import Any

import pandas as pd

_VALID_SPLITS = {"train", "validation", "test"}


def _normalize_split_label(value: object) -> str:
    return str(value or "").strip().lower()


def assert_holdout_split_integrity(
    events: pd.DataFrame,
    *,
    time_col: str = "enter_ts",
    split_col: str = "split_label",
) -> dict[str, Any]:
    if events.empty:
        return {
            "status": "empty",
            "counts": {"train": 0, "validation": 0, "test": 0},
        }
    if time_col not in events.columns:
        raise ValueError(f"Holdout integrity sentinel failed: missing `{time_col}` column.")
    if split_col not in events.columns:
        raise ValueError(f"Holdout integrity sentinel failed: missing `{split_col}` column.")

    ts = pd.to_datetime(events[time_col], utc=True, errors="coerce")
    labels = events[split_col].map(_normalize_split_label)

    invalid_label_mask = ~labels.isin(_VALID_SPLITS)
    if invalid_label_mask.any():
        invalid_values = sorted(
            {
                str(v).strip() or "<empty>"
                for v in events.loc[invalid_label_mask, split_col].tolist()
            }
        )
        raise ValueError(
            "Holdout integrity sentinel failed: invalid split labels "
            f"{invalid_values}. Expected one of {sorted(_VALID_SPLITS)}."
        )

    frame = pd.DataFrame({"_ts": ts, "_split": labels})
    frame = frame.dropna(subset=["_ts"])
    if frame.empty:
        raise ValueError("Holdout integrity sentinel failed: all holdout timestamps are null.")

    counts: dict[str, int] = {
        split: int((frame["_split"] == split).sum()) for split in ("train", "validation", "test")
    }
    ranges: dict[str, dict[str, str]] = {}
    for split in ("train", "validation", "test"):
        subset = frame[frame["_split"] == split]
        if subset.empty:
            continue
        ranges[split] = {
            "start": str(subset["_ts"].min().isoformat()),
            "end": str(subset["_ts"].max().isoformat()),
        }

    def _end(split: str) -> pd.Timestamp | None:
        subset = frame[frame["_split"] == split]
        if subset.empty:
            return None
        return subset["_ts"].max()

    def _start(split: str) -> pd.Timestamp | None:
        subset = frame[frame["_split"] == split]
        if subset.empty:
            return None
        return subset["_ts"].min()

    train_end = _end("train")
    val_start = _start("validation")
    val_end = _end("validation")
    test_start = _start("test")

    error_messages = []
    if train_end is not None and val_start is not None and train_end >= val_start:
        error_messages.append("train/validation overlap or inversion detected")
    if val_end is not None and test_start is not None and val_end >= test_start:
        error_messages.append("validation/test overlap or inversion detected")
    if (
        val_start is None
        and train_end is not None
        and test_start is not None
        and train_end >= test_start
    ):
        error_messages.append("train/test overlap or inversion detected")

    if error_messages:
        msg = f"Holdout integrity sentinel failed: {'; '.join(error_messages)}."
        raise ValueError(msg)

    return {
        "status": "ok",
        "counts": counts,
        "ranges": ranges,
    }


def assert_no_lookahead_join(
    merged: pd.DataFrame,
    *,
    event_ts_col: str = "event_ts",
    feature_ts_col: str = "feature_ts",
    context: str = "",
) -> None:
    if merged.empty:
        return
    if event_ts_col not in merged.columns or feature_ts_col not in merged.columns:
        raise ValueError(
            "Lookahead sentinel failed: merged frame missing required timestamp columns "
            f"({event_ts_col}, {feature_ts_col})."
        )

    event_ts = pd.to_datetime(merged[event_ts_col], utc=True, errors="coerce")
    feature_ts = pd.to_datetime(merged[feature_ts_col], utc=True, errors="coerce")
    leak_mask = feature_ts.notna() & event_ts.notna() & (feature_ts > event_ts)
    if leak_mask.any():
        leaked = int(leak_mask.sum())
        sample = (
            merged.loc[leak_mask, [event_ts_col, feature_ts_col]].head(1).to_dict(orient="records")
        )
        location = f" ({context})" if context else ""
        raise ValueError(
            "Lookahead sentinel failed: future feature timestamp joined to event"
            f"{location}; leaked_rows={leaked}, sample={sample}"
        )
