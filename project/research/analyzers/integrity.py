from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.research.analyzers.base import (
    AnalyzerResult,
    BaseEventAnalyzer,
    ensure_timestamp,
    resolve_event_time_column,
    summarize_counts_by_group,
)


class IntegrityAnalyzer(BaseEventAnalyzer):
    name = "integrity"

    def analyze(
        self, events: pd.DataFrame, *, market: pd.DataFrame | None = None, **kwargs: Any
    ) -> AnalyzerResult:
        frame = self.validate_events(events)
        if frame.empty:
            return AnalyzerResult(name=self.name, summary={"n_events": 0}, tables={})

        time_col = resolve_event_time_column(frame)
        frame[time_col] = ensure_timestamp(frame[time_col])
        frame = frame.dropna(subset=[time_col]).sort_values(time_col).reset_index(drop=True)

        deltas_sec = frame[time_col].diff().dt.total_seconds()
        min_spacing_seconds = float(kwargs.get("min_spacing_seconds", 300.0))
        cluster_rate = (
            float(((deltas_sec <= min_spacing_seconds) & deltas_sec.notna()).mean())
            if len(frame) > 1
            else 0.0
        )

        by_month = {}
        if not frame.empty:
            month_key = frame[time_col].dt.strftime("%Y-%m")
            by_month = {
                str(k): int(v) for k, v in frame.groupby(month_key).size().to_dict().items()
            }

        missing_feature_rejections = (
            int(frame.get("missing_feature_rejection", pd.Series(dtype=bool)).fillna(False).sum())
            if "missing_feature_rejection" in frame.columns
            else 0
        )
        warmup_discard_count = (
            int(frame.get("warmup_discard", pd.Series(dtype=bool)).fillna(False).sum())
            if "warmup_discard" in frame.columns
            else 0
        )
        pit_ok = (
            bool(frame.get("pit_ok", pd.Series([True] * len(frame))).fillna(True).all())
            if len(frame)
            else True
        )

        summary = {
            "n_events": int(len(frame)),
            "cluster_rate": cluster_rate,
            "avg_inter_event_seconds": float(np.nanmean(deltas_sec.to_numpy(dtype=float)))
            if len(frame) > 1
            else None,
            "events_per_month": by_month,
            "events_by_asset": summarize_counts_by_group(frame, "asset"),
            "events_by_type": summarize_counts_by_group(frame, "event_type"),
            "warmup_discard_count": warmup_discard_count,
            "missing_feature_rejection_count": missing_feature_rejections,
            "pit_ok": pit_ok,
        }
        return AnalyzerResult(name=self.name, summary=summary, tables={"integrity_events": frame})
