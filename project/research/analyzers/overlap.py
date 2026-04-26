from __future__ import annotations

from typing import Any

import pandas as pd

from project.research.analyzers.base import (
    AnalyzerResult,
    BaseEventAnalyzer,
    ensure_timestamp,
    resolve_event_time_column,
)


class OverlapAnalyzer(BaseEventAnalyzer):
    name = "overlap"

    def analyze(
        self,
        events: pd.DataFrame,
        *,
        market: pd.DataFrame | None = None,
        reference_events: pd.DataFrame | None = None,
        **kwargs: Any,
    ) -> AnalyzerResult:
        frame = self.validate_events(events)
        ref = (
            self.validate_events(reference_events)
            if reference_events is not None
            else pd.DataFrame()
        )
        if frame.empty or ref.empty:
            return AnalyzerResult(
                name=self.name,
                summary={"n_events": len(frame), "reference_events": len(ref)},
                tables={},
            )

        time_col = resolve_event_time_column(frame)
        ref_time_col = resolve_event_time_column(ref)
        frame = frame.copy()
        ref = ref.copy()
        frame[time_col] = ensure_timestamp(frame[time_col])
        ref[ref_time_col] = ensure_timestamp(ref[ref_time_col])
        lhs = set(frame[time_col].dropna().tolist())
        rhs = set(ref[ref_time_col].dropna().tolist())
        intersection = lhs & rhs
        union = lhs | rhs
        jaccard = float(len(intersection) / len(union)) if union else 0.0
        summary = {
            "n_events": len(frame),
            "reference_events": len(ref),
            "exact_overlap_count": len(intersection),
            "jaccard_overlap": jaccard,
            "lhs_overlap_rate": float(len(intersection) / len(lhs)) if lhs else 0.0,
            "rhs_overlap_rate": float(len(intersection) / len(rhs)) if rhs else 0.0,
        }
        table = (
            pd.DataFrame({"timestamp": sorted(intersection)})
            if intersection
            else pd.DataFrame(columns=["timestamp"])
        )
        return AnalyzerResult(name=self.name, summary=summary, tables={"overlap_timestamps": table})
