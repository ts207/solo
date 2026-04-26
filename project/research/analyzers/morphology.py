from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.research.analyzers.base import (
    AnalyzerResult,
    BaseEventAnalyzer,
    ensure_timestamp,
    resolve_event_time_column,
    resolve_market_time_column,
    resolve_price_column,
)


def _attach_market_window(
    events: pd.DataFrame,
    market: pd.DataFrame,
    *,
    pre_bars: int,
    post_bars: int,
) -> pd.DataFrame:
    time_col = resolve_event_time_column(events)
    market_time_col = resolve_market_time_column(market)
    price_col = resolve_price_column(market)

    events = events.copy()
    events[time_col] = ensure_timestamp(events[time_col])
    market = market.copy()
    market[market_time_col] = ensure_timestamp(market[market_time_col])
    market = (
        market.dropna(subset=[market_time_col, price_col])
        .sort_values(market_time_col)
        .reset_index(drop=True)
    )
    market = market.rename(columns={market_time_col: "_ts", price_col: "_px"})
    market["_idx"] = np.arange(len(market), dtype=int)

    aligned = pd.merge_asof(
        events.sort_values(time_col),
        market[["_ts", "_px", "_idx"]],
        left_on=time_col,
        right_on="_ts",
        direction="backward",
    )
    px = market["_px"]
    aligned["pre_return_bps"] = np.nan
    aligned["event_move_bps"] = np.nan
    aligned["post_return_bps"] = np.nan
    for i, row in aligned.iterrows():
        idx = row.get("_idx")
        if pd.isna(idx):
            continue
        idx = int(idx)
        event_px = float(px.iloc[idx])
        if idx - pre_bars >= 0:
            aligned.at[i, "pre_return_bps"] = (
                event_px / float(px.iloc[idx - pre_bars]) - 1.0
            ) * 10000.0
        if idx + 1 < len(px):
            aligned.at[i, "event_move_bps"] = (
                float(px.iloc[min(idx + 1, len(px) - 1)]) / event_px - 1.0
            ) * 10000.0
        if idx + post_bars < len(px):
            aligned.at[i, "post_return_bps"] = (
                float(px.iloc[idx + post_bars]) / event_px - 1.0
            ) * 10000.0
    return aligned


class MorphologyAnalyzer(BaseEventAnalyzer):
    name = "morphology"

    def analyze(
        self, events: pd.DataFrame, *, market: pd.DataFrame | None = None, **kwargs: Any
    ) -> AnalyzerResult:
        frame = self.validate_events(events)
        if frame.empty:
            return AnalyzerResult(name=self.name, summary={"n_events": 0}, tables={})
        pre_bars = int(kwargs.get("pre_bars", 3))
        post_bars = int(kwargs.get("post_bars", 3))

        if market is None or market.empty:
            durations = pd.to_numeric(
                frame.get("duration_bars", pd.Series(np.nan, index=frame.index)), errors="coerce"
            )
            intensity_col = (
                "evt_signal_intensity" if "evt_signal_intensity" in frame.columns else "intensity"
            )
            intensity = pd.to_numeric(
                frame.get(intensity_col, pd.Series(np.nan, index=frame.index)), errors="coerce"
            )
            summary = {
                "n_events": len(frame),
                "avg_duration_bars": float(durations.mean()) if durations.notna().any() else None,
                "intensity_mean": float(intensity.mean()) if intensity.notna().any() else None,
                "intensity_p90": float(intensity.quantile(0.9))
                if intensity.notna().any()
                else None,
            }
            return AnalyzerResult(
                name=self.name, summary=summary, tables={"morphology_events": frame}
            )

        aligned = _attach_market_window(
            frame, self.validate_market(market), pre_bars=pre_bars, post_bars=post_bars
        )
        intensity_col = (
            "evt_signal_intensity" if "evt_signal_intensity" in aligned.columns else "intensity"
        )
        intensity = pd.to_numeric(
            aligned.get(intensity_col, pd.Series(np.nan, index=aligned.index)), errors="coerce"
        )
        durations = pd.to_numeric(
            aligned.get("duration_bars", pd.Series(np.nan, index=aligned.index)), errors="coerce"
        )
        summary = {
            "n_events": len(aligned),
            "pre_event_drift_bps": float(aligned["pre_return_bps"].mean())
            if aligned["pre_return_bps"].notna().any()
            else None,
            "event_bar_move_bps": float(aligned["event_move_bps"].mean())
            if aligned["event_move_bps"].notna().any()
            else None,
            "post_event_return_bps": float(aligned["post_return_bps"].mean())
            if aligned["post_return_bps"].notna().any()
            else None,
            "intensity_mean": float(intensity.mean()) if intensity.notna().any() else None,
            "intensity_p90": float(intensity.quantile(0.9)) if intensity.notna().any() else None,
            "avg_duration_bars": float(durations.mean()) if durations.notna().any() else None,
        }
        return AnalyzerResult(
            name=self.name, summary=summary, tables={"morphology_events": aligned}
        )
