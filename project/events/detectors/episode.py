from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.events.episodes import build_episodes
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id


class EpisodeDetector(ThresholdDetector):
    max_gap: int = 0
    anchor_rule: str = "peak"

    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return pd.DataFrame(columns=EVENT_COLUMNS)
        work = df.copy().reset_index(drop=True)
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
        features = self.prepare_features(work, **params)
        mask = self.compute_raw_mask(work, features=features, **params)
        intensity = self.compute_intensity(work, features=features, **params)
        episodes = build_episodes(
            mask, score=intensity, max_gap=int(params.get("max_gap", self.max_gap))
        )
        rows = []
        for sub_idx, episode in enumerate(episodes):
            idx = int(
                episode.peak_idx
                if str(params.get("anchor_rule", self.anchor_rule)).lower() == "peak"
                else episode.start_idx
            )
            ts = work.at[idx, "timestamp"]
            if pd.isna(ts):
                continue
            intensity_val = intensity.iloc[idx] if hasattr(intensity, "iloc") else intensity[idx]
            intensity_scalar = float(np.nan_to_num(intensity_val, nan=1.0))
            severity = self.compute_severity(idx, intensity_scalar, features, **params)
            direction = self.compute_direction(idx, features, **params)
            meta = {
                "causal": bool(self.causal),
                **dict(self.compute_metadata(idx, features, **params)),
            }
            row = emit_event(
                event_type=self.event_type,
                symbol=symbol,
                event_id=format_event_id(self.event_type, symbol, idx, sub_idx),
                eval_bar_ts=ts,
                direction=direction,
                intensity=intensity_scalar,
                severity=severity,
                timeframe_minutes=self.timeframe_minutes,
                causal=self.causal,
                metadata={
                    "start_idx": int(episode.start_idx),
                    "end_idx": int(episode.end_idx),
                    "peak_idx": int(episode.peak_idx),
                    "duration_bars": int(episode.duration_bars),
                    "episode_id": f"{self.event_type.lower()}_{symbol}_{sub_idx:04d}",
                    "event_idx": idx,
                    **meta,
                },
            )
            rows.append(row)
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=EVENT_COLUMNS)
