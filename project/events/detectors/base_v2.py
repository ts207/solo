from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.detector_contract import DetectorLogicContract
from project.events.event_output_schema import (
    DetectedEvent,
    empty_event_output_frame,
    normalize_event_output_frame,
)
from project.events.registry import get_detector_contract
from project.events.sparsify import sparsify_mask


class BaseDetectorV2(DetectorLogicContract):
    """Shared v2 detector base with uniform DetectedEvent emission."""

    event_name: str = "UNKNOWN"
    required_columns: tuple[str, ...] = ("timestamp",)

    def __init__(self, **_: Any) -> None:
        self._contract = get_detector_contract(self.event_name)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        return {}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        raise NotImplementedError

    def compute_intensity(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        mask = self.compute_raw_mask(df, features=features, **params)
        return pd.Series(mask.fillna(False).astype(float), index=df.index, dtype=float)

    def compute_severity(
        self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> float:
        return min(max(float(intensity), 0.0), 1.0)

    def compute_confidence(
        self, idx: int, features: Mapping[str, pd.Series], **params: Any
    ) -> float:
        return 0.5

    def compute_data_quality(
        self, idx: int, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        return "ok"

    def compute_metadata(
        self, idx: int, features: Mapping[str, pd.Series], **params: Any
    ) -> Mapping[str, Any]:
        return {}

    def compute_source_features(
        self, idx: int, features: Mapping[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in features.items():
            if hasattr(value, 'iloc'):
                raw = value.iloc[idx]
                if pd.isna(raw):
                    continue
                if isinstance(raw, (np.floating, np.integer)):
                    out[key] = float(raw)
                elif isinstance(raw, (bool, np.bool_)):
                    out[key] = bool(raw)
        return out

    def compute_merge_key(
        self, idx: int, work: pd.DataFrame, features: Mapping[str, pd.Series], **params: Any
    ) -> str | None:
        cluster = str(self.compute_metadata(idx, features, **params).get('cluster_id', self.event_name)).strip().lower()
        symbol = str(params.get('symbol', 'UNKNOWN')).upper()
        return f"{symbol}:{cluster}"

    def compute_cooldown_until(
        self, idx: int, ts: pd.Timestamp, **params: Any
    ):
        cooldown = int(params.get('cooldown_bars', self._contract.cooldown_bars) or 0)
        timeframe = str(params.get('timeframe', '5m')).strip().lower()
        if cooldown <= 0:
            return None
        minutes = 5
        if timeframe.endswith('m'):
            try:
                minutes = int(timeframe[:-1])
            except ValueError:
                minutes = 5
        return ts + timedelta(minutes=minutes * cooldown)

    def build_event(
        self,
        *,
        idx: int,
        ts_start: pd.Timestamp,
        ts_end: pd.Timestamp,
        intensity: float,
        features: Mapping[str, pd.Series],
        params: dict,
        detector_metadata: Mapping[str, Any] | None = None,
        merge_key: str | None = None,
        cooldown_until: pd.Timestamp | None = None,
    ) -> DetectedEvent:
        quality_flag = self.compute_data_quality(idx, features, **params)
        return DetectedEvent(
            event_name=self._contract.event_name,
            event_version=self._contract.event_version,
            detector_class=self._contract.detector_class,
            symbol=str(params.get('symbol', 'UNKNOWN')).upper(),
            timeframe=str(params.get('timeframe', '5m')),
            ts_start=ts_start,
            ts_end=ts_end,
            canonical_family=self._contract.canonical_family,
            subtype=self._contract.subtype,
            phase=self._contract.phase,
            evidence_mode=self._contract.evidence_mode,
            role=self._contract.role,
            confidence=self.compute_confidence(idx, features, **params) if self._contract.supports_confidence else None,
            severity=self.compute_severity(idx, intensity, features, **params) if self._contract.supports_severity else None,
            trigger_value=float(intensity),
            threshold_snapshot={
                'version': self._contract.threshold_schema_version,
                'calibration_mode': self._contract.calibration_mode,
                'merge_gap_bars': self._contract.merge_gap_bars,
                'cooldown_bars': int(params.get('cooldown_bars', self._contract.cooldown_bars) or 0),
            },
            source_features=self.compute_source_features(idx, features, **params),
            detector_metadata={"event_idx": int(idx), **dict(detector_metadata or {})},
            required_context_present=True,
            data_quality_flag=quality_flag if self._contract.emits_quality_flag else 'ok',
            merge_key=merge_key,
            cooldown_until=cooldown_until,
        )

    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        features = self.prepare_features(df)
        return self.compute_intensity(df, features=features)

    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        return None


    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        """Compatibility wrapper for legacy callers expecting BaseEventDetector.detect()."""
        params = dict(params)
        params.setdefault("symbol", symbol)
        params.setdefault("timeframe", params.get("timeframe", "5m"))
        events = self.detect_events(df, params)
        if events.empty:
            return events
        legacy = events.copy()
        legacy["event_type"] = legacy["event_name"]
        legacy["symbol"] = legacy["symbol"].fillna(symbol)
        legacy["timestamp"] = pd.to_datetime(legacy["ts_start"], utc=True, errors="coerce")
        legacy["signal_ts"] = legacy["timestamp"]
        legacy["detected_ts"] = legacy["timestamp"]
        legacy["eval_bar_ts"] = legacy["timestamp"]
        legacy["event_score"] = pd.to_numeric(legacy.get("severity"), errors="coerce").fillna(0.0)
        legacy["evt_signal_intensity"] = pd.to_numeric(legacy.get("trigger_value"), errors="coerce").fillna(0.0)
        # Flatten common metadata for compatibility with legacy analyzers/tests.
        for payload_col in ("detector_metadata", "source_features"):
            if payload_col in legacy.columns:
                extracted = pd.json_normalize(legacy[payload_col].apply(lambda x: x if isinstance(x, dict) else {}))
                for col in extracted.columns:
                    if col not in legacy.columns:
                        legacy[col] = extracted[col]
        return legacy

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return empty_event_output_frame()

        work = df.copy().reset_index(drop=True)
        work['timestamp'] = pd.to_datetime(work['timestamp'], utc=True, errors='coerce')
        features = self.prepare_features(work, **params)
        mask = self.compute_raw_mask(work, features=features, **params)
        intensity_series = self.compute_intensity(work, features=features, **params)
        cooldown = int(params.get("cooldown_bars", self._contract.cooldown_bars) or 0)
        if cooldown > 0:
            indices = sparsify_mask(mask.astype("boolean"), min_spacing=cooldown)
        else:
            indices = np.flatnonzero(mask.fillna(False).to_numpy()).astype(int).tolist()

        rows = []
        for idx in indices:
            ts = work.at[idx, 'timestamp']
            if pd.isna(ts):
                continue
            intensity = float(np.nan_to_num(intensity_series.iloc[idx], nan=1.0))
            merge_key = self.compute_merge_key(idx, work, features, **params)
            cooldown_until = self.compute_cooldown_until(idx, ts, **params)
            event = self.build_event(
                idx=idx,
                ts_start=ts,
                ts_end=ts,
                intensity=intensity,
                features=features,
                params=params,
                detector_metadata=dict(self.compute_metadata(idx, features, **params)),
                merge_key=merge_key,
                cooldown_until=cooldown_until,
            )
            rows.append(event.as_dict())
        return normalize_event_output_frame(pd.DataFrame(rows))


FamilyBaseDetector = BaseDetectorV2
