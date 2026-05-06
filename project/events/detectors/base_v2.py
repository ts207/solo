from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from project.events.detector_contract import DetectorLogicContract
from project.events.event_output_schema import (
    DetectedEvent,
    empty_event_output_frame,
    normalize_event_output_frame,
)
from project.events.sparsify import sparsify_mask
from project.events.polarity import (
    anchor_role_from_event,
    infer_magnitude_from_features,
    infer_semantics_from_event,
    infer_side_from_features,
)


class BaseDetectorV2(DetectorLogicContract):
    """Shared v2 detector base with uniform DetectedEvent emission."""

    event_name: str = "UNKNOWN"
    event_version: str = "v2"
    required_columns: tuple[str, ...] = ("timestamp",)
    supports_confidence: bool = True
    supports_severity: bool = True
    supports_quality_flag: bool = True
    supports_event_side: bool = True
    supports_magnitude: bool = True
    supports_severity_bucket: bool = True
    cooldown_semantics: str = "event_timestamp_plus_cooldown_bars"
    merge_key_strategy: str = "symbol_plus_cluster_id"
    # Safe-by-default: detectors must opt in via class/spec governance.
    promotion_eligible: bool = False
    planning_default: bool = False
    runtime_default: bool = False

    def __init__(self, **_: Any) -> None:
        from project.events.registry import get_detector_contract

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

    def compute_polarity_semantics(
        self, idx: int, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        meta = dict(self.compute_metadata(idx, features, **params))
        return infer_semantics_from_event(
            event_id=self._contract.event_name,
            family=self._contract.canonical_family,
            subtype=self._contract.subtype,
            role=self._contract.role,
            metadata=meta,
        )

    def compute_event_side(
        self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        semantics = self.compute_polarity_semantics(idx, features, **params)
        return infer_side_from_features(features, idx, semantics=semantics, fallback_intensity=float(intensity))[0]

    def compute_polarity_source(
        self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        semantics = self.compute_polarity_semantics(idx, features, **params)
        return infer_side_from_features(features, idx, semantics=semantics, fallback_intensity=float(intensity))[1]

    def compute_magnitude(
        self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> float:
        value, _source = infer_magnitude_from_features(features, idx, fallback=float(intensity))
        return float(value if value is not None else abs(float(intensity)))

    def compute_magnitude_source(
        self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        _value, source = infer_magnitude_from_features(features, idx, fallback=float(intensity))
        return source

    def compute_anchor_role(
        self, idx: int, features: Mapping[str, pd.Series], semantics: str, **params: Any
    ) -> str:
        return anchor_role_from_event(
            role=self._contract.role,
            deployment_disposition=str(getattr(self._contract, "deployment_disposition", "")),
            family=self._contract.canonical_family,
            event_id=self._contract.event_name,
            semantics=semantics,
        )

    def compute_severity_bucket(
        self, idx: int, severity: float, features: Mapping[str, pd.Series], **params: Any
    ) -> str:
        if severity >= 0.90:
            return "extreme"
        if severity >= 0.66:
            return "high"
        if severity >= 0.33:
            return "medium"
        return "low"

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
        computed_metadata = dict(detector_metadata or {})
        quality_flag = self.compute_data_quality(idx, features, **params)
        metadata_trade_eligible = computed_metadata.get("trade_eligible", True)
        severity = self.compute_severity(idx, intensity, features, **params) if self._contract.supports_severity else None
        polarity_semantics = self.compute_polarity_semantics(idx, features, **params)
        event_side = self.compute_event_side(idx, intensity, features, **params) if self._contract.supports_event_side else "unknown"
        polarity_source = self.compute_polarity_source(idx, intensity, features, **params) if self._contract.supports_event_side else "unknown"
        magnitude = self.compute_magnitude(idx, intensity, features, **params) if self._contract.supports_magnitude else None
        magnitude_source = self.compute_magnitude_source(idx, intensity, features, **params) if self._contract.supports_magnitude else "unknown"
        anchor_role = self.compute_anchor_role(idx, features, polarity_semantics, **params)
        severity_bucket = (
            self.compute_severity_bucket(idx, float(severity or 0.0), features, **params)
            if self._contract.supports_severity_bucket
            else "unknown"
        )
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
            severity=severity,
            event_side=event_side,
            magnitude=magnitude,
            severity_bucket=severity_bucket,
            polarity_semantics=polarity_semantics,
            polarity_source=polarity_source,
            magnitude_source=magnitude_source,
            anchor_role=anchor_role,
            trigger_value=float(intensity),
            threshold_snapshot={
                'version': self._contract.threshold_schema_version,
                'calibration_mode': self._contract.calibration_mode,
                'merge_gap_bars': self._contract.merge_gap_bars,
                'cooldown_bars': int(params.get('cooldown_bars', self._contract.cooldown_bars) or 0),
            },
            source_features=self.compute_source_features(idx, features, **params),
            detector_metadata={"event_idx": int(idx), **computed_metadata},
            required_context_present=True,
            data_quality_flag=quality_flag if self._contract.supports_quality_flag else 'ok',
            trade_eligible=bool(metadata_trade_eligible) and str(quality_flag).strip().lower() != "invalid",
            merge_key=merge_key,
            cooldown_until=cooldown_until,
        )

    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        features = self.prepare_features(df)
        return self.compute_intensity(df, features=features)

    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        """Validate generic v2 no-lookahead invariants.

        Timestamp/index checks catch impossible event placement. A bounded
        prefix-stability replay catches detectors that only fire because they
        read rows after the event bar. Replays use the params recorded by
        ``detect_events`` when available; manually constructed frames still get
        timestamp and event_idx validation.
        """
        if event_frame is None or event_frame.empty:
            return
        if df is None or df.empty:
            raise ValueError("cannot validate events against an empty source frame")
        if "timestamp" not in df.columns:
            raise ValueError("source frame missing timestamp column")

        source_ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        valid_source_ts = source_ts.dropna()
        if valid_source_ts.empty:
            raise ValueError("source frame has no valid timestamps")
        if bool(valid_source_ts.duplicated().any()):
            raise ValueError("source frame contains duplicate timestamps")
        if not valid_source_ts.is_monotonic_increasing:
            raise ValueError("source frame timestamps must be monotonic increasing")

        event_ts_col = next(
            (column for column in ("ts_start", "timestamp", "signal_ts") if column in event_frame.columns),
            None,
        )
        if event_ts_col is None:
            raise ValueError("event frame missing ts_start/timestamp/signal_ts column")
        event_ts = pd.to_datetime(event_frame[event_ts_col], utc=True, errors="coerce")
        valid_event_ts = event_ts.dropna()
        if valid_event_ts.empty:
            return
        source_min = valid_source_ts.min()
        source_max = valid_source_ts.max()
        if bool((valid_event_ts < source_min).any()):
            raise ValueError("event frame contains timestamps before source frame start")
        if bool((valid_event_ts > source_max).any()):
            raise ValueError("event frame contains timestamps after source frame end")

        if "ts_end" in event_frame.columns:
            event_end = pd.to_datetime(event_frame["ts_end"], utc=True, errors="coerce")
            valid_end = event_ts.notna() & event_end.notna()
            if bool((event_end[valid_end] < event_ts[valid_end]).any()):
                raise ValueError("event frame contains ts_end before ts_start")
            if bool((event_end.dropna() > source_max).any()):
                raise ValueError("event frame contains ts_end after source frame end")

        if "detector_metadata" not in event_frame.columns:
            return

        event_indices: list[tuple[int, pd.Timestamp, int]] = []
        for row_pos, (ts_value, metadata) in enumerate(zip(event_ts, event_frame["detector_metadata"])):
            if not isinstance(metadata, Mapping) or "event_idx" not in metadata or pd.isna(ts_value):
                continue
            try:
                event_idx = int(metadata["event_idx"])
            except Exception as exc:
                raise ValueError("detector_metadata.event_idx must be an integer") from exc
            if event_idx < 0 or event_idx >= len(source_ts):
                raise ValueError("detector_metadata.event_idx outside source frame bounds")
            source_value = source_ts.iloc[event_idx]
            if pd.isna(source_value):
                raise ValueError("detector_metadata.event_idx points at invalid source timestamp")
            if pd.Timestamp(ts_value) != pd.Timestamp(source_value):
                raise ValueError("event timestamp must equal source timestamp at detector_metadata.event_idx")
            event_indices.append((event_idx, pd.Timestamp(ts_value), row_pos))

        if not event_indices or not event_frame.attrs.get("validated_by_prefix_replay", True):
            return

        params = dict(event_frame.attrs.get("detector_params", {}) or {})
        max_checks = int(event_frame.attrs.get("max_prefix_replay_checks", 25) or 25)
        if len(event_indices) > max_checks:
            positions = np.linspace(0, len(event_indices) - 1, num=max_checks, dtype=int)
            checked = [event_indices[int(pos)] for pos in positions]
        else:
            checked = event_indices
        expected_names = event_frame.get("event_name")
        for event_idx, ts_value, row_pos in checked:
            prefix = df.iloc[: event_idx + 1].copy()
            replay = self.detect_events(prefix, params)
            replay_ts_col = next(
                (column for column in ("ts_start", "timestamp", "signal_ts") if column in replay.columns),
                None,
            )
            if replay.empty or replay_ts_col is None:
                raise ValueError(f"prefix replay did not reproduce {self.event_name} event at {ts_value}")
            replay_ts = pd.to_datetime(replay[replay_ts_col], utc=True, errors="coerce")
            same_ts = replay_ts == ts_value
            if expected_names is not None and "event_name" in replay.columns:
                same_ts = same_ts & (
                    replay["event_name"].astype(str).str.upper() == str(expected_names.iloc[row_pos]).upper()
                )
            if not bool(same_ts.fillna(False).any()):
                raise ValueError(
                    f"prefix replay did not reproduce {self.event_name} event at {ts_value}; "
                    "detector may depend on future rows"
                )


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
        out = normalize_event_output_frame(pd.DataFrame(rows))
        out.attrs["detector_params"] = dict(params or {})
        return out


FamilyBaseDetector = BaseDetectorV2
