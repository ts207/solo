from typing import Any, Mapping

import numpy as np
import pandas as pd
from datetime import datetime

from project.events.detector_contract import DetectorLogicContract
from project.events.event_output_schema import DetectedEvent
from project.events.registry import get_detector_contract

class FamilyBaseDetector(DetectorLogicContract):
    """
    Base class for v2 family detectors.
    Provides standard merge/cooldown, calibration metadata, and quality flags.
    """
    
    event_name: str = "UNKNOWN"
    
    def __init__(self, **kwargs):
        self._contract = get_detector_contract(self.event_name)

    def check_required_columns(self, df: pd.DataFrame) -> None:
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"{self.__class__.__name__} missing required columns: {missing}")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        return {}

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        raise NotImplementedError

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        mask = self.compute_raw_mask(df, features=features, **params)
        return pd.Series(mask.fillna(False).astype(float), index=df.index, dtype=float)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 0.5 # Default moderate severity [0.0, 1.0]

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 0.5 # Default moderate confidence [0.0, 1.0]

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return "ok"

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {}
        
    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        features = self.prepare_features(df)
        return self.compute_intensity(df, features=features)

    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        pass # Implemented generically

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return pd.DataFrame()

        work = df.copy().reset_index(drop=True)
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")

        features = self.prepare_features(work, **params)
        intensity_series = self.compute_intensity(work, features=features, **params)
        mask = self.compute_raw_mask(work, features=features, **params)
        
        indices = np.flatnonzero(mask.fillna(False).to_numpy()).astype(int).tolist()
        
        rows = []
        symbol = params.get("symbol", "UNKNOWN")
        timeframe = params.get("timeframe", "5m")
        
        for sub_idx, idx in enumerate(indices):
            ts = work.at[idx, "timestamp"]
            if pd.isna(ts):
                continue
                
            intensity_val = intensity_series.iloc[idx]
            intensity = float(np.nan_to_num(intensity_val, nan=1.0))
            
            severity = self.compute_severity(idx, intensity, features, **params)
            confidence = self.compute_confidence(idx, features, **params)
            quality_flag = self.compute_data_quality(idx, features, **params)
            
            meta = self.compute_metadata(idx, features, **params)
            
            ev = DetectedEvent(
                event_name=self._contract.event_name,
                event_version=self._contract.event_version,
                detector_class=self._contract.detector_class,
                symbol=symbol,
                timeframe=timeframe,
                ts_start=ts,
                ts_end=ts, # Onset-only by default
                canonical_family=self._contract.canonical_family,
                subtype=self._contract.subtype,
                phase=self._contract.phase,
                evidence_mode=self._contract.evidence_mode,
                role=self._contract.role,
                confidence=confidence if self._contract.supports_confidence else None,
                severity=severity if self._contract.supports_severity else None,
                trigger_value=intensity,
                threshold_snapshot={"version": self._contract.threshold_schema_version},
                source_features={},
                detector_metadata=dict(meta),
                required_context_present=True,
                data_quality_flag=quality_flag if self._contract.emits_quality_flag else "ok",
                merge_key=None,
                cooldown_until=None,
            )
            
            # To dict for dataframe
            rows.append(vars(ev))
            
        events = pd.DataFrame(rows) if rows else pd.DataFrame()
        return events
