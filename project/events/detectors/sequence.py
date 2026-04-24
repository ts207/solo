from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Mapping, Optional

import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.events.detectors.base import BaseEventDetector
from project.events.detectors.registry import get_detector
from project.spec_validation import load_ontology_events

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load_registered_event_metadata() -> dict[str, dict[str, Any]]:
    try:
        registry = get_domain_registry()
    except Exception:
        return {}
    return {
        event_type: {
            "sequence_eligible": spec.sequence_eligible,
            "enabled": spec.enabled,
        }
        for event_type, spec in registry.event_definitions.items()
    }


class EventSequenceDetector(BaseEventDetector):
    """
    Composes two detectors (A then B) with a configurable time window between them.
    Anchor/Trigger and window are resolved from the event spec.
    """

    def __init__(
        self,
        anchor_event: Optional[str] = None,
        trigger_event: Optional[str] = None,
        max_window: int = 48,
    ):
        self.anchor_event = anchor_event
        self.trigger_event = trigger_event
        self.max_window = max_window
        self._anchor_detector: Optional[BaseEventDetector] = None
        self._trigger_detector: Optional[BaseEventDetector] = None
        self._spec_resolved = False

    def _resolve_spec(self):
        if self._spec_resolved:
            return

        all_specs = load_ontology_events()
        spec = all_specs.get(self.event_type)
        if not spec:
            log.warning(f"Could not load spec for sequence event {self.event_type}")
            return

        params = spec.get("parameters", {})

        # Resolve anchor and trigger from spec if not provided in init
        if self.anchor_event is None:
            self.anchor_event = params.get("anchor_event")
        if self.trigger_event is None:
            self.trigger_event = params.get("trigger_event")

        # Fallback to naming convention if still missing
        if self.anchor_event is None or self.trigger_event is None:
            parts = self.event_type.replace("SEQ_", "").split("_THEN_")
            if len(parts) == 2:
                # Map common abbreviations in sequence names to canonical event types
                mapping = {
                    "FND_EXTREME": "FND_DISLOC",
                    "LIQ_VACUUM": "LIQUIDITY_VACUUM",
                    "DEPTH_RECOVERY": "VOL_RELAXATION_START", # Better proxy for recovery than collapse
                    "OI_SPIKEPOS": "OI_SPIKE_POSITIVE",
                    "VOL_COMP": "RANGE_COMPRESSION_END",
                    "BREAKOUT": "BREAKOUT_TRIGGER",
                    "EXHAUST": "FORCED_FLOW_EXHAUSTION",
                }
                if self.anchor_event is None:
                    self.anchor_event = mapping.get(parts[0], parts[0])
                if self.trigger_event is None:
                    self.trigger_event = mapping.get(parts[1], parts[1])

        self.max_window = int(params.get("max_gap_bars", params.get("sequence_window", self.max_window)))
        self._spec_resolved = True

    def _ensure_detectors(self):
        self._resolve_spec()

        if self._anchor_detector is None:
            if not self.anchor_event:
                raise ValueError(f"Anchor event not defined for {self.event_type}")
            self._validate_component_event(self.anchor_event, role="anchor")
            self._anchor_detector = get_detector(self.anchor_event)
            if self._anchor_detector is None:
                raise ValueError(f"Unknown anchor event: {self.anchor_event}")

        if self._trigger_detector is None:
            if not self.trigger_event:
                raise ValueError(f"Trigger event not defined for {self.event_type}")
            self._validate_component_event(self.trigger_event, role="trigger")
            self._trigger_detector = get_detector(self.trigger_event)
            if self._trigger_detector is None:
                raise ValueError(f"Unknown trigger event: {self.trigger_event}")

    def _validate_component_event(self, event_name: str, *, role: str) -> None:
        candidate = str(event_name or "").strip()
        if candidate.startswith("SEQ_"):
            raise ValueError(f"Sequence {role} event '{candidate}' is not allowed.")
        metadata = _load_registered_event_metadata().get(candidate, {})
        if isinstance(metadata, dict) and not metadata.get("sequence_eligible", True):
            raise ValueError(f"Sequence {role} event '{candidate}' is not sequence-eligible.")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        self._ensure_detectors()
        # Merge event-specific params with global params
        merged_params = {**params}

        anchor_features = self._anchor_detector.prepare_features(df, **merged_params)
        trigger_features = self._trigger_detector.prepare_features(df, **merged_params)

        # We need to compute both masks to find the sequence
        anchor_mask = self._anchor_detector.compute_raw_mask(df, features=anchor_features, **merged_params)
        trigger_mask = self._trigger_detector.compute_raw_mask(df, features=trigger_features, **merged_params)

        # Namespace features to avoid collisions (e.g. 'close', 'rv_96')
        features = {
            "anchor_mask": anchor_mask,
            "trigger_mask": trigger_mask,
        }
        for k, v in anchor_features.items():
            features[f"anchor_{k}"] = v
        for k, v in trigger_features.items():
            features[f"trigger_{k}"] = v

        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        anchor_mask = features["anchor_mask"].fillna(False).astype(bool)
        trigger_mask = features["trigger_mask"].fillna(False).astype(bool)

        window = int(params.get("max_gap_bars", params.get("sequence_window", self.max_window)))

        # Sequence logic: A occurs at t, B occurs at t+k where 1 <= k <= window
        any_prior_anchor = anchor_mask.shift(1).rolling(window=window, min_periods=1).max().fillna(0).astype(bool)

        return (any_prior_anchor & trigger_mask).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        # Intensity is the average of both intensities at their respective times
        self._ensure_detectors()

        # Reconstruct namespaced features for individual detectors
        anchor_raw_features = {k.replace("anchor_", ""): v for k, v in features.items() if k.startswith("anchor_")}
        trigger_raw_features = {k.replace("trigger_", ""): v for k, v in features.items() if k.startswith("trigger_")}

        a_intensity = self._anchor_detector.compute_intensity(df, features=anchor_raw_features, **params)
        t_intensity = self._trigger_detector.compute_intensity(df, features=trigger_raw_features, **params)

        # Shift anchor intensity forward to match trigger time (approximate/ma)
        window = int(params.get("max_gap_bars", params.get("sequence_window", self.max_window)))
        a_intensity_recent = a_intensity.shift(1).rolling(window=window, min_periods=1).max()

        return (a_intensity_recent.fillna(0.0) + t_intensity.fillna(0.0)) / 2.0

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        # Direction follows the trigger event
        self._ensure_detectors()
        trigger_raw_features = {k.replace("trigger_", ""): v for k, v in features.items() if k.startswith("trigger_")}
        return self._trigger_detector.compute_direction(idx, trigger_raw_features, **params)
