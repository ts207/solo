from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.events.detectors.composite import CompositeDetector
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.research.analyzers import run_analyzer_suite
from project.events.detectors.registry import register_detector, get_detector
from project.events.detectors.interaction import EventInteractionDetector
import os

from project.events.contract_registry import load_active_event_contracts, load_research_motif_specs


class CrossAssetInteractionDetector(CompositeDetector):
    """Detects predictive interactions across different assets.
    
    Concretely: Does an OI spike on ETH predict a volatility transition on BTC?
    Does a liquidity vacuum on SOL create a spread opportunity against ETH?
    """
    event_type = "CROSS_ASSET_INTERACTION"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        # Expects multi-asset features to be merged into the dataframe
        # e.g., 'oi_spike_eth', 'vol_btc', 'liq_vacuum_sol', etc.
        features = {}
        for key in df.columns:
            if any(p in key for p in ["oi_spike", "vol_", "liq_vacuum", "spread_"]):
                features[key] = df[key]
        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        # Placeholder for complex interaction logic
        # Typically used via explicit hypothesis search over interactions
        return pd.Series(False, index=df.index)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return pd.Series(0.0, index=df.index)


_ACTIVE_CONTRACTS = load_active_event_contracts()

_DETECTORS = {}
if "CROSS_ASSET_INTERACTION" in _ACTIVE_CONTRACTS:
    _DETECTORS["CROSS_ASSET_INTERACTION"] = CrossAssetInteractionDetector

# Research-only interaction motifs are intentionally excluded from the default
# executable runtime registry. They can be enabled explicitly for offline
# research by setting EDGE_ENABLE_RESEARCH_MOTIFS=1.
def _research_motif_registration_enabled() -> bool:
    return str(os.getenv("EDGE_ENABLE_RESEARCH_MOTIFS", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _register_research_motif_detectors() -> None:
    if not _research_motif_registration_enabled():
        return
    try:
        all_specs = load_research_motif_specs()
        for et, spec in all_specs.items():
            if not et.startswith("INT_"):
                continue
            params = spec.get("parameters", {})

            def make_detector_cls(name, left, right, o, l):
                class DynamicInteractionDetector(EventInteractionDetector):
                    def __init__(self, *args, **kwargs):
                        super().__init__(
                            interaction_name=name,
                            left_id=left,
                            right_id=right,
                            op=o,
                            lag=l,
                        )
                return DynamicInteractionDetector

            _DETECTORS[et] = make_detector_cls(
                et,
                params.get("left_event", ""),
                params.get("right_event", ""),
                params.get("op", "confirm"),
                int(params.get("max_gap_bars", 6)),
            )
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to auto-load interaction events: {e}")


_register_research_motif_detectors()

for et, cls in _DETECTORS.items():
    register_detector(et, cls)


def detect_interaction_family(
    df: pd.DataFrame, symbol: str, event_type: str = "CROSS_ASSET_INTERACTION", **params: Any
) -> pd.DataFrame:
    detector_cls = _DETECTORS.get(event_type)
    if detector_cls is None:
        raise ValueError(f"Unknown interaction event type: {event_type}")
    return detector_cls().detect(df, symbol=symbol, **params)
