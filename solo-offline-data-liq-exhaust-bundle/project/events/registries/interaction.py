from __future__ import annotations

import logging
import os
from typing import Any

from project.events.detectors.registry import register_detector

STATIC_INTERACTION_EVENT_TYPES = ("CROSS_ASSET_INTERACTION",)


def get_static_interaction_detectors() -> dict[str, type]:
    from project.events.families.interaction import CrossAssetInteractionDetector

    return {"CROSS_ASSET_INTERACTION": CrossAssetInteractionDetector}


def get_interaction_detectors(
    *,
    include_research_motifs: bool | None = None,
) -> dict[str, type]:
    from project.events.contract_registry import (
        load_active_event_contracts,
        load_research_motif_specs,
    )
    from project.events.detectors.interaction import EventInteractionDetector
    from project.events.families.interaction import CrossAssetInteractionDetector

    detectors: dict[str, type] = {}

    active_contracts = load_active_event_contracts()
    if "CROSS_ASSET_INTERACTION" in active_contracts:
        detectors["CROSS_ASSET_INTERACTION"] = CrossAssetInteractionDetector

    enable_research = include_research_motifs
    if enable_research is None:
        enable_research = str(os.getenv("EDGE_ENABLE_RESEARCH_MOTIFS", "0")).strip().lower() in {
            "1", "true", "yes", "on",
        }

    if enable_research:
        try:
            all_specs = load_research_motif_specs()
            for et, spec in all_specs.items():
                if not et.startswith("INT_"):
                    continue
                if et in detectors:
                    continue
                params = spec.get("parameters", {})

                def make_detector_cls(name: str, left: str, right: str, op: str, lag: int) -> type:
                    class DynamicInteractionDetector(EventInteractionDetector):
                        def __init__(self, *args: Any, **kwargs: Any) -> None:
                            super().__init__(
                                interaction_name=name,
                                left_id=left,
                                right_id=right,
                                op=op,
                                lag=lag,
                            )

                    return DynamicInteractionDetector

                detectors[et] = make_detector_cls(
                    et,
                    params.get("left_event", ""),
                    params.get("right_event", ""),
                    params.get("op", "confirm"),
                    int(params.get("max_gap_bars", 6)),
                )
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to auto-load interaction events: {e}")

    return detectors


def ensure_interaction_detectors_registered(
    *,
    include_research_motifs: bool | None = None,
) -> None:
    for event_type, detector_cls in get_interaction_detectors(
        include_research_motifs=include_research_motifs,
    ).items():
        register_detector(event_type, detector_cls)


__all__ = [
    "STATIC_INTERACTION_EVENT_TYPES",
    "ensure_interaction_detectors_registered",
    "get_interaction_detectors",
    "get_static_interaction_detectors",
]