from __future__ import annotations

from project.events.detectors.registry import register_detector

SEQUENCE_EVENT_TYPES = (
    "SEQ_FND_EXTREME_THEN_BREAKOUT",
    "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY",
    "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE",
    "SEQ_VOL_COMP_THEN_BREAKOUT",
)


def get_sequence_detectors() -> dict[str, type]:
    from project.events.families.sequence import (
        SeqFndExtremeThenBreakoutDetector,
        SeqLiqVacuumThenDepthRecoveryDetector,
        SeqOiSpikeposThenVolSpikeDetector,
        SeqVolCompThenBreakoutDetector,
    )

    return {
        "SEQ_FND_EXTREME_THEN_BREAKOUT": SeqFndExtremeThenBreakoutDetector,
        "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY": SeqLiqVacuumThenDepthRecoveryDetector,
        "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE": SeqOiSpikeposThenVolSpikeDetector,
        "SEQ_VOL_COMP_THEN_BREAKOUT": SeqVolCompThenBreakoutDetector,
    }


def ensure_sequence_detectors_registered() -> None:
    for event_type, detector_cls in get_sequence_detectors().items():
        register_detector(event_type, detector_cls)


__all__ = [
    "SEQUENCE_EVENT_TYPES",
    "ensure_sequence_detectors_registered",
    "get_sequence_detectors",
]
