from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.registry import get_detector, get_detector_class
from project.events.families.sequence import (
    SeqFndExtremeThenBreakoutDetector,
    SeqLiqVacuumThenDepthRecoveryDetector,
    SeqOiSpikeposThenVolSpikeDetector,
    SeqVolCompThenBreakoutDetector,
)
from project.events.registries.sequence import SEQUENCE_EVENT_TYPES, get_sequence_detectors
from project.events.registries.statistical import (
    STATISTICAL_EVENT_TYPES,
    get_statistical_detectors,
)


def _statistical_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    close[-8:] = close[-9] * np.array([1.0, 1.01, 1.03, 1.06, 1.10, 1.15, 1.18, 1.20])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "rv_96": np.concatenate([np.full(n - 8, 0.001), np.linspace(0.002, 0.02, 8)]),
        }
    )


def test_statistical_and_sequence_families_use_registry_modules() -> None:
    assert {
        "ZSCORE_STRETCH",
        "BAND_BREAK",
        "OVERSHOOT_AFTER_SHOCK",
        "GAP_OVERSHOOT",
    } <= set(STATISTICAL_EVENT_TYPES)
    assert {
        "SEQ_FND_EXTREME_THEN_BREAKOUT",
        "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY",
        "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE",
        "SEQ_VOL_COMP_THEN_BREAKOUT",
    } <= set(SEQUENCE_EVENT_TYPES)


def test_statistical_registry_dispatch_resolves_and_emits_rows() -> None:
    statistical_detectors = get_statistical_detectors()
    detector_cls = get_detector_class("ZSCORE_STRETCH")
    detector = get_detector("ZSCORE_STRETCH")

    assert detector_cls is statistical_detectors["ZSCORE_STRETCH"]
    assert detector is not None

    events = detector.detect(
        _statistical_df(),
        symbol="BTCUSDT",
        timeframe="5m",
        lookback_window=120,
        threshold_window=120,
    )

    assert not events.empty
    assert set(events["event_type"]) == {"ZSCORE_STRETCH"}


def test_sequence_registry_dispatch_resolves_specific_detector_classes() -> None:
    sequence_detectors = get_sequence_detectors()
    assert get_detector_class("SEQ_FND_EXTREME_THEN_BREAKOUT") is SeqFndExtremeThenBreakoutDetector
    assert (
        get_detector_class("SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY")
        is SeqLiqVacuumThenDepthRecoveryDetector
    )
    assert get_detector_class("SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE") is SeqOiSpikeposThenVolSpikeDetector
    assert get_detector_class("SEQ_VOL_COMP_THEN_BREAKOUT") is SeqVolCompThenBreakoutDetector
    assert sequence_detectors["SEQ_FND_EXTREME_THEN_BREAKOUT"] is SeqFndExtremeThenBreakoutDetector
