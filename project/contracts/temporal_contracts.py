from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

ObservationClock = Literal[
    "bar_open",
    "bar_close",
    "event_timestamp",
    "publish_timestamp",
    "exchange_timestamp",
]

CalibrationMode = Literal[
    "none",
    "rolling",
    "expanding",
    "train_fit",
    "fixed_external",
]

FitScope = Literal[
    "none",
    "streaming",
    "split_train_only",
]

OutputMode = Literal[
    "point_feature",
    "event_detector",
    "alignment",
    "label",
]


@dataclass(frozen=True)
class TemporalContract:
    """
    Defines the temporal assumptions and PIT constraints for a module.
    """

    name: str
    output_mode: OutputMode
    observation_clock: ObservationClock
    invariance: Literal["pit", "stochastic", "unstable"] = "unstable"
    decision_lag_bars: int = 0
    lookback_bars: int | None = None

    uses_current_observation: bool = False
    uses_cross_section: bool = False

    calibration_mode: CalibrationMode = "none"
    fit_scope: FitScope = "none"

    max_source_staleness_bars: int | None = None
    max_source_staleness_timedelta: str | None = None

    requires_sorted_unique_timestamp: bool = True
    requires_monotonic_inputs: bool = True

    approved_primitives: Sequence[str] = field(default_factory=tuple)
    notes: str = ""
