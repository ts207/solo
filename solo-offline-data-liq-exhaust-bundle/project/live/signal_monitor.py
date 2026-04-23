from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Tuple

_LOG = logging.getLogger(__name__)

# Defaults
_DEFAULT_SILENCE_WARN_SEC: float = 4 * 3600.0   # 4 h with no fires → warn
_DEFAULT_SILENCE_ALERT_SEC: float = 8 * 3600.0  # 8 h → alert (possible detector failure)
_DEFAULT_FILL_WINDOW: int = 20                   # rolling window size for fill calibration
_DEFAULT_MIN_FILL_SAMPLES: int = 5               # don't report calibration until this many fills
_DEFAULT_FILL_WARN_RATIO: float = 0.70           # actual/predicted < 0.70 → warn
_DEFAULT_FILL_ALERT_RATIO: float = 0.50          # < 0.50 → alert


@dataclass(frozen=True)
class SilenceStatus:
    thesis_id: str
    event_family: str
    seconds_since_last_fire: float
    level: str  # "ok" | "warn" | "alert"


@dataclass(frozen=True)
class FillCalibrationStatus:
    thesis_id: str
    sample_count: int
    predicted_mean: float
    actual_rate: float
    calibration_ratio: float   # actual_rate / predicted_mean
    level: str  # "ok" | "warn" | "alert" | "insufficient_data"


@dataclass(frozen=True)
class SignalMonitorReport:
    timestamp: str
    silence_statuses: List[SilenceStatus]
    fill_calibration_statuses: List[FillCalibrationStatus]
    any_alert: bool
    any_warn: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "any_alert": self.any_alert,
            "any_warn": self.any_warn,
            "silence": [
                {
                    "thesis_id": s.thesis_id,
                    "event_family": s.event_family,
                    "seconds_since_last_fire": s.seconds_since_last_fire,
                    "level": s.level,
                }
                for s in self.silence_statuses
            ],
            "fill_calibration": [
                {
                    "thesis_id": f.thesis_id,
                    "sample_count": f.sample_count,
                    "predicted_mean": f.predicted_mean,
                    "actual_rate": f.actual_rate,
                    "calibration_ratio": f.calibration_ratio,
                    "level": f.level,
                }
                for f in self.fill_calibration_statuses
            ],
        }


class SignalMonitor:
    """
    Tracks two continuous health signals that are not covered by LiveQualityGate
    or DecayMonitor:

    1. Signal silence — time since the last event fired per thesis/family.
       Detects detector failure or regime change that has silenced all triggers.

    2. Fill calibration — rolling actual fill rate vs predicted fill probability.
       Detects when the fill model has diverged from live execution conditions.
    """

    def __init__(
        self,
        *,
        silence_warn_sec: float = _DEFAULT_SILENCE_WARN_SEC,
        silence_alert_sec: float = _DEFAULT_SILENCE_ALERT_SEC,
        fill_window: int = _DEFAULT_FILL_WINDOW,
        min_fill_samples: int = _DEFAULT_MIN_FILL_SAMPLES,
        fill_warn_ratio: float = _DEFAULT_FILL_WARN_RATIO,
        fill_alert_ratio: float = _DEFAULT_FILL_ALERT_RATIO,
    ) -> None:
        self.silence_warn_sec = float(silence_warn_sec)
        self.silence_alert_sec = float(silence_alert_sec)
        self.fill_window = max(1, int(fill_window))
        self.min_fill_samples = max(1, int(min_fill_samples))
        self.fill_warn_ratio = float(fill_warn_ratio)
        self.fill_alert_ratio = float(fill_alert_ratio)

        # thesis_id → (event_family, last_fired_at)
        self._last_fire: Dict[str, Tuple[str, datetime]] = {}
        # thesis_id → deque of (predicted_fill_probability, was_filled)
        self._fill_records: Dict[str, Deque[Tuple[float, bool]]] = {}

    def record_event_fired(
        self,
        thesis_id: str,
        event_family: str,
        timestamp: datetime | None = None,
    ) -> None:
        """Call when a non-reject trade intent is emitted for a thesis."""
        ts = timestamp or datetime.now(timezone.utc)
        self._last_fire[str(thesis_id)] = (str(event_family), ts)

    def record_fill_outcome(
        self,
        thesis_id: str,
        predicted_fill_probability: float,
        was_filled: bool,
        timestamp: datetime | None = None,
    ) -> None:
        """Call when an order placed for a thesis is either filled or expired/cancelled."""
        tid = str(thesis_id)
        if tid not in self._fill_records:
            self._fill_records[tid] = deque(maxlen=self.fill_window)
        self._fill_records[tid].append((float(predicted_fill_probability), bool(was_filled)))

    def check(self, now: datetime | None = None) -> SignalMonitorReport:
        ts = now or datetime.now(timezone.utc)

        silence_statuses: list[SilenceStatus] = []
        for tid, (family, last_ts) in self._last_fire.items():
            elapsed = (ts - last_ts).total_seconds()
            if elapsed >= self.silence_alert_sec:
                level = "alert"
                _LOG.warning(
                    "Signal silence ALERT: thesis=%s family=%s silence=%.0fs",
                    tid, family, elapsed,
                )
            elif elapsed >= self.silence_warn_sec:
                level = "warn"
                _LOG.info(
                    "Signal silence WARN: thesis=%s family=%s silence=%.0fs",
                    tid, family, elapsed,
                )
            else:
                level = "ok"
            silence_statuses.append(
                SilenceStatus(
                    thesis_id=tid,
                    event_family=family,
                    seconds_since_last_fire=elapsed,
                    level=level,
                )
            )

        fill_statuses: list[FillCalibrationStatus] = []
        for tid, records in self._fill_records.items():
            n = len(records)
            if n < self.min_fill_samples:
                fill_statuses.append(
                    FillCalibrationStatus(
                        thesis_id=tid,
                        sample_count=n,
                        predicted_mean=0.0,
                        actual_rate=0.0,
                        calibration_ratio=1.0,
                        level="insufficient_data",
                    )
                )
                continue
            predicted_mean = sum(p for p, _ in records) / n
            actual_rate = sum(1 for _, filled in records if filled) / n
            ratio = actual_rate / max(predicted_mean, 1e-9)
            if ratio <= self.fill_alert_ratio:
                level = "alert"
                _LOG.warning(
                    "Fill calibration ALERT: thesis=%s predicted=%.2f actual=%.2f ratio=%.2f",
                    tid, predicted_mean, actual_rate, ratio,
                )
            elif ratio <= self.fill_warn_ratio:
                level = "warn"
                _LOG.info(
                    "Fill calibration WARN: thesis=%s predicted=%.2f actual=%.2f ratio=%.2f",
                    tid, predicted_mean, actual_rate, ratio,
                )
            else:
                level = "ok"
            fill_statuses.append(
                FillCalibrationStatus(
                    thesis_id=tid,
                    sample_count=n,
                    predicted_mean=predicted_mean,
                    actual_rate=actual_rate,
                    calibration_ratio=ratio,
                    level=level,
                )
            )

        any_alert = any(s.level == "alert" for s in silence_statuses + fill_statuses)  # type: ignore[operator]
        any_warn = any(s.level in {"warn", "alert"} for s in silence_statuses + fill_statuses)  # type: ignore[operator]

        return SignalMonitorReport(
            timestamp=ts.isoformat(),
            silence_statuses=silence_statuses,
            fill_calibration_statuses=fill_statuses,
            any_alert=any_alert,
            any_warn=any_warn,
        )
