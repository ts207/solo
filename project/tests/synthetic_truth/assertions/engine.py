from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from project.events.detectors.registry import get_detector
from project.tests.synthetic_truth.assertions.matchers import (
    Matcher,
)
from project.tests.synthetic_truth.scenarios.factory import ScenarioFactory, ScenarioSpec


@dataclass
class ValidationError:
    error_type: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    passed: bool
    scenario_name: str
    event_type: str
    polarity: str
    errors: list[ValidationError] = field(default_factory=list)
    events_detected: dict[str, int] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> str:
        if self.passed:
            return f"PASS: {self.scenario_name}"
        lines = [f"FAIL: {self.scenario_name}"]
        for err in self.errors:
            lines.append(f"  - {err.error_type}: {err.message}")
        return "\n".join(lines)


class EventTruthValidator:
    def __init__(
        self,
        factory_or_spec: ScenarioFactory | ScenarioSpec,
        symbol: str = "BTCUSDT",
        timing_tolerance: int = 10,
    ):
        if isinstance(factory_or_spec, ScenarioSpec):
            self.factory = ScenarioFactory(factory_or_spec)
        else:
            self.factory = factory_or_spec
        self.symbol = symbol
        self.timing_tolerance = timing_tolerance
        self._matchers: list[Matcher] = []

    def validate(self, seed: int | None = None) -> ValidationResult:
        start_time = time.time()

        df, ground_truth = self.factory.create(seed=seed)
        spec = self.factory.spec

        events = self._run_detectors(df, spec.event_type)

        errors = []
        errors.extend(self._check_expected_triggers(events, ground_truth))
        errors.extend(self._check_false_positives(events, ground_truth))
        errors.extend(self._check_timing(events, ground_truth))

        execution_time_ms = (time.time() - start_time) * 1000

        events_detected = {}
        if not events.empty:
            for event_type in events["event_type"].unique():
                events_detected[str(event_type)] = int(
                    (events["event_type"] == event_type).sum()
                )

        return ValidationResult(
            passed=len(errors) == 0,
            scenario_name=spec.name,
            event_type=spec.event_type,
            polarity=spec.polarity,
            errors=errors,
            events_detected=events_detected,
            execution_time_ms=execution_time_ms,
            details={
                "n_bars": len(df),
                "injection_point": ground_truth.get("injection_point"),
                "injection_duration": ground_truth.get("injection_duration"),
            },
        )

    def _run_detectors(self, df: pd.DataFrame, event_type: str) -> pd.DataFrame:
        detector = get_detector(event_type)
        if detector is None:
            return pd.DataFrame()

        try:
            events = detector.detect(df, symbol=self.symbol)
            return events
        except Exception:
            return pd.DataFrame()

    def _check_expected_triggers(
        self, events: pd.DataFrame, ground_truth: dict
    ) -> list[ValidationError]:
        errors = []
        expected_events = ground_truth.get("expected_events", {})

        for event_type, should_trigger in expected_events.items():
            if not should_trigger:
                continue

            if events.empty:
                errors.append(
                    ValidationError(
                        error_type="MISSED_TRIGGER",
                        message=f"Expected {event_type} but no events detected",
                        details={"event_type": event_type},
                    )
                )
                continue

            detected_types = set(events["event_type"].astype(str).str.upper().unique())
            if event_type.upper() not in detected_types:
                errors.append(
                    ValidationError(
                        error_type="MISSED_TRIGGER",
                        message=f"Expected {event_type} but not detected",
                        details={"event_type": event_type, "detected": list(detected_types)},
                    )
                )

        return errors

    def _check_false_positives(
        self, events: pd.DataFrame, ground_truth: dict
    ) -> list[ValidationError]:
        errors = []
        excluded_events = ground_truth.get("excluded_events", {})

        if events.empty:
            return errors

        detected_types = set(events["event_type"].astype(str).str.upper().unique())

        for event_type in excluded_events:
            if event_type.upper() in detected_types:
                count = int((events["event_type"].astype(str).str.upper() == event_type.upper()).sum())
                errors.append(
                    ValidationError(
                        error_type="FALSE_POSITIVE",
                        message=f"{event_type} detected {count} times (should not fire)",
                        details={"event_type": event_type, "count": count},
                    )
                )

        return errors

    def _check_timing(
        self, events: pd.DataFrame, ground_truth: dict
    ) -> list[ValidationError]:
        errors = []

        if events.empty:
            return errors

        injection_point = ground_truth.get("injection_point", 0)
        injection_duration = ground_truth.get("injection_duration", 20)
        event_type = ground_truth.get("event_type", "")

        target_events = events[events["event_type"].astype(str).str.upper() == event_type.upper()]
        if target_events.empty:
            return errors

        for idx, row in target_events.iterrows():
            meta = row.get("features_payload", {})
            if not isinstance(meta, dict):
                continue
            event_bar = meta.get("event_idx", -1)
            if event_bar < 0:
                continue

            window_start = injection_point - self.timing_tolerance
            window_end = injection_point + injection_duration + self.timing_tolerance

            if not (window_start <= event_bar <= window_end):
                errors.append(
                    ValidationError(
                        error_type="TIMING_ERROR",
                        message=f"Event at bar {event_bar}, expected ~{injection_point}",
                        details={
                            "event_bar": event_bar,
                            "expected_bar": injection_point,
                            "window": (window_start, window_end),
                        },
                    )
                )

        return errors
