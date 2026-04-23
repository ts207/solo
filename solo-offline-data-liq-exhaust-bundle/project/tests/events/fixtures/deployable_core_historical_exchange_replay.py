from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.events.detectors.registry import get_detector
from project.events.event_output_schema import validate_event_output_frame
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_replay_baseline import (
    summarize_detector_events,
)

FIXTURE_DIR = Path(__file__).with_name("historical_exchange_slices")
SLICE_PATH = FIXTURE_DIR / "btcusdt_bybit_5m_2024_01_01_03_market_context.csv"
BASELINE_PATH = Path(__file__).with_name("deployable_core_historical_exchange_baseline.json")
BASELINE_SCHEMA_VERSION = 1
FIXTURE_SCHEMA_VERSION = "historical_exchange_slice_v1"


@dataclass(frozen=True)
class HistoricalExchangeSlice:
    slice_id: str
    symbol: str
    timeframe: str
    venue: str
    source_lineage: str
    path: Path
    expected_present: tuple[str, ...]
    expected_absent: tuple[str, ...]
    params_by_detector: dict[str, dict[str, Any]]


def historical_exchange_slices() -> tuple[HistoricalExchangeSlice, ...]:
    present = ("LIQUIDATION_CASCADE", "VOL_SHOCK", "VOL_SPIKE")
    return (
        HistoricalExchangeSlice(
            slice_id="btcusdt_bybit_5m_2024_01_01_03_market_context",
            symbol="BTCUSDT",
            timeframe="5m",
            venue="bybit_v5",
            source_lineage=(
                "data/lake/runs/liquidation_std_gate_sho_20260416T102301Z_043299a9a9/"
                "features/perp/BTCUSDT/5m/market_context/year=2024/month=01/"
                "market_context_BTCUSDT_2024-01.parquet"
            ),
            path=SLICE_PATH,
            expected_present=present,
            expected_absent=tuple(sorted(DEPLOYABLE_CORE_EVENT_TYPES - set(present))),
            params_by_detector={"LIQUIDATION_CASCADE": {"liq_median_window": 20}},
        ),
    )


def load_historical_exchange_slice(slice_spec: HistoricalExchangeSlice) -> pd.DataFrame:
    frame = pd.read_csv(slice_spec.path)
    for column in ("timestamp", "funding_event_ts"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame


def frame_digest(frame: pd.DataFrame) -> str:
    payload = frame.to_json(orient="split", date_format="iso", double_precision=12)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def detector_params_for_slice(
    slice_spec: HistoricalExchangeSlice,
    event_name: str,
) -> dict[str, Any]:
    params = {"symbol": slice_spec.symbol, "timeframe": slice_spec.timeframe}
    params.update(slice_spec.params_by_detector.get(event_name, {}))
    return params


def build_historical_exchange_replay_baseline() -> dict[str, Any]:
    slices: list[dict[str, Any]] = []
    for slice_spec in historical_exchange_slices():
        frame = load_historical_exchange_slice(slice_spec)
        detector_results: list[dict[str, Any]] = []
        for event_name in sorted(DEPLOYABLE_CORE_EVENT_TYPES):
            detector = get_detector(event_name)
            if detector is None:
                raise AssertionError(f"Missing deployable-core detector: {event_name}")
            params = detector_params_for_slice(slice_spec, event_name)
            events = detector.detect_events(frame.copy(deep=True), dict(params))
            validate_event_output_frame(events, require_rows=False)
            detector_results.append(
                summarize_detector_events(
                    detector=detector,
                    events=events,
                    params=params,
                    include_events=False,
                )
            )

        slices.append(
            {
                "slice_id": slice_spec.slice_id,
                "symbol": slice_spec.symbol,
                "timeframe": slice_spec.timeframe,
                "venue": slice_spec.venue,
                "source_lineage": slice_spec.source_lineage,
                "fixture_schema_version": FIXTURE_SCHEMA_VERSION,
                "fixture_path": str(slice_spec.path.relative_to(Path(__file__).parents[4])),
                "frame_rows": int(len(frame)),
                "frame_start": frame["timestamp"].iloc[0].isoformat(),
                "frame_end": frame["timestamp"].iloc[-1].isoformat(),
                "frame_digest": frame_digest(frame),
                "expected_present": list(slice_spec.expected_present),
                "expected_absent": list(slice_spec.expected_absent),
                "detector_results": detector_results,
            }
        )

    return {
        "baseline_schema_version": BASELINE_SCHEMA_VERSION,
        "baseline_type": "deployable_core_historical_exchange_replay",
        "fixture_lineage": "checked_in_exchange_feature_slice_csv",
        "fixture_schema_version": FIXTURE_SCHEMA_VERSION,
        "slices": slices,
    }


def load_historical_exchange_replay_baseline(path: Path = BASELINE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_historical_exchange_replay_baseline(
    path: Path = BASELINE_PATH,
    *,
    baseline: dict[str, Any] | None = None,
) -> None:
    payload = baseline if baseline is not None else build_historical_exchange_replay_baseline()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def compare_historical_exchange_replay_baseline(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    for key in (
        "baseline_schema_version",
        "baseline_type",
        "fixture_lineage",
        "fixture_schema_version",
    ):
        if baseline.get(key) != current.get(key):
            failures.append(f"{key}: baseline={baseline.get(key)!r} current={current.get(key)!r}")

    baseline_slices = {str(item.get("slice_id")): item for item in baseline.get("slices", [])}
    current_slices = {str(item.get("slice_id")): item for item in current.get("slices", [])}
    missing = sorted(set(baseline_slices) - set(current_slices))
    extra = sorted(set(current_slices) - set(baseline_slices))
    if missing:
        failures.append(f"missing current slices: {missing}")
    if extra:
        failures.append(f"unexpected current slices: {extra}")

    for slice_id in sorted(set(baseline_slices) & set(current_slices)):
        base_slice = baseline_slices[slice_id]
        current_slice = current_slices[slice_id]
        for key in sorted(set(base_slice) | set(current_slice)):
            if key == "detector_results":
                continue
            if base_slice.get(key) != current_slice.get(key):
                failures.append(f"{slice_id}: slice metadata drift in {key}")

        base_results = {
            str(item.get("event_name")): item for item in base_slice.get("detector_results", [])
        }
        current_results = {
            str(item.get("event_name")): item for item in current_slice.get("detector_results", [])
        }
        missing_results = sorted(set(base_results) - set(current_results))
        extra_results = sorted(set(current_results) - set(base_results))
        if missing_results:
            failures.append(f"{slice_id}: missing detector results {missing_results}")
        if extra_results:
            failures.append(f"{slice_id}: unexpected detector results {extra_results}")

        for event_name in sorted(set(base_results) & set(current_results)):
            base_result = base_results[event_name]
            current_result = current_results[event_name]
            differing_fields = [
                key
                for key in sorted(set(base_result) | set(current_result))
                if base_result.get(key) != current_result.get(key)
            ]
            if differing_fields:
                failures.append(f"{slice_id}/{event_name}: replay drift in {differing_fields}")
    return failures
