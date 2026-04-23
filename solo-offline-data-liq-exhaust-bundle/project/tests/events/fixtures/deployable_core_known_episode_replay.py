from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.registry import get_detector
from project.events.event_output_schema import validate_event_output_frame
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_replay_baseline import (
    summarize_detector_events,
)


BASELINE_PATH = Path(__file__).with_name("deployable_core_known_episode_baseline.json")
GENERATOR_VERSION = "known_episode_replay_v1"


@dataclass(frozen=True)
class KnownEpisodeFixture:
    episode_id: str
    label: str
    lineage: str
    expected_present: tuple[str, ...]
    expected_absent: tuple[str, ...]
    frame: pd.DataFrame
    params_by_detector: dict[str, dict[str, Any]]


def _base_frame(start: str, periods: int = 3200) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "close": np.full(periods, 100.0),
            "high": np.full(periods, 100.5),
            "low": np.full(periods, 99.5),
            "volume": np.full(periods, 1000.0),
            "depth_usd": np.full(periods, 100_000.0),
            "spread_bps": np.full(periods, 2.0),
            "liquidation_notional": np.full(periods, 100.0),
            "oi_delta_1h": np.zeros(periods),
            "oi_notional": np.full(periods, 10_000.0),
            "ms_oi_state": np.full(periods, 2.5),
            "ms_oi_confidence": np.full(periods, 1.0),
            "ms_oi_entropy": np.zeros(periods),
            "rv_96": np.full(periods, 0.01),
            "range_96": np.full(periods, 0.02),
            "range_med_2880": np.full(periods, 0.02),
            "ms_vol_state": np.full(periods, 2.0),
            "ms_vol_confidence": np.full(periods, 1.0),
            "ms_vol_entropy": np.zeros(periods),
            "close_spot": np.full(periods, 100.0),
            "close_perp": np.full(periods, 100.05),
            "funding_rate_scaled": np.full(periods, 0.00002),
            "ms_funding_state": np.full(periods, 2.5),
            "ms_funding_confidence": np.full(periods, 1.0),
            "ms_funding_entropy": np.zeros(periods),
            "ms_imbalance_24": np.zeros(periods),
            "ms_spread_state": np.full(periods, 1.0),
            "ms_spread_confidence": np.full(periods, 1.0),
            "ms_spread_entropy": np.zeros(periods),
        }
    )
    return frame


def _basis_funding_dislocation_fixture() -> KnownEpisodeFixture:
    frame = _base_frame("2024-02-01")
    frame.loc[frame.index[-6:], "close_perp"] = [100.0, 100.4, 100.9, 102.0, 104.0, 106.0]
    frame.loc[frame.index[-6:], "funding_rate_scaled"] = [
        0.00002,
        0.00004,
        0.00007,
        0.00020,
        0.00035,
        0.00045,
    ]
    return KnownEpisodeFixture(
        episode_id="basis_funding_dislocation_2024_02_synthetic",
        label="Basis and funding dislocation replay slice",
        lineage="reproducible_synthetic_market_slice: basis widens while funding aligns with perp premium",
        expected_present=("BASIS_DISLOC", "FND_DISLOC", "SPOT_PERP_BASIS_SHOCK"),
        expected_absent=tuple(sorted(DEPLOYABLE_CORE_EVENT_TYPES - {"BASIS_DISLOC", "FND_DISLOC", "SPOT_PERP_BASIS_SHOCK"})),
        frame=frame,
        params_by_detector={},
    )


def _liquidity_liquidation_vol_fixture() -> KnownEpisodeFixture:
    frame = _base_frame("2024-03-05")
    stress_idx = frame.index[-12:]
    frame.loc[stress_idx, "depth_usd"] = np.linspace(12_000.0, 4_000.0, len(stress_idx))
    frame.loc[stress_idx, "spread_bps"] = np.linspace(8.0, 34.0, len(stress_idx))
    frame.loc[stress_idx, "ms_imbalance_24"] = np.linspace(0.55, 0.85, len(stress_idx))

    shock_idx = frame.index[-18:]
    close_path = np.concatenate([np.linspace(100.0, 112.0, 9), np.linspace(112.0, 96.0, 9)])
    frame.loc[shock_idx, "close"] = close_path
    frame.loc[shock_idx, "high"] = close_path + np.linspace(1.5, 5.0, len(shock_idx))
    frame.loc[shock_idx, "low"] = close_path - np.linspace(1.5, 7.0, len(shock_idx))
    frame.loc[shock_idx, "rv_96"] = np.linspace(0.03, 0.40, len(shock_idx))
    frame.loc[shock_idx, "range_96"] = np.linspace(0.04, 0.18, len(shock_idx))
    frame.loc[shock_idx, "volume"] = np.linspace(2_000.0, 15_000.0, len(shock_idx))

    cascade_idx = frame.index[-10:]
    frame.loc[cascade_idx, "liquidation_notional"] = np.linspace(8_000.0, 120_000.0, len(cascade_idx))
    frame.loc[cascade_idx, "oi_delta_1h"] = -np.linspace(250.0, 1_400.0, len(cascade_idx))
    frame.loc[cascade_idx, "oi_notional"] = np.linspace(10_000.0, 7_500.0, len(cascade_idx))

    return KnownEpisodeFixture(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        label="Liquidity vacuum, liquidation cascade, and volatility shock replay slice",
        lineage="reproducible_synthetic_market_slice: book thins, liquidation flow spikes, price/realized-vol shock unfolds",
        expected_present=(
            "LIQUIDITY_STRESS_DIRECT",
            "LIQUIDITY_SHOCK",
            "LIQUIDITY_VACUUM",
            "LIQUIDATION_CASCADE",
            "VOL_SPIKE",
            "VOL_SHOCK",
        ),
        expected_absent=tuple(
            sorted(
                DEPLOYABLE_CORE_EVENT_TYPES
                - {
                    "LIQUIDITY_STRESS_DIRECT",
                    "LIQUIDITY_SHOCK",
                    "LIQUIDITY_VACUUM",
                    "LIQUIDATION_CASCADE",
                    "VOL_SPIKE",
                    "VOL_SHOCK",
                }
            )
        ),
        frame=frame,
        params_by_detector={"LIQUIDATION_CASCADE": {"liq_median_window": 20}},
    )


def known_episode_fixtures() -> tuple[KnownEpisodeFixture, ...]:
    return (_basis_funding_dislocation_fixture(), _liquidity_liquidation_vol_fixture())


def _frame_digest(frame: pd.DataFrame) -> str:
    payload = frame.to_json(orient="split", date_format="iso", double_precision=12)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _detector_params(fixture: KnownEpisodeFixture, event_name: str) -> dict[str, Any]:
    params = {"symbol": "BTCUSDT", "timeframe": "5m"}
    params.update(fixture.params_by_detector.get(event_name, {}))
    return params


def detector_params_for_fixture(fixture: KnownEpisodeFixture, event_name: str) -> dict[str, Any]:
    return _detector_params(fixture, event_name)


def build_known_episode_replay_baseline() -> dict[str, Any]:
    episodes: list[dict[str, Any]] = []
    for fixture in known_episode_fixtures():
        detector_results: list[dict[str, Any]] = []
        for event_name in sorted(DEPLOYABLE_CORE_EVENT_TYPES):
            detector = get_detector(event_name)
            if detector is None:
                raise AssertionError(f"Missing deployable-core detector: {event_name}")
            params = _detector_params(fixture, event_name)
            events = detector.detect_events(fixture.frame.copy(deep=True), dict(params))
            validate_event_output_frame(events, require_rows=False)
            detector_results.append(
                summarize_detector_events(
                    detector=detector,
                    events=events,
                    params=params,
                    include_events=False,
                )
            )

        episodes.append(
            {
                "episode_id": fixture.episode_id,
                "label": fixture.label,
                "lineage": fixture.lineage,
                "generator_version": GENERATOR_VERSION,
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "frame_rows": int(len(fixture.frame)),
                "frame_start": fixture.frame["timestamp"].iloc[0].isoformat(),
                "frame_end": fixture.frame["timestamp"].iloc[-1].isoformat(),
                "frame_digest": _frame_digest(fixture.frame),
                "expected_present": list(fixture.expected_present),
                "expected_absent": list(fixture.expected_absent),
                "detector_results": detector_results,
            }
        )

    return {
        "baseline_schema_version": 1,
        "baseline_type": "deployable_core_known_episode_replay",
        "fixture_lineage": "checked_in_reproducible_market_slice_generators",
        "generator_version": GENERATOR_VERSION,
        "episodes": episodes,
    }


def load_known_episode_replay_baseline(path: Path = BASELINE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_known_episode_replay_baseline(
    path: Path = BASELINE_PATH,
    *,
    baseline: dict[str, Any] | None = None,
) -> None:
    payload = baseline if baseline is not None else build_known_episode_replay_baseline()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def compare_known_episode_replay_baseline(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    for key in ("baseline_schema_version", "baseline_type", "fixture_lineage", "generator_version"):
        if baseline.get(key) != current.get(key):
            failures.append(f"{key}: baseline={baseline.get(key)!r} current={current.get(key)!r}")

    baseline_episodes = {str(item.get("episode_id")): item for item in baseline.get("episodes", [])}
    current_episodes = {str(item.get("episode_id")): item for item in current.get("episodes", [])}
    missing = sorted(set(baseline_episodes) - set(current_episodes))
    extra = sorted(set(current_episodes) - set(baseline_episodes))
    if missing:
        failures.append(f"missing current episodes: {missing}")
    if extra:
        failures.append(f"unexpected current episodes: {extra}")

    for episode_id in sorted(set(baseline_episodes) & set(current_episodes)):
        base_episode = baseline_episodes[episode_id]
        current_episode = current_episodes[episode_id]
        for key in sorted(set(base_episode) | set(current_episode)):
            if key == "detector_results":
                continue
            if base_episode.get(key) != current_episode.get(key):
                failures.append(f"{episode_id}: episode metadata drift in {key}")

        base_results = {
            str(item.get("event_name")): item for item in base_episode.get("detector_results", [])
        }
        current_results = {
            str(item.get("event_name")): item for item in current_episode.get("detector_results", [])
        }
        missing_results = sorted(set(base_results) - set(current_results))
        extra_results = sorted(set(current_results) - set(base_results))
        if missing_results:
            failures.append(f"{episode_id}: missing detector results {missing_results}")
        if extra_results:
            failures.append(f"{episode_id}: unexpected detector results {extra_results}")

        for event_name in sorted(set(base_results) & set(current_results)):
            base_result = base_results[event_name]
            current_result = current_results[event_name]
            differing_fields = [
                key
                for key in sorted(set(base_result) | set(current_result))
                if base_result.get(key) != current_result.get(key)
            ]
            if differing_fields:
                failures.append(f"{episode_id}/{event_name}: replay drift in {differing_fields}")
    return failures
