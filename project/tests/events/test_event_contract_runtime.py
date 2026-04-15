from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.events.contract_registry import (
    allowed_runtime_aliases,
    load_active_event_contracts,
    validate_contract_completeness,
)
from project.events.detectors.funding import FundingExtremeOnsetDetector
from project.events.detectors.registry import list_registered_event_types, load_all_detectors
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.families.basis import CrossVenueDesyncDetector
from project.events.families.desync import CrossAssetDesyncDetector
from project.spec_validation import load_ontology_events


def test_active_event_set_parity_across_compiled_runtime_and_audit_loaders() -> None:
    compiled_active_events = set(get_domain_registry().event_ids)
    runtime_active_events = set(EVENT_REGISTRY_SPECS.keys())
    audited_active_events = set(load_ontology_events().keys())

    assert compiled_active_events == runtime_active_events == audited_active_events


def test_default_runtime_detector_registry_excludes_research_motifs() -> None:
    load_all_detectors()
    registered = set(list_registered_event_types())
    active = set(get_domain_registry().event_ids)
    allowed_aliases = set(allowed_runtime_aliases())

    assert not {name for name in registered if name.startswith("INT_")}
    assert registered - active <= allowed_aliases


def test_cross_asset_desync_requires_pair_data_and_does_not_fallback() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=200, freq="5min", tz="UTC"),
            "close": 100 + pd.Series(range(200), dtype=float) * 0.1,
        }
    )

    out = CrossAssetDesyncDetector().detect(df, symbol="BTC")
    assert out.empty


def test_funding_extreme_onset_requires_acceleration_and_persistence() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=12, freq="5min", tz="UTC"),
            "funding_abs_pct": [40, 60, 80, 96, 96, 96, 80, 70, 96, 96, 96, 96],
            "funding_abs": [0.0001, 0.00012, 0.00015, 0.0004, 0.00055, 0.00075, 0.0003, 0.0002, 0.00045, 0.00046, 0.00047, 0.00048],
            "funding_rate_scaled": [0.0, 0.0, 0.0, 0.0004, 0.00055, 0.00075, 0.0003, 0.0, 0.00045, 0.00046, 0.00047, 0.00048],
        }
    )

    out = FundingExtremeOnsetDetector().detect(
        df,
        symbol="BTC",
        extreme_pct=95.0,
        accel_pct=50.0,
        accel_lookback=1,
        persistence_bars=3,
        threshold_window=6,
    )

    assert len(out) == 1
    assert out.iloc[0]["event_type"] == "FUNDING_EXTREME_ONSET"
    assert int(out.iloc[0]["event_idx"]) >= 5
    assert float(out.iloc[0]["funding_persistence_bars"]) >= 3.0
    assert float(out.iloc[0]["funding_accel_rank"]) >= 50.0


def test_cross_venue_desync_emits_episode_onset_once_until_revert() -> None:
    spot = [100.0] * 6 + [100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    perp = [100.0] * 6 + [110.0, 110.0, 110.0, 110.0, 100.1, 100.0]
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=len(spot), freq="5min", tz="UTC"),
            "close_spot": spot,
            "close_perp": perp,
        }
    )

    out = CrossVenueDesyncDetector().detect(
        df,
        symbol="BTC",
        lookback_window=6,
        threshold_quantile=0.75,
        threshold_floor=0.5,
        min_basis_bps=1.0,
        persistence_bars=2,
        revert_z=0.5,
        window_end=6,
    )

    assert len(out) == 1


def test_active_event_contracts_are_complete_and_tiered() -> None:
    contracts = load_active_event_contracts()
    missing = validate_contract_completeness(contracts)

    assert contracts
    assert not missing
    assert all(contract["tier"] for contract in contracts.values())
    assert all(contract["operational_role"] for contract in contracts.values())


def test_active_event_contracts_use_specific_threshold_and_calibration_policies() -> None:
    contracts = load_active_event_contracts()

    assert all(contract["threshold_method"] != "declared_detector_threshold" for contract in contracts.values())
    assert all(
        contract["calibration_method"] != "Documented by detector-specific calibration policy and stability checks."
        for contract in contracts.values()
    )


def test_single_definition_per_exhaustion_detector_class_and_timestamp_prepare_features() -> None:
    text = Path("project/events/detectors/exhaustion.py").read_text(encoding="utf-8")
    assert text.count("class MomentumDivergenceDetector") == 1
    assert text.count("class ClimaxVolumeDetector") == 1
    assert text.count("class FailedContinuationDetector") == 1

    temporal_text = Path("project/events/families/temporal.py").read_text(encoding="utf-8")
    assert temporal_text.count("class FundingTimestampDetector") == 1
    assert temporal_text.count("def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:") >= 1
