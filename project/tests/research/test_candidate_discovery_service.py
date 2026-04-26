from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from project.research.services import candidate_discovery_service as svc


def test_resolve_sample_quality_policy_honors_overrides(tmp_path: Path) -> None:
    config = svc.CandidateDiscoveryConfig(
        run_id="run-1",
        symbols=("BTCUSDT",),
        config_paths=("a.yaml",),
        data_root=tmp_path,
        event_type="BASIS_DISLOC",
        timeframe="5m",
        horizon_bars=12,
        out_dir=tmp_path / "out",
        run_mode="exploratory",
        split_scheme_id="default",
        embargo_bars=3,
        purge_bars=2,
        train_only_lambda_used=1.0,
        discovery_profile="standard",
        candidate_generation_method="registry",
        concept_file=None,
        entry_lag_bars=1,
        shift_labels_k=0,
        fees_bps=4.0,
        slippage_bps=2.0,
        cost_bps=6.0,
        cost_calibration_mode="auto",
        cost_min_tob_coverage=0.5,
        cost_tob_tolerance_minutes=30,
        candidate_origin_run_id=None,
        frozen_spec_hash=None,
        min_validation_n_obs=12,
        min_test_n_obs=None,
        min_total_n_obs=40,
    )
    policy = svc._resolve_sample_quality_policy(config)
    assert policy["min_validation_n_obs"] == 12
    assert policy["min_test_n_obs"] == 10
    assert policy["min_total_n_obs"] == 40
    assert policy["explicit_overrides"]["min_validation_n_obs"] is True


def test_execute_candidate_discovery_returns_fast_failure_for_invalid_lag(tmp_path: Path) -> None:
    config = svc.CandidateDiscoveryConfig(
        run_id="run-1",
        symbols=("BTCUSDT",),
        config_paths=("a.yaml",),
        data_root=tmp_path,
        event_type="BASIS_DISLOC",
        timeframe="5m",
        horizon_bars=12,
        out_dir=tmp_path / "out",
        run_mode="exploratory",
        split_scheme_id="default",
        embargo_bars=3,
        purge_bars=2,
        train_only_lambda_used=1.0,
        discovery_profile="standard",
        candidate_generation_method="registry",
        concept_file=None,
        entry_lag_bars=0,
        shift_labels_k=0,
        fees_bps=4.0,
        slippage_bps=2.0,
        cost_bps=6.0,
        cost_calibration_mode="auto",
        cost_min_tob_coverage=0.5,
        cost_tob_tolerance_minutes=30,
        candidate_origin_run_id=None,
        frozen_spec_hash=None,
    )
    result = svc.execute_candidate_discovery(config)
    assert result.exit_code == 1
    assert result.combined_candidates.empty


def test_execute_candidate_discovery_success_path(monkeypatch, tmp_path: Path) -> None:
    events_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
            "enter_ts": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
            "split_label": ["train", "validation", "test"],
        }
    )
    candidates_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "symbol": ["BTCUSDT"],
            "direction": [1.0],
            "family_id": ["basis"],
            "validation_n_obs": [2],
            "test_n_obs": [2],
            "is_discovery_pre_sample_quality": [True],
            "is_discovery": [True],
            "rejected_by_sample_quality": [False],
            "split_label": ["validation"],
        }
    )

    monkeypatch.setattr(svc, "resolve_execution_costs", lambda **kwargs: SimpleNamespace(
        config_digest="digest",
        cost_bps=6.0,
        fee_bps_per_side=4.0,
        slippage_bps_per_fill=2.0,
    ))

    class DummyCalibrator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def estimate(self, symbol, events_df):
            return SimpleNamespace(
                cost_bps=6.0,
                fee_bps_per_side=4.0,
                slippage_bps_per_fill=2.0,
                avg_dynamic_cost_bps=6.0,
                cost_input_coverage=1.0,
                cost_model_valid=True,
                cost_model_source="stub",
                regime_multiplier=1.0,
            )

    monkeypatch.setattr(svc, "ToBRegimeCostCalibrator", DummyCalibrator)
    monkeypatch.setattr(svc, "start_manifest", lambda *args, **kwargs: {"status": "started"})
    monkeypatch.setattr(svc, "finalize_manifest", lambda manifest, status, **kwargs: manifest.update({"status": status, **kwargs}))
    monkeypatch.setattr(svc, "write_candidate_reports", lambda **kwargs: None)
    monkeypatch.setattr(svc, "build_false_discovery_diagnostics", lambda df: {"n": len(df)})
    monkeypatch.setattr(svc, "_split_and_score_candidates", lambda *args, **kwargs: candidates_df.copy())
    monkeypatch.setattr(svc, "apply_validation_multiple_testing", lambda df: df.copy())
    monkeypatch.setattr(svc, "apply_sample_quality_gates", lambda df, **kwargs: df.copy())
    monkeypatch.setattr(svc, "get_prepare_events_diagnostics", lambda df: {"present": True})
    monkeypatch.setattr(svc, "build_prepare_events_diagnostics", lambda **kwargs: {"built": True})
    monkeypatch.setattr(svc, "phase2_split_counts", lambda df: {"validation": 1, "test": 1})

    monkeypatch.setattr(svc.discovery, "bars_to_timeframe", lambda bars: f"{bars}m")
    monkeypatch.setattr(svc.discovery, "resolve_registry_direction_policy", lambda *args, **kwargs: {"policy": "long", "source": "stub", "resolved": True, "direction_sign": 1.0})
    monkeypatch.setattr(svc.discovery, "_synthesize_registry_candidates", lambda **kwargs: candidates_df.copy())
    monkeypatch.setattr(svc, "prepare_events_dataframe", lambda **kwargs: events_df.copy())
    monkeypatch.setattr(svc, "load_features", lambda **kwargs: pd.DataFrame({"timestamp": events_df["timestamp"]}))

    class DummyRegistry:
        def __init__(self):
            self.items = []

        def register(self, hyp):
            self.items.append(hyp)
            return f"hyp-{len(self.items)}"

        def write_artifacts(self, out_dir):
            return "registry-hash"

    monkeypatch.setattr(svc, "HypothesisRegistry", DummyRegistry)

    config = svc.CandidateDiscoveryConfig(
        run_id="run-1",
        symbols=("BTCUSDT",),
        config_paths=("a.yaml",),
        data_root=tmp_path,
        event_type="BASIS_DISLOC",
        timeframe="5m",
        horizon_bars=12,
        out_dir=tmp_path / "out",
        run_mode="exploratory",
        split_scheme_id="default",
        embargo_bars=3,
        purge_bars=2,
        train_only_lambda_used=1.0,
        discovery_profile="standard",
        candidate_generation_method="registry",
        concept_file=None,
        entry_lag_bars=1,
        shift_labels_k=0,
        fees_bps=4.0,
        slippage_bps=2.0,
        cost_bps=6.0,
        cost_calibration_mode="auto",
        cost_min_tob_coverage=0.5,
        cost_tob_tolerance_minutes=30,
        candidate_origin_run_id=None,
        frozen_spec_hash=None,
    )

    result = svc.execute_candidate_discovery(config)
    assert result.exit_code == 0
    assert result.manifest["status"] == "success"
    assert list(result.symbol_candidates) == ["BTCUSDT"]
    assert not result.combined_candidates.empty
    assert result.combined_candidates.loc[0, "hypothesis_id"] == "hyp-1"
