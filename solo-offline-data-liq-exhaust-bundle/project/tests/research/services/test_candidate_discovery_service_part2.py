from __future__ import annotations

import json
from types import SimpleNamespace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import project.research.services.candidate_discovery_service as svc
import project.research.experiment_engine as experiment_engine


# Shared helpers (duplicated from part1 to keep each file self-contained)
def _make_features(n_bars: int = 80, *, freq: str = "5min") -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n_bars, freq=freq, tz="UTC")
    close = np.arange(100.0, 100.0 + float(n_bars))
    return pd.DataFrame({"timestamp": ts, "close": close, "atr_14": np.full(n_bars, 1.0)})


def _make_events_from_features(
    features: pd.DataFrame, n_events: int = 30, start_bar: int = 5
) -> pd.DataFrame:
    event_ts = features["timestamp"].iloc[start_bar : start_bar + n_events].reset_index(drop=True)
    return pd.DataFrame({
        "enter_ts": event_ts,
        "timestamp": event_ts,
        "symbol": ["BTCUSDT"] * len(event_ts),
        "event_type": ["VOL_SHOCK"] * len(event_ts),
    })


def _run_candidate_discovery(tmp_path, **overrides):
    config = svc.CandidateDiscoveryConfig(
        run_id="r1", symbols=("BTCUSDT",), config_paths=(), data_root=tmp_path,
        event_type="VOL_SHOCK", timeframe="5m", horizon_bars=24,
        out_dir=tmp_path / "phase2", run_mode="exploratory", split_scheme_id="WF_60_20_20",
        embargo_bars=0, purge_bars=0, train_only_lambda_used=0.0,
        discovery_profile="standard", candidate_generation_method="phase2_v1",
        concept_file=None, entry_lag_bars=1, shift_labels_k=0, fees_bps=None,
        slippage_bps=None, cost_bps=None, cost_calibration_mode="auto",
        cost_min_tob_coverage=0.6, cost_tob_tolerance_minutes=5,
        candidate_origin_run_id=None, frozen_spec_hash=None,
    )
    if overrides:
        config = svc.CandidateDiscoveryConfig(**(config.__dict__ | overrides))
    return svc.execute_candidate_discovery(config)


class _HypothesisRegistry:
    def register(self, hyp):
        return "hyp-1"

    def write_artifacts(self, out_dir):
        return "hash-1"


class _Hypothesis:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def test_run_candidate_discovery_service_passes_registry_root_to_experiment_discovery(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        "project.core.execution_costs.load_configs",
        lambda paths: {"fee_bps_per_side": 4.0, "slippage_bps_per_fill": 2.0},
    )
    events = pd.DataFrame(
        {
            "enter_ts": pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC"),
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="15min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 4,
            "event_type": ["VOL_SHOCK"] * 4,
            "close": [100, 101, 102, 103],
        }
    )
    cands = pd.DataFrame(
        [
            {
                "candidate_id": "cand_exp",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "family_id": "fam_exp",
                "horizon": "24",
            }
        ]
    )
    captured: dict[str, object] = {}

    def _experiment(**kwargs):
        captured["registry_root"] = kwargs.get("registry_root")
        captured["experiment_plan"] = kwargs.get("experiment_plan")
        return cands.copy()

    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(svc, "prepare_events_dataframe", lambda **kwargs: events.copy())
    monkeypatch.setattr(svc.discovery, "_synthesize_experiment_hypotheses", _experiment)
    monkeypatch.setattr(svc.discovery, "bars_to_timeframe", lambda bars: "6h")
    monkeypatch.setattr(svc.discovery, "action_name_from_direction", lambda direction: "LONG")
    monkeypatch.setattr(svc, "HypothesisRegistry", _HypothesisRegistry)
    monkeypatch.setattr(svc, "Hypothesis", _Hypothesis)
    monkeypatch.setattr(
        experiment_engine,
        "build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(hypotheses=[]),
    )

    result = _run_candidate_discovery(
        tmp_path,
        experiment_config="exp.yaml",
        registry_root=tmp_path / "registries",
    )

    assert result.exit_code == 0
    assert captured["registry_root"] == tmp_path / "registries"
    assert captured["experiment_plan"] is not None


def test_split_and_score_candidates_forwards_round_trip_cost(monkeypatch):
    features = _make_features(n_bars=40)
    events = _make_events_from_features(features, n_events=10, start_bar=3)
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "cand_cost",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_cost",
                "horizon": "15m",
                "horizon_bars": 3,
            }
        ]
    )
    captured_cost_bps: list[float] = []

    def _build_event_return_frame(*args, **kwargs):
        captured_cost_bps.append(float(kwargs.get("cost_bps", 0.0)))
        cost_penalty = float(kwargs.get("cost_bps", 0.0)) / 1000.0
        return pd.DataFrame(
            {
                "forward_return": [0.03 - cost_penalty, 0.02 - cost_penalty, 0.01 - cost_penalty],
                "cluster_day": ["a", "b", "c"],
                "split_label": ["train", "validation", "test"],
            }
        )

    monkeypatch.setattr(svc, "build_event_return_frame", _build_event_return_frame)
    out = svc._split_and_score_candidates(
        candidates,
        events,
        horizon_bars=3,
        split_scheme_id="WF_60_20_20",
        purge_bars=0,
        embargo_bars=0,
        bar_duration_minutes=5,
        features_df=features,
        entry_lag_bars=1,
        shift_labels_k=0,
        cost_estimate=SimpleNamespace(
            cost_bps=6.0,
            fee_bps_per_side=4.0,
            slippage_bps_per_fill=2.0,
            round_trip_cost_bps=12.0,
            avg_dynamic_cost_bps=6.0,
            cost_input_coverage=1.0,
            cost_model_valid=True,
            cost_model_source="stub",
            regime_multiplier=1.0,
        ),
        cost_coordinate={
            "config_digest": "digest",
            "execution_model": {},
            "after_cost_includes_funding_carry": False,
            "fee_bps_per_side": 4.0,
            "slippage_bps_per_fill": 2.0,
            "cost_bps": 6.0,
            "round_trip_cost_bps": 12.0,
        },
    )

    assert captured_cost_bps == [12.0, 12.0, 12.0, 12.0, 12.0]
    assert float(out.iloc[0]["round_trip_cost_bps"]) == 12.0


def test_run_candidate_discovery_service_split_scheme_changes_split_plan(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "project.core.execution_costs.load_configs",
        lambda paths: {"fee_bps_per_side": 4.0, "slippage_bps_per_fill": 2.0},
    )
    features = _make_features(n_bars=120)
    events = _make_events_from_features(features, n_events=60, start_bar=10)
    cands = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_1",
                "horizon": "5m",
                "horizon_bars": 1,
            }
        ]
    )

    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(svc, "prepare_events_dataframe", lambda **kwargs: events.copy())
    monkeypatch.setattr(svc, "load_features", lambda **kwargs: features.copy())
    monkeypatch.setattr(
        svc.discovery, "_synthesize_registry_candidates", lambda **kwargs: cands.copy()
    )
    monkeypatch.setattr(svc.discovery, "bars_to_timeframe", lambda bars: "5m")
    monkeypatch.setattr(svc.discovery, "action_name_from_direction", lambda direction: "LONG")
    monkeypatch.setattr(svc, "HypothesisRegistry", _HypothesisRegistry)
    monkeypatch.setattr(svc, "Hypothesis", _Hypothesis)

    result_default = _run_candidate_discovery(
        tmp_path,
        run_id="r_split_default",
        out_dir=tmp_path / "phase2_default",
        split_scheme_id="WF_60_20_20",
    )
    result_alt = _run_candidate_discovery(
        tmp_path,
        run_id="r_split_alt",
        out_dir=tmp_path / "phase2_alt",
        split_scheme_id="WF_50_25_25",
    )

    row_default = result_default.combined_candidates.iloc[0]
    row_alt = result_alt.combined_candidates.iloc[0]
    assert row_default["split_plan_id"] == "TVT_60_20_20"
    assert row_alt["split_plan_id"] == "TVT_50_25_25"
    assert int(row_default["validation_n_obs"]) != int(row_alt["validation_n_obs"])


def test_run_candidate_discovery_service_shift_labels_changes_estimate(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "project.core.execution_costs.load_configs",
        lambda paths: {"fee_bps_per_side": 4.0, "slippage_bps_per_fill": 2.0},
    )
    features = _make_features(n_bars=120)
    events = _make_events_from_features(features, n_events=40, start_bar=10)
    cands = pd.DataFrame(
        [
            {
                "candidate_id": "cand_shift",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_shift",
                "horizon": "5m",
                "horizon_bars": 1,
            }
        ]
    )

    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(svc, "prepare_events_dataframe", lambda **kwargs: events.copy())
    monkeypatch.setattr(svc, "load_features", lambda **kwargs: features.copy())
    monkeypatch.setattr(
        svc.discovery, "_synthesize_registry_candidates", lambda **kwargs: cands.copy()
    )
    monkeypatch.setattr(svc.discovery, "bars_to_timeframe", lambda bars: "5m")
    monkeypatch.setattr(svc.discovery, "action_name_from_direction", lambda direction: "LONG")
    monkeypatch.setattr(svc, "HypothesisRegistry", _HypothesisRegistry)
    monkeypatch.setattr(svc, "Hypothesis", _Hypothesis)

    result_k0 = _run_candidate_discovery(
        tmp_path,
        run_id="r_shift_0",
        out_dir=tmp_path / "phase2_shift_0",
        shift_labels_k=0,
    )
    result_k5 = _run_candidate_discovery(
        tmp_path,
        run_id="r_shift_5",
        out_dir=tmp_path / "phase2_shift_5",
        shift_labels_k=5,
    )

    est0 = float(result_k0.combined_candidates.iloc[0]["estimate_bps"])
    est5 = float(result_k5.combined_candidates.iloc[0]["estimate_bps"])
    assert est0 != est5


def test_run_candidate_discovery_service_cost_bps_changes_estimate(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "project.core.execution_costs.load_configs",
        lambda paths: {"fee_bps_per_side": 4.0, "slippage_bps_per_fill": 2.0},
    )
    features = _make_features(n_bars=120)
    events = _make_events_from_features(features, n_events=40, start_bar=10)
    cands = pd.DataFrame(
        [
            {
                "candidate_id": "cand_cost",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_cost",
                "horizon": "5m",
                "horizon_bars": 1,
            }
        ]
    )

    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(svc, "prepare_events_dataframe", lambda **kwargs: events.copy())
    monkeypatch.setattr(svc, "load_features", lambda **kwargs: features.copy())
    monkeypatch.setattr(
        svc.discovery, "_synthesize_registry_candidates", lambda **kwargs: cands.copy()
    )
    monkeypatch.setattr(svc.discovery, "bars_to_timeframe", lambda bars: "5m")
    monkeypatch.setattr(svc.discovery, "action_name_from_direction", lambda direction: "LONG")
    monkeypatch.setattr(svc, "HypothesisRegistry", _HypothesisRegistry)
    monkeypatch.setattr(svc, "Hypothesis", _Hypothesis)
    monkeypatch.setattr(
        svc,
        "build_event_return_frame",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "forward_return": [
                    0.03 - float(kwargs.get("cost_bps", 0.0)) / 1000.0,
                    0.02 - float(kwargs.get("cost_bps", 0.0)) / 1000.0,
                    0.01 - float(kwargs.get("cost_bps", 0.0)) / 1000.0,
                ],
                "cluster_day": ["a", "b", "c"],
                "split_label": ["train", "validation", "test"],
            }
        ),
    )

    result_free = _run_candidate_discovery(
        tmp_path,
        run_id="r_cost_free",
        out_dir=tmp_path / "phase2_cost_free",
        cost_bps=0.0,
    )
    result_costly = _run_candidate_discovery(
        tmp_path,
        run_id="r_cost_costly",
        out_dir=tmp_path / "phase2_cost_costly",
        cost_bps=25.0,
    )

    row_free = result_free.combined_candidates.iloc[0]
    row_costly = result_costly.combined_candidates.iloc[0]
    assert float(row_costly["estimate_bps"]) < float(row_free["estimate_bps"])
    assert float(row_costly["resolved_cost_bps"]) == 25.0


def test_split_and_score_candidates_uses_candidate_risk_params(monkeypatch):
    features = _make_features(n_bars=120)
    events = _make_events_from_features(features, n_events=50, start_bar=10)
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "wide_tp",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_wide",
                "horizon": "15m",
                "horizon_bars": 3,
                "take_profit_bps": 500.0,
            },
            {
                "candidate_id": "tight_tp",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_tight",
                "horizon": "15m",
                "horizon_bars": 3,
                "take_profit_bps": 50.0,
            },
        ]
    )

    out = svc._split_and_score_candidates(
        candidates,
        events,
        horizon_bars=3,
        split_scheme_id="WF_60_20_20",
        purge_bars=0,
        embargo_bars=0,
        bar_duration_minutes=5,
        features_df=features,
        entry_lag_bars=1,
        shift_labels_k=0,
        cost_estimate=svc.ResolvedCandidateCostEstimate(
            cost_bps=0.0,
            fee_bps_per_side=0.0,
            slippage_bps_per_fill=0.0,
            cost_model_source="static",
            cost_input_coverage=1.0,
            cost_model_valid=True,
            regime_multiplier=1.0,
        ),
    )

    wide = float(out.loc[out["candidate_id"] == "wide_tp", "estimate_bps"].iloc[0])
    tight = float(out.loc[out["candidate_id"] == "tight_tp", "estimate_bps"].iloc[0])
    assert wide != tight


def test_split_and_score_candidates_expectancy_uses_train_rows_only(monkeypatch):
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "cand_train_only",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_train_only",
                "horizon": "5m",
                "horizon_bars": 1,
            },
        ]
    )
    events = pd.DataFrame(
        {
            "enter_ts": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 3,
            "event_type": ["VOL_SHOCK"] * 3,
            "split_label": ["train", "validation", "test"],
            "split_plan_id": ["TVT_60_20_20"] * 3,
        }
    )
    return_frame = pd.DataFrame(
        {
            "forward_return": [0.01, 0.50, -0.25],
            "cluster_day": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "split_label": ["train", "validation", "test"],
        }
    )

    monkeypatch.setattr(
        svc, "build_event_return_frame", lambda *args, **kwargs: return_frame.copy()
    )
    monkeypatch.setattr(
        svc,
        "estimate_effect_from_frame",
        lambda frame, **kwargs: SimpleNamespace(
            estimate=float(frame["forward_return"].mean()) if not frame.empty else 0.0,
            stderr=0.0,
            ci_low=0.0,
            ci_high=0.0,
            p_value_raw=1.0,
            n_obs=len(frame),
            n_clusters=len(frame),
            method="mock",
            cluster_col="cluster_day",
        ),
    )

    out = svc._split_and_score_candidates(
        candidates,
        events,
        horizon_bars=1,
        split_scheme_id="WF_60_20_20",
        purge_bars=0,
        embargo_bars=0,
        bar_duration_minutes=5,
        features_df=pd.DataFrame(),
        entry_lag_bars=1,
        shift_labels_k=0,
        cost_estimate=None,
    )

    row = out.iloc[0]
    assert abs(float(row["expectancy"]) - 0.01) < 1e-12
    assert int(row["train_n_obs"]) == 1
    assert int(row["validation_n_obs"]) == 1
    assert int(row["test_n_obs"]) == 1
    assert int(row["sample_size"]) == 2
    assert int(row["n_obs"]) == 2


def test_split_and_score_candidates_emits_confirmatory_evidence(monkeypatch):
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "cand_confirmatory",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "direction": 1.0,
                "rule_template": "continuation",
                "family_id": "fam_confirmatory",
                "horizon": "5m",
                "horizon_bars": 1,
            },
        ]
    )
    timestamps = pd.date_range("2024-01-01", periods=16, freq="5min", tz="UTC")
    events = pd.DataFrame(
        {
            "enter_ts": timestamps,
            "timestamp": timestamps,
            "symbol": ["BTCUSDT"] * len(timestamps),
            "event_type": ["VOL_SHOCK"] * len(timestamps),
            "split_label": ["train"] * 8 + ["validation"] * 4 + ["test"] * 4,
            "split_plan_id": ["TVT_60_20_20"] * len(timestamps),
            "vol_regime": ["low" if idx % 2 == 0 else "high" for idx in range(len(timestamps))],
        }
    )
    features = pd.DataFrame(
        {"timestamp": pd.date_range("2024-01-01", periods=32, freq="5min", tz="UTC")}
    )

    def _fake_return_frame(sym_events, _features, *, direction_override=None, entry_lag_bars=1, **kwargs):
        if sym_events.empty:
            return pd.DataFrame(
                columns=[
                    "forward_return",
                    "forward_return_raw",
                    "cost_return",
                    "cluster_day",
                    "split_label",
                    "event_ts",
                    "vol_regime",
                ]
            )
        sign = float(direction_override) if pd.notna(direction_override) else 1.0
        lag_penalty = 0.003 if int(entry_lag_bars) > 1 else 0.0
        base = np.linspace(0.01, 0.06, len(sym_events)) * sign - lag_penalty
        ts = pd.to_datetime(sym_events["timestamp"], utc=True)
        return pd.DataFrame(
            {
                "forward_return": base,
                "forward_return_raw": base + 0.001,
                "cost_return": [0.0005] * len(sym_events),
                "cluster_day": ts.dt.strftime("%Y-%m-%d"),
                "split_label": sym_events["split_label"].tolist(),
                "event_ts": ts,
                "vol_regime": sym_events.get("vol_regime", pd.Series(["unknown"] * len(sym_events))),
            }
        )

    monkeypatch.setattr(svc, "build_event_return_frame", _fake_return_frame)
    monkeypatch.setattr(
        svc,
        "estimate_effect_from_frame",
        lambda frame, **kwargs: SimpleNamespace(
            estimate=float(frame["forward_return"].mean()) if not frame.empty else 0.0,
            stderr=0.01,
            ci_low=0.0,
            ci_high=0.0,
            p_value_raw=0.05,
            n_obs=len(frame),
            n_clusters=len(frame),
            method="mock",
            cluster_col="cluster_day",
        ),
    )

    out = svc._split_and_score_candidates(
        candidates,
        events,
        horizon_bars=1,
        split_scheme_id="WF_60_20_20",
        purge_bars=0,
        embargo_bars=0,
        bar_duration_minutes=5,
        features_df=features,
        entry_lag_bars=1,
        shift_labels_k=0,
        cost_estimate=None,
    )

    row = out.iloc[0]
    returns_oos = json.loads(row["returns_oos_combined"])
    pnl_series = json.loads(row["pnl_series"])
    timestamps = json.loads(row["timestamps"])
    fold_scores = json.loads(row["fold_scores"])
    regime_counts = json.loads(row["regime_counts"])

    assert len(returns_oos) == 4
    assert len(pnl_series) == 4
    assert len(timestamps) == 4
    assert len(fold_scores) == 3
    assert regime_counts == {"high": 2, "low": 2}
    assert 0.0 <= float(row["control_pass_rate"]) <= 1.0
    assert isinstance(bool(row["gate_delay_robustness"]), bool)
    assert isinstance(bool(row["gate_regime_stability"]), bool)


def test_split_and_score_candidates_limits_placebo_checks_to_evaluation_rows(monkeypatch):
    timestamps = pd.date_range("2024-01-01", periods=16, freq="5min", tz="UTC")
    return_frame = pd.DataFrame(
        {
            "forward_return": [0.20] * 8 + [0.05] * 8,
            "forward_return_raw": [0.20] * 8 + [0.05] * 8,
            "cost_return": [0.0005] * 16,
            "cluster_day": timestamps.strftime("%Y-%m-%d"),
            "split_label": ["train"] * 8 + ["validation"] * 4 + ["test"] * 4,
            "event_ts": timestamps,
            "vol_regime": ["low" if idx % 2 == 0 else "high" for idx in range(len(timestamps))],
        }
    )
    delayed_frame = return_frame.copy()
    shift_placebo_frame = pd.DataFrame(
        {
            "forward_return": [0.20] * 8 + [0.001] * 8,
            "forward_return_raw": [0.20] * 8 + [0.001] * 8,
            "cost_return": [0.0005] * 16,
            "cluster_day": timestamps.strftime("%Y-%m-%d"),
            "split_label": ["train"] * 8 + ["validation"] * 4 + ["test"] * 4,
            "event_ts": timestamps,
            "vol_regime": ["low" if idx % 2 == 0 else "high" for idx in range(len(timestamps))],
        }
    )

    evidence = svc.candidate_scoring._build_confirmatory_evidence(
        return_frame=return_frame,
        delayed_frame=delayed_frame,
        shift_placebo_frame=shift_placebo_frame,
        random_placebo_frame=shift_placebo_frame,
        direction_placebo_frame=shift_placebo_frame,
    )

    assert bool(evidence["pass_shift_placebo"]) is True
    assert bool(evidence["pass_random_entry_placebo"]) is True
    assert bool(evidence["pass_direction_reversal_placebo"]) is True
    assert float(evidence["control_pass_rate"]) == 0.0


def test_standard_sample_quality_floors_are_defensible():
    """Regression: standard sample quality floors must be >= 10 for credible split evidence.
    Floors of 2 are not statistically defensible.
    """
    from project.research.services.candidate_discovery_service import DEFAULT_SAMPLE_QUALITY_POLICY

    standard = DEFAULT_SAMPLE_QUALITY_POLICY["standard"]
    assert standard["min_validation_n_obs"] >= 10, (
        f"min_validation_n_obs must be >= 10; got {standard['min_validation_n_obs']}. "
        "Two events in a holdout split provide near-zero statistical power."
    )
    assert standard["min_test_n_obs"] >= 10, (
        f"min_test_n_obs must be >= 10; got {standard['min_test_n_obs']}."
    )
    assert standard["min_total_n_obs"] >= 30, (
        f"min_total_n_obs must be >= 30; got {standard['min_total_n_obs']}."
    )
