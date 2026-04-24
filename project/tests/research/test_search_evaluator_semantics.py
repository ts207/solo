from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from pathlib import Path

from project.core.column_registry import ColumnRegistry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.search.search_feature_utils import prepare_search_features_for_symbol


def _base_features(periods: int = 40) -> pd.DataFrame:
    timestamps = pd.date_range("2024-01-01", periods=periods, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["BTCUSDT"] * len(timestamps),
            "close": 100.0 + np.arange(len(timestamps), dtype=float),
            "split_label": ["test"] * len(timestamps),
            "rv_pct_17280": [0.1] * len(timestamps),
        }
    )


def _patch_robustness(monkeypatch) -> None:
    monkeypatch.setattr(
        "project.research.search.evaluator.evaluate_by_regime",
        lambda *args, **kwargs: pd.DataFrame(
            [{"regime": "baseline", "n": 4, "mean_return_bps": 1.0, "t_stat": 1.0, "hit_rate": 1.0, "valid": True}]
        ),
    )
    monkeypatch.setattr(
        "project.research.search.evaluator.evaluate_stress_scenarios",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        "project.research.search.evaluator.detect_kill_switches",
        lambda *args, **kwargs: pd.DataFrame(),
    )


def test_prepare_search_features_for_symbol_merges_event_direction_metadata(monkeypatch, tmp_path: Path):
    base = _base_features()

    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_flags",
        lambda **kwargs: pd.DataFrame(
            {
                "timestamp": [base.loc[0, "timestamp"]],
                "symbol": ["BTCUSDT"],
                EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column: [True],
            }
        ),
    )
    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_events",
        lambda **kwargs: pd.DataFrame(
            {
                "timestamp": [base.loc[0, "timestamp"]],
                "symbol": ["BTCUSDT"],
                "event_type": ["VOL_SHOCK"],
                "sign": [1],
                "direction": ["up"],
            }
        ),
    )

    features = prepare_search_features_for_symbol(
        run_id="dummy",
        symbol="BTCUSDT",
        timeframe="5m",
        data_root=tmp_path,
        load_features_fn=lambda **kwargs: base.copy(),
    )

    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]
    assert direction_col in features.columns
    assert features.loc[0, direction_col] == 1.0


def test_prepare_search_features_for_symbol_coalesces_duplicate_event_direction_columns(monkeypatch, tmp_path: Path):
    base = _base_features()
    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]

    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_flags",
        lambda **kwargs: pd.DataFrame(
            {
                "timestamp": [base.loc[0, "timestamp"]],
                "symbol": ["BTCUSDT"],
                EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column: [True],
                direction_col: [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_events",
        lambda **kwargs: pd.DataFrame(
            {
                "timestamp": [base.loc[0, "timestamp"]],
                "symbol": ["BTCUSDT"],
                "event_type": ["VOL_SHOCK"],
                "sign": [1],
                "direction": ["up"],
            }
        ),
    )

    features = prepare_search_features_for_symbol(
        run_id="dummy",
        symbol="BTCUSDT",
        timeframe="5m",
        data_root=tmp_path,
        load_features_fn=lambda **kwargs: base.copy(),
    )

    assert direction_col in features.columns
    assert f"{direction_col}_x" not in features.columns
    assert f"{direction_col}_y" not in features.columns
    assert f"{direction_col}__dir" not in features.columns
    assert features.loc[0, direction_col] == 1.0


def test_prepare_search_features_for_symbol_preserves_direction_sign_from_flags(monkeypatch, tmp_path: Path):
    base = _base_features()
    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]

    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_flags",
        lambda **kwargs: pd.DataFrame(
            {
                "timestamp": [base.loc[0, "timestamp"], base.loc[1, "timestamp"]],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column: [True, True],
                direction_col: [1.0, -1.0],
            }
        ),
    )
    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_events",
        lambda **kwargs: pd.DataFrame(columns=["timestamp", "symbol", "event_type", "sign", "direction"]),
    )

    features = prepare_search_features_for_symbol(
        run_id="dummy",
        symbol="BTCUSDT",
        timeframe="5m",
        data_root=tmp_path,
        load_features_fn=lambda **kwargs: base.copy(),
    )

    assert features.loc[0, direction_col] == 1.0
    assert features.loc[1, direction_col] == -1.0


def test_prepare_search_features_for_symbol_materializes_expected_zero_hit_event_columns(
    monkeypatch,
    tmp_path: Path,
):
    base = _base_features()

    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_flags",
        lambda **kwargs: pd.DataFrame(columns=["timestamp", "symbol"]),
    )
    monkeypatch.setattr(
        "project.research.search.search_feature_utils.load_registry_events",
        lambda **kwargs: pd.DataFrame(columns=["timestamp", "symbol", "event_type"]),
    )

    features = prepare_search_features_for_symbol(
        run_id="dummy",
        symbol="BTCUSDT",
        timeframe="5m",
        data_root=tmp_path,
        expected_event_ids=["VOL_SHOCK"],
        load_features_fn=lambda **kwargs: base.copy(),
    )

    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    assert signal_col in features.columns
    assert bool(features[signal_col].any()) is False

    _patch_robustness(monkeypatch)
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is False
    assert metrics.loc[0, "invalid_reason"] == "no_trigger_hits"


def test_event_templates_use_spec_side_policy_in_canonical_evaluator(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5, 10, 15], signal_col] = True
    features[ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = np.nan
    features.loc[[0, 5, 10, 15], ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = 1.0

    continuation = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )
    mean_reversion = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="mean_reversion",
    )

    metrics = evaluate_hypothesis_batch(
        [continuation, mean_reversion],
        features,
        min_sample_size=1,
    ).set_index("template_id")

    assert bool(metrics.loc["continuation", "valid"]) is True
    assert bool(metrics.loc["mean_reversion", "valid"]) is True
    assert float(metrics.loc["continuation", "mean_return_bps"]) > 0.0
    assert float(metrics.loc["mean_reversion", "mean_return_bps"]) < 0.0
    assert float(metrics.loc["continuation", "cost_adjusted_return_bps"]) < float(
        metrics.loc["continuation", "mean_return_bps"]
    )
    assert 0.0 <= float(metrics.loc["continuation", "p_value"]) <= 1.0


def test_event_trigger_event_direction_filters_signal_rows(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features(periods=120)
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]
    features[signal_col] = False
    features.loc[[0, 10, 20, 30], signal_col] = True
    features[direction_col] = np.nan
    features.loc[[0, 20], direction_col] = 1.0
    features.loc[[10, 30], direction_col] = -1.0

    directed = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK", event_direction="up"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    metrics = evaluate_hypothesis_batch([directed], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is True
    assert int(metrics.loc[0, "n"]) == 2


def test_evaluated_hypotheses_materialize_entry_lag_metadata(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features(periods=120)
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5, 10, 15], signal_col] = True
    features[ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = np.nan
    features.loc[[0, 5, 10, 15], ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = 1.0

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="short",
        horizon="24b",
        template_id="continuation",
        entry_lag=2,
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert int(metrics.loc[0, "entry_lag"]) == 2
    assert int(metrics.loc[0, "entry_lag_bars"]) == 2
    assert bool(metrics.loc[0, "valid"]) is True


def test_event_templates_accept_arbitrary_bar_count_horizons(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features(periods=120)
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5, 10, 15], signal_col] = True
    features[ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = np.nan
    features.loc[[0, 5, 10, 15], ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]] = 1.0

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="72b",
        template_id="continuation",
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is True
    assert float(metrics.loc[0, "mean_return_bps"]) > 0.0


def test_gate_templates_fail_closed_in_canonical_evaluator(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5], signal_col] = True

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="only_if_regime",
    )

    monkeypatch.setattr(
        "project.research.search.evaluator_utils.operator_semantics",
        lambda _: {"side_policy": "both", "label_target": "gate", "requires_direction": False},
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is False
    assert metrics.loc[0, "invalid_reason"] == "gate_template_unsupported"


def test_non_forward_label_targets_fail_closed_in_canonical_evaluator(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5], signal_col] = True

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    monkeypatch.setattr(
        "project.research.search.evaluator_utils.operator_semantics",
        lambda _: {"side_policy": "both", "label_target": "mfe_h", "requires_direction": True},
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is False
    assert metrics.loc[0, "invalid_reason"] == "unsupported_label_target"


def test_non_default_profiles_fail_closed_in_canonical_evaluator(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5], signal_col] = True

    cost_spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
        cost_profile="premium",
    )
    objective_spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
        objective_profile="sharpe",
    )

    metrics = evaluate_hypothesis_batch([cost_spec, objective_spec], features, min_sample_size=1)

    assert list(metrics["invalid_reason"]) == [
        "unsupported_cost_profile",
        "unsupported_objective_profile",
    ]


def test_evaluate_hypothesis_batch_drops_boundary_crossing_event_windows(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[2, signal_col] = True
    features["split_label"] = ["train"] * 3 + ["validation"] * 2 + ["test"] * (len(features) - 5)

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="4b",
        template_id="continuation",
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is False
    assert metrics.loc[0, "invalid_reason"] == "no_split_compatible_events"


def test_evaluate_hypothesis_batch_does_not_reject_cross_family_templates(monkeypatch, caplog):
    _patch_robustness(monkeypatch)
    features = _base_features()
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    features[signal_col] = False
    features.loc[[0, 5], signal_col] = True

    incompatible = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="basis_repair",
    )
    valid = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    with caplog.at_level(logging.INFO, logger="project.research.search.evaluator"):
        evaluate_hypothesis_batch([incompatible, valid], features, min_sample_size=1)

    assert "1 valid, 1 invalid (unsupported_label_target=1)" in caplog.text
    assert "incompatible_template_family" not in caplog.text


def test_feature_condition_filters_trigger_rows_before_entry_lag(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features(periods=120)
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]
    features[signal_col] = False
    features.loc[[0, 5, 10, 15], signal_col] = True
    features[direction_col] = np.nan
    features.loc[[0, 10], direction_col] = 1.0
    features.loc[[5, 15], direction_col] = -1.0

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
        entry_lag=1,
        feature_condition=TriggerSpec.feature_predicate(direction_col, ">", 0.0),
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert bool(metrics.loc[0, "valid"]) is True
    assert int(metrics.loc[0, "n"]) == 2
    assert float(metrics.loc[0, "mean_return_bps"]) > 0.0


def test_evaluate_hypothesis_batch_emits_candidate_event_timestamps_sidecar(monkeypatch):
    _patch_robustness(monkeypatch)
    features = _base_features(periods=120)
    signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
    direction_col = ColumnRegistry.event_direction_cols("VOL_SHOCK")[0]
    features[signal_col] = False
    features.loc[[0, 10, 20, 30], signal_col] = True
    features[direction_col] = np.nan
    features.loc[[0, 10, 20, 30], direction_col] = 1.0

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    metrics = evaluate_hypothesis_batch([spec], features, min_sample_size=1)

    assert "candidate_event_timestamps" in metrics.attrs
    event_timestamps = metrics.attrs["candidate_event_timestamps"]
    assert not event_timestamps.empty
    assert {
        "hypothesis_id",
        "trigger_key",
        "event_timestamp",
        "split_label",
    }.issubset(event_timestamps.columns)
    assert event_timestamps["hypothesis_id"].nunique() == 1
    assert event_timestamps["hypothesis_id"].iat[0] == metrics.loc[0, "hypothesis_id"]
    assert str(event_timestamps["event_timestamp"].dtype) == "datetime64[ns, UTC]"
    assert set(event_timestamps["split_label"]) == {"test"}
