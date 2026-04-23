from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import yaml

import project.research.phase2_search_engine as search_engine
import project.research.search.hierarchical_search as hierarchical_search
from project.core.column_registry import ColumnRegistry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.knowledge.schemas import canonical_json, region_key, stable_hash
from project.research.phase2_search_engine import (
    _attach_candidate_run_lineage,
    _build_gate_funnel,
    _expected_event_ids_from_search_spec_doc,
    _classify_metrics_counts,
    _expected_event_ids_from_hypotheses,
    _filter_previously_tested_hypotheses,
    _latest_hierarchical_stage_frame,
    _materialize_interaction_trigger_columns,
    _materialize_sequence_trigger_columns,
    _normalize_phase2_candidate_artifact,
    _resolve_search_min_t_stat,
    _write_event_scoped_search_spec,
)


def test_write_event_scoped_search_spec_narrows_default_broad_spec(tmp_path: Path) -> None:
    out_dir = tmp_path / "search_engine"

    resolved = _write_event_scoped_search_spec(
        search_spec="spec/search_space.yaml",
        phase2_event_type="VOL_SHOCK",
        out_dir=out_dir,
    )

    resolved_path = Path(resolved)
    assert resolved_path.exists()
    payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8"))
    assert payload["events"] == ["VOL_SHOCK"]
    assert payload["triggers"]["events"] == ["VOL_SHOCK"]
    assert payload["expression_templates"] == [
        "mean_reversion",
        "continuation",
        "trend_continuation",
    ]
    assert payload["horizons"] == ["60m"]
    assert payload["include_sequences"] is False
    assert payload["include_interactions"] is False
    assert "states" not in payload
    assert "transitions" not in payload
    assert payload["metadata"]["auto_scope"] == "event:VOL_SHOCK"


def test_write_event_scoped_search_spec_preserves_explicit_nondefault_spec(tmp_path: Path) -> None:
    out_dir = tmp_path / "search_engine"

    resolved = _write_event_scoped_search_spec(
        search_spec="spec/search/search_benchmark_vol_shock.yaml",
        phase2_event_type="VOL_SHOCK",
        out_dir=out_dir,
    )

    assert resolved == "spec/search/search_benchmark_vol_shock.yaml"
    assert not out_dir.exists()


def test_normalize_phase2_candidate_artifact_preserves_empty_contract_columns() -> None:
    out = _normalize_phase2_candidate_artifact(pd.DataFrame({"run_id": pd.Series(dtype="object")}))

    assert out.empty
    assert {"candidate_id", "hypothesis_id", "event_type", "symbol", "run_id"}.issubset(
        set(out.columns)
    )


def test_classify_metrics_counts_separates_min_sample_rejections() -> None:
    metrics = pd.DataFrame(
        [
            {"valid": False, "invalid_reason": "min_sample_size", "n": 1, "t_stat": 0.0},
            {"valid": False, "invalid_reason": "direction_resolution_failed", "n": 40, "t_stat": 0.0},
            {"valid": True, "invalid_reason": "", "n": 40, "t_stat": 1.0},
            {"valid": True, "invalid_reason": "", "n": 40, "t_stat": 2.0},
        ]
    )

    valid_metrics_rows, rejected_invalid_metrics, rejected_by_min_n = _classify_metrics_counts(
        metrics,
        min_n=30,
        min_t_stat=1.5,
    )

    assert valid_metrics_rows == 2
    assert rejected_by_min_n == 1
    assert rejected_invalid_metrics == 1


def test_resolve_search_min_t_stat_uses_phase2_gate_default_when_cli_omitted() -> None:
    assert _resolve_search_min_t_stat(explicit_min_t_stat=None, phase2_gates={"min_t_stat": 1.75}) == 1.75


def test_resolve_search_min_t_stat_prefers_explicit_cli_override() -> None:
    assert _resolve_search_min_t_stat(explicit_min_t_stat=2.25, phase2_gates={"min_t_stat": 1.75}) == 2.25


def test_build_gate_funnel_tracks_cumulative_survivors() -> None:
    metrics = pd.DataFrame(
        [
            {"valid": True, "n": 80},
            {"valid": True, "n": 40},
            {"valid": False, "n": 90},
        ]
    )
    candidate_universe = pd.DataFrame([{"candidate_id": "u1"}, {"candidate_id": "u2"}])
    written_candidates = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_bridge_tradable": True,
            },
            {
                "candidate_id": "c2",
                "gate_oos_validation": True,
                "gate_after_cost_positive": False,
                "gate_after_cost_stressed_positive": False,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_bridge_tradable": False,
            },
        ]
    )

    observed = _build_gate_funnel(
        hypotheses_generated=5,
        feasible_hypotheses=3,
        metrics=metrics,
        candidate_universe=candidate_universe,
        written_candidates=written_candidates,
        min_n=50,
    )

    assert observed == {
        "generated": 5,
        "feasible": 3,
        "metrics_emitted": 3,
        "valid_metrics": 2,
        "pass_min_sample_size": 1,
        "bridge_candidate_universe": 2,
        "phase2_candidates_written": 2,
        "pass_oos_validation": 2,
        "pass_after_cost_positive": 1,
        "pass_after_cost_stressed_positive": 1,
        "pass_multiplicity": 1,
        "pass_regime_stable": 1,
        "phase2_final": 1,
    }


def test_latest_hierarchical_stage_frame_prefers_latest_non_empty_stage() -> None:
    trigger = pd.DataFrame([{"hypothesis_id": "h_a"}])
    execution = pd.DataFrame([{"hypothesis_id": "h_c"}])

    observed = _latest_hierarchical_stage_frame(
        {
            "trigger_viability": trigger,
            "template_refinement": pd.DataFrame(),
            "execution_refinement": execution,
            "context_refinement": pd.DataFrame(),
        }
    )

    assert len(observed) == 1
    assert observed.iloc[0]["hypothesis_id"] == "h_c"


def test_hierarchical_diagnostics_keep_stage_counts_when_all_candidates_fail_gate(
    tmp_path: Path, monkeypatch
) -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=8, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 8,
            "close": [100.0, 100.2, 100.4, 100.1, 99.9, 100.0, 100.1, 100.2],
        }
    )
    stage_frame = pd.DataFrame(
        {
            "candidate_id": [f"cand_{idx}" for idx in range(6)],
            "gate_search_min_t_stat": [False] * 6,
            "t_stat": [-0.28, 0.29, -0.14, 0.18, -0.11, 0.09],
        }
    )
    hypothesis = HypothesisSpec(
        trigger=TriggerSpec.event("PULLBACK_PIVOT"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )

    monkeypatch.setattr(search_engine, "load_features", lambda **_: features.copy())
    monkeypatch.setattr(
        search_engine,
        "generate_hypotheses_with_audit",
        lambda *args, **kwargs: (
            [hypothesis],
            {
                "counts": {"generated": 1, "feasible": 1, "rejected": 0},
                "rejection_reason_counts": {},
            },
        ),
    )
    monkeypatch.setattr(search_engine, "_build_required_walkforward_folds", lambda _features: [])
    monkeypatch.setattr(
        search_engine,
        "_load_search_spec_doc",
        lambda _path: {"triggers": {"events": ["PULLBACK_PIVOT"]}},
    )
    monkeypatch.setattr(search_engine, "_load_hierarchical_config", lambda _doc: {"enabled": True})
    monkeypatch.setattr(
        "project.spec_validation.expand_triggers",
        lambda doc: {"events": list((doc.get("triggers") or {}).get("events") or [])},
    )
    monkeypatch.setattr(
        hierarchical_search,
        "run_hierarchical_search",
        lambda **_: SimpleNamespace(
            final_candidates=stage_frame.copy(),
            stage_artifacts={
                "trigger_viability": pd.DataFrame(),
                "template_refinement": pd.DataFrame(),
                "execution_refinement": pd.DataFrame(),
                "context_refinement": stage_frame.copy(),
            },
            candidates_evaluated_total=len(stage_frame),
        ),
    )

    search_spec_path = tmp_path / "search_spec.yaml"
    search_spec_path.write_text("triggers:\n  events:\n    - PULLBACK_PIVOT\n", encoding="utf-8")

    rc = search_engine.run(
        run_id="hierarchical_gate_rejects",
        symbols="BTCUSDT",
        data_root=tmp_path,
        out_dir=tmp_path / "out",
        timeframe="5m",
        search_spec=str(search_spec_path),
        min_t_stat=1.5,
        min_n=30,
    )

    assert rc == 0
    diagnostics = json.loads(
        search_engine.phase2_diagnostics_path(
            run_id="hierarchical_gate_rejects", data_root=tmp_path
        ).read_text(encoding="utf-8")
    )
    assert diagnostics["metrics_rows"] == 6
    assert diagnostics["valid_metrics_rows"] == 6
    assert diagnostics["rejected_by_min_t_stat"] == 6
    assert diagnostics["bridge_candidates_rows"] == 0
    assert diagnostics["symbol_diagnostics"][0]["metrics_rows"] == 6
    assert diagnostics["symbol_diagnostics"][0]["rejected_by_min_t_stat"] == 6
    assert diagnostics["symbol_diagnostics"][0]["gate_funnel"]["bridge_candidate_universe"] == 6
    assert diagnostics["symbol_diagnostics"][0]["gate_funnel"]["phase2_candidates_written"] == 0


def test_flat_search_materializes_generated_event_columns_without_reloading_features(
    tmp_path: Path, monkeypatch
) -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=80, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 80,
            "close": [100.0 + idx * 0.01 for idx in range(80)],
        }
    )
    hypothesis = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="continuation",
    )
    load_calls = {"count": 0}

    def _load_features(**_kwargs):
        load_calls["count"] += 1
        return features.copy()

    def _run_distributed_search(_hypotheses, loaded_features, **_kwargs):
        signal_col = EVENT_REGISTRY_SPECS["VOL_SHOCK"].signal_column
        assert signal_col in loaded_features.columns
        return pd.DataFrame()

    monkeypatch.setattr(search_engine, "load_features", _load_features)
    monkeypatch.setattr(
        search_engine,
        "generate_hypotheses_with_audit",
        lambda *args, **kwargs: (
            [hypothesis],
            {
                "counts": {"generated": 1, "feasible": 1, "rejected": 0},
                "rejection_reason_counts": {},
            },
        ),
    )
    monkeypatch.setattr(search_engine, "_build_required_walkforward_folds", lambda _features: [])
    monkeypatch.setattr(search_engine, "_load_hierarchical_config", lambda _doc: None)
    monkeypatch.setattr(search_engine, "run_distributed_search", _run_distributed_search)
    monkeypatch.setattr(search_engine, "_load_search_spec_doc", lambda _path: {"triggers": {}})
    monkeypatch.setattr("project.spec_validation.expand_triggers", lambda doc: {"events": []})

    rc = search_engine.run(
        run_id="single_load_flat_search",
        symbols="BTCUSDT",
        data_root=tmp_path,
        out_dir=tmp_path / "out",
        timeframe="5m",
        search_spec=str(tmp_path / "search_spec.yaml"),
        min_t_stat=1.5,
        min_n=1,
    )

    assert rc == 0
    assert load_calls["count"] == 1


def test_materialize_sequence_trigger_columns_builds_expected_mask() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 6,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
            EVENT_REGISTRY_SPECS["FND_DISLOC"].signal_column: [False, True, False, False, True, False],
            EVENT_REGISTRY_SPECS["BREAKOUT_TRIGGER"].signal_column: [False, False, True, False, False, True],
        }
    )
    sequence_spec = HypothesisSpec(
        trigger=TriggerSpec.sequence(
            "SEQ_TEST_CHAIN",
            ["FND_DISLOC", "BREAKOUT_TRIGGER"],
            [2],
        ),
        direction="long",
        horizon="24b",
        template_id="continuation",
    )

    observed = _materialize_sequence_trigger_columns(features, [sequence_spec])

    sequence_col = ColumnRegistry.sequence_cols("SEQ_TEST_CHAIN")[0]
    assert sequence_col in observed.columns
    assert observed[sequence_col].tolist() == [False, False, True, False, False, True]


def test_materialize_sequence_trigger_columns_writes_zero_hit_column_when_components_absent() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
            "symbol": ["ETHUSDT"] * 3,
            "close": [100.0, 100.5, 101.0],
        }
    )
    sequence_spec = HypothesisSpec(
        trigger=TriggerSpec.sequence(
            "SEQ_ZERO_CHAIN",
            ["LIQUIDITY_VACUUM", "VOL_RELAXATION_START"],
            [48],
        ),
        direction="long",
        horizon="12b",
        template_id="mean_reversion",
    )

    observed = _materialize_sequence_trigger_columns(features, [sequence_spec])

    sequence_col = ColumnRegistry.sequence_cols("SEQ_ZERO_CHAIN")[0]
    assert sequence_col in observed.columns
    assert observed[sequence_col].tolist() == [False, False, False]


def test_expected_event_ids_from_hypotheses_includes_sequence_components() -> None:
    hypotheses = [
        HypothesisSpec(
            trigger=TriggerSpec.sequence(
                "SEQ_COMPONENTS",
                ["FND_DISLOC", "BREAKOUT_TRIGGER"],
                [96],
            ),
            direction="long",
            horizon="24b",
            template_id="continuation",
        )
    ]

    assert _expected_event_ids_from_hypotheses(hypotheses) == ["FND_DISLOC", "BREAKOUT_TRIGGER"]


def test_expected_event_ids_from_hypotheses_includes_interaction_event_components() -> None:
    hypotheses = [
        HypothesisSpec(
            trigger=TriggerSpec.interaction(
                "INT_BREAKOUT_EXCLUDE_VOL",
                "BREAKOUT_TRIGGER",
                "VOL_SPIKE",
                "exclude",
                lag=12,
            ),
            direction="long",
            horizon="24b",
            template_id="continuation",
        )
    ]

    assert _expected_event_ids_from_hypotheses(hypotheses) == ["BREAKOUT_TRIGGER", "VOL_SPIKE"]


def test_expected_event_ids_from_search_spec_doc_includes_expanded_event_triggers() -> None:
    payload = {
        "version": 1,
        "kind": "search_spec",
        "search_mode": "subset",
        "triggers": {
            "events": ["vol_spike", "TREND_DECELERATION", "VOL_SPIKE"],
        },
        "expression_templates": ["continuation"],
    }

    assert set(_expected_event_ids_from_search_spec_doc(payload)) == {
        "VOL_SPIKE",
        "TREND_DECELERATION",
    }


def test_materialize_interaction_trigger_columns_builds_confirm_mask() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 6,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
            EVENT_REGISTRY_SPECS["BREAKOUT_TRIGGER"].signal_column: [False, True, False, False, False, False],
            EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column: [False, False, True, False, False, False],
        }
    )
    interaction_spec = HypothesisSpec(
        trigger=TriggerSpec.interaction(
            "INT_CONFIRM_TEST",
            "BREAKOUT_TRIGGER",
            "VOL_SPIKE",
            "confirm",
            lag=10,
        ),
        direction="long",
        horizon="24b",
        template_id="continuation",
    )

    observed = _materialize_interaction_trigger_columns(features, [interaction_spec])

    interaction_col = ColumnRegistry.interaction_cols("INT_CONFIRM_TEST")[0]
    assert interaction_col in observed.columns
    assert observed[interaction_col].tolist() == [False, False, True, False, False, False]


def test_materialize_interaction_trigger_columns_respects_left_event_direction() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=7, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 7,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
            EVENT_REGISTRY_SPECS["BREAKOUT_TRIGGER"].signal_column: [False, True, False, False, True, False, False],
            EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column: [False, False, True, False, False, True, False],
            ColumnRegistry.event_direction_cols("BREAKOUT_TRIGGER")[0]: [0.0, 1.0, 0.0, 0.0, -1.0, 0.0, 0.0],
        }
    )
    interaction_spec = HypothesisSpec(
        trigger=TriggerSpec.interaction(
            "INT_CONFIRM_UP_ONLY",
            "BREAKOUT_TRIGGER",
            "VOL_SPIKE",
            "confirm",
            lag=10,
            left_direction="up",
        ),
        direction="short",
        horizon="24b",
        template_id="continuation",
    )

    observed = _materialize_interaction_trigger_columns(features, [interaction_spec])

    interaction_col = ColumnRegistry.interaction_cols("INT_CONFIRM_UP_ONLY")[0]
    assert interaction_col in observed.columns
    assert observed[interaction_col].tolist() == [False, False, True, False, False, False, False]


def test_materialize_interaction_trigger_columns_handles_missing_event_direction_column() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 4,
            EVENT_REGISTRY_SPECS["FALSE_BREAKOUT"].signal_column: [False, True, False, False],
            "bull_trend_regime": [0.0, 1.0, 1.0, 0.0],
        }
    )
    interaction_spec = HypothesisSpec(
        trigger=TriggerSpec.interaction(
            "INT_FALSEBREAK_BULL",
            "FALSE_BREAKOUT",
            "BULL_TREND_REGIME",
            "and",
            lag=12,
            left_direction="up",
        ),
        direction="short",
        horizon="12b",
        template_id="mean_reversion",
    )

    observed = _materialize_interaction_trigger_columns(features, [interaction_spec])

    interaction_col = ColumnRegistry.interaction_cols("INT_FALSEBREAK_BULL")[0]
    assert interaction_col in observed.columns
    assert observed[interaction_col].tolist() == [False, False, False, False]


def test_materialize_interaction_trigger_columns_builds_exclude_mask() -> None:
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 6,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
            EVENT_REGISTRY_SPECS["BREAKOUT_TRIGGER"].signal_column: [False, True, False, False, True, False],
            EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column: [False, False, True, False, False, False],
        }
    )
    interaction_spec = HypothesisSpec(
        trigger=TriggerSpec.interaction(
            "INT_EXCLUDE_TEST",
            "BREAKOUT_TRIGGER",
            "VOL_SPIKE",
            "exclude",
            lag=10,
        ),
        direction="long",
        horizon="24b",
        template_id="continuation",
    )

    observed = _materialize_interaction_trigger_columns(features, [interaction_spec])

    interaction_col = ColumnRegistry.interaction_cols("INT_EXCLUDE_TEST")[0]
    assert interaction_col in observed.columns
    assert observed[interaction_col].tolist() == [False, False, False, False, True, False]


def test_attach_candidate_run_lineage_sets_missing_run_id() -> None:
    frame = pd.DataFrame(
        [
            {"candidate_id": "cand_1", "hypothesis_id": "hyp_1"},
            {"candidate_id": "cand_2", "hypothesis_id": "hyp_2", "run_id": ""},
        ]
    )

    observed = _attach_candidate_run_lineage(frame, run_id="run_test")

    assert observed["run_id"].tolist() == ["run_test", "run_test"]


def test_filter_previously_tested_hypotheses_skips_only_matching_symbol_region() -> None:
    hypothesis = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12",
        template_id="continuation",
        context={"vol_regime": "high"},
        entry_lag=1,
    )
    excluded_region_key = region_key(
        {
            "program_id": "prog_demo",
            "symbol_scope": "ETHUSDT",
            "event_type": "VOL_SHOCK",
            "trigger_type": "EVENT",
            "template_id": "continuation",
            "direction": "long",
            "horizon": "12",
            "entry_lag": 1,
            "context_hash": stable_hash((canonical_json({"vol_regime": "high"}),)),
        }
    )

    eth_filtered, eth_skipped = _filter_previously_tested_hypotheses(
        [hypothesis],
        program_id="prog_demo",
        symbol="ETHUSDT",
        avoid_region_keys={excluded_region_key},
    )
    btc_filtered, btc_skipped = _filter_previously_tested_hypotheses(
        [hypothesis],
        program_id="prog_demo",
        symbol="BTCUSDT",
        avoid_region_keys={excluded_region_key},
    )

    assert eth_filtered == []
    assert eth_skipped == 1
    assert btc_filtered == [hypothesis]
    assert btc_skipped == 0


def test_main_writes_real_manifest(tmp_path: Path, monkeypatch) -> None:
    run_id = "phase2_manifest_run"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    def _fake_run(**kwargs) -> int:
        out_dir = kwargs["out_dir"]
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "edge_score": 1.0}]
        ).to_parquet(
            search_engine.phase2_candidates_path(run_id=run_id, data_root=tmp_path), index=False
        )
        (out_dir / "regime_conditional_candidates.parquet").parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"candidate_id": "adj_1"}]).to_parquet(
            out_dir / "regime_conditional_candidates.parquet",
            index=False,
        )
        search_engine.phase2_diagnostics_path(run_id=run_id, data_root=tmp_path).write_text(
            json.dumps(
                {
                    "hypotheses_generated": 4,
                    "valid_metrics_rows": 4,
                    "rejected_hypotheses": 3,
                    "final_candidate_count": 1,
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(search_engine, "run", _fake_run)

    rc = search_engine.main(["--run_id", run_id, "--symbols", "BTCUSDT", "--data_root", str(tmp_path)])

    assert rc == 0
    manifest = json.loads((tmp_path / "runs" / run_id / "phase2_search_engine.json").read_text())
    assert manifest["status"] == "success"
    assert manifest["stats"]["final_candidate_count"] == 1
    assert manifest["stats"]["candidate_rows"] == 1
    assert manifest["stats"]["regime_conditional_candidate_rows"] == 1
