"""Phase 4.2 / 4.3 / 4.4 — Alpha Depth complete test suite.

4.2  Per-regime evaluation as discovery signal
     - _write_regime_conditional_candidates: output schema, filtering, sorting
     - _load_regime_conditional_candidates: path resolution, error handling
     - _build_next_actions: regime candidates injected into explore_adjacent

4.3  Context complexity penalty in stability_score
     - _count_context_dimensions: dict, JSON string, None inputs
     - _context_complexity_penalty: formula verification
     - stability_score: penalty applied / bypassed via flag

4.4  AllocationSpec research-to-live handoff
     - _resolve_sizing_inputs: mean_return_bps and stressed_after_cost paths
     - SizingPolicySpec: new fields present and nullable
     - AllocationSpec.from_blueprint: new kwargs flow through
     - _build_allocation_spec: audit_row → sizing inputs populated
     - _check_marginal_contribution: similarity threshold gating
"""
from __future__ import annotations

import json
import math
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from project.core.exceptions import DataIntegrityError
from project.portfolio.allocation_spec import AllocationSpec, SizingPolicySpec

# ---------------------------------------------------------------------------
# Phase 4.4 imports
# ---------------------------------------------------------------------------
from project.research.compile_strategy_blueprints import (
    _build_allocation_spec,
    _check_marginal_contribution,
    _resolve_sizing_inputs,
)

# ---------------------------------------------------------------------------
# Phase 4.2 imports
# ---------------------------------------------------------------------------
from project.research.phase2_search_engine import (
    _REGIME_CANDIDATE_COLUMNS,
    _write_regime_conditional_candidates,
)

# ---------------------------------------------------------------------------
# Phase 4.3 imports
# ---------------------------------------------------------------------------
from project.research.promotion.promotion_scoring import (
    _context_complexity_penalty,
    _count_context_dimensions,
    stability_score,
)
from project.research.update_campaign_memory import (
    _build_belief_state,
    _build_next_actions,
    _load_regime_conditional_candidates,
)

# ===========================================================================
# Phase 4.2 — _write_regime_conditional_candidates
# ===========================================================================

class TestWriteRegimeConditionalCandidates:

    def _make_final_df(self, n: int = 20) -> pd.DataFrame:
        rng = np.random.default_rng(42)
        rows = []
        for i in range(n):
            t = rng.uniform(-3, 3)
            rows.append({
                "hypothesis_id": f"hyp_{i:03d}",
                "trigger_key": f"event:EVT_{i % 5}",
                "template_id": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "t_stat": t,
                "mean_return_bps": t * 2.0,
                "robustness_score": rng.uniform(0, 1),
                "context_json": "{}",
            })
        return pd.DataFrame(rows)

    def test_output_file_created(self, tmp_path):
        df = self._make_final_df()
        _write_regime_conditional_candidates(df, tmp_path)
        assert (tmp_path / "regime_conditional_candidates.parquet").exists()

    def test_schema_matches_expected_columns(self, tmp_path):
        df = self._make_final_df()
        _write_regime_conditional_candidates(df, tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        for col in _REGIME_CANDIDATE_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_filters_to_weak_positive_only(self, tmp_path):
        """Only hypotheses with 0.5 <= t_stat < 1.5 and positive return survive."""
        df = pd.DataFrame([
            {"hypothesis_id": "strong", "trigger_key": "event:EVT_A",
             "t_stat": 2.5, "mean_return_bps": 10.0,   # t_stat too high → excluded
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.8, "context_json": "{}"},
            {"hypothesis_id": "weak_pos", "trigger_key": "event:EVT_B",
             "t_stat": 1.1, "mean_return_bps": 5.0,    # qualifies
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.5, "context_json": "{}"},
            {"hypothesis_id": "negative", "trigger_key": "event:EVT_C",
             "t_stat": 1.0, "mean_return_bps": -3.0,   # negative return → excluded
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.3, "context_json": "{}"},
            {"hypothesis_id": "noise", "trigger_key": "event:EVT_D",
             "t_stat": 0.2, "mean_return_bps": 2.0,    # t_stat too low → excluded
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.1, "context_json": "{}"},
        ])
        _write_regime_conditional_candidates(df, tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        assert len(result) == 1
        assert result.iloc[0]["event_type"] == "EVT_B"

    def test_sorted_by_mean_return_descending(self, tmp_path):
        df = pd.DataFrame([
            {"hypothesis_id": f"h{i}", "trigger_key": f"event:EVT_{i}",
             "t_stat": 1.0, "mean_return_bps": float(i),
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.5, "context_json": "{}"}
            for i in range(1, 6)  # returns: 1,2,3,4,5 — all qualify (t=1.0)
        ])
        _write_regime_conditional_candidates(df, tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        returns = result["mean_return_bps"].tolist()
        assert returns == sorted(returns, reverse=True)

    def test_top_k_cap(self, tmp_path):
        rows = [
            {"hypothesis_id": f"h{i}", "trigger_key": f"event:EVT_{i}",
             "t_stat": 1.0, "mean_return_bps": float(i + 1),
             "template_id": "m", "direction": "long", "horizon": "12b",
             "robustness_score": 0.5, "context_json": "{}"}
            for i in range(50)
        ]
        df = pd.DataFrame(rows)
        _write_regime_conditional_candidates(df, tmp_path, top_k=10)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        assert len(result) <= 10

    def test_empty_final_df_writes_empty_file(self, tmp_path):
        _write_regime_conditional_candidates(pd.DataFrame(), tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        assert result.empty

    def test_extracts_event_type_from_trigger_key(self, tmp_path):
        df = pd.DataFrame([{
            "hypothesis_id": "h0", "trigger_key": "event:LIQUIDATION_CASCADE",
            "t_stat": 1.0, "mean_return_bps": 5.0,
            "template_id": "m", "direction": "long", "horizon": "12b",
            "robustness_score": 0.5, "context_json": "{}",
        }])
        _write_regime_conditional_candidates(df, tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        assert result.iloc[0]["event_type"] == "LIQUIDATION_CASCADE"

    def test_state_trigger_key_parsed(self, tmp_path):
        df = pd.DataFrame([{
            "hypothesis_id": "h0", "trigger_key": "state:HIGH_VOL_REGIME",
            "t_stat": 1.0, "mean_return_bps": 5.0,
            "template_id": "m", "direction": "long", "horizon": "12b",
            "robustness_score": 0.5, "context_json": "{}",
        }])
        _write_regime_conditional_candidates(df, tmp_path)
        result = pd.read_parquet(tmp_path / "regime_conditional_candidates.parquet")
        assert result.iloc[0]["event_type"] == "HIGH_VOL_REGIME"


class TestLoadRegimeConditionalCandidates:

    def test_returns_empty_when_path_missing(self, tmp_path):
        result = _load_regime_conditional_candidates(run_id="nonexistent_run", data_root=tmp_path)
        assert result.empty

    def test_loads_parquet_from_expected_path(self, tmp_path):
        rcc_dir = tmp_path / "reports" / "phase2" / "run_001"
        rcc_dir.mkdir(parents=True)
        df = pd.DataFrame([{
            "event_type": "VOL_SPIKE", "template_id": "mean_reversion",
            "direction": "long", "horizon": "12b",
            "trigger_key": "event:VOL_SPIKE", "t_stat": 1.1,
            "mean_return_bps": 4.5, "robustness_score": 0.6, "context_json": "{}",
        }])
        df.to_parquet(rcc_dir / "regime_conditional_candidates.parquet", index=False)
        result = _load_regime_conditional_candidates(run_id="run_001", data_root=tmp_path)
        assert len(result) == 1
        assert result.iloc[0]["event_type"] == "VOL_SPIKE"

    def test_loads_parquet_from_hypothesis_search_path(self, tmp_path):
        rcc_dir = tmp_path / "reports" / "hypothesis_search" / "run_hs"
        rcc_dir.mkdir(parents=True)
        df = pd.DataFrame([{
            "event_type": "VOL_SPIKE", "template_id": "mean_reversion",
            "direction": "long", "horizon": "12b",
            "trigger_key": "event:VOL_SPIKE", "t_stat": 1.1,
            "mean_return_bps": 4.5, "robustness_score": 0.6, "context_json": "{}",
        }])
        df.to_parquet(rcc_dir / "regime_conditional_candidates.parquet", index=False)

        result = _load_regime_conditional_candidates(run_id="run_hs", data_root=tmp_path)

        assert len(result) == 1
        assert result.iloc[0]["event_type"] == "VOL_SPIKE"

    def test_raises_on_corrupt_file(self, tmp_path):
        rcc_dir = tmp_path / "reports" / "phase2" / "run_bad"
        rcc_dir.mkdir(parents=True)
        (rcc_dir / "regime_conditional_candidates.parquet").write_bytes(b"NOTPARQUET")
        with pytest.raises(DataIntegrityError):
            _load_regime_conditional_candidates(run_id="run_bad", data_root=tmp_path)

    def test_ignores_legacy_nested_search_engine_path(self, tmp_path):
        nested_dir = tmp_path / "reports" / "phase2" / "run_pref" / "search_engine"
        nested_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"event_type": "NESTED"}]).to_parquet(
            nested_dir / "regime_conditional_candidates.parquet", index=False
        )

        result = _load_regime_conditional_candidates(run_id="run_pref", data_root=tmp_path)

        assert result.empty


class TestBuildNextActionsRegimeCandidates:

    def _empty_state(self):
        return {
            "belief_state": {}, "next_actions": {}, "latest_reflection": {},
            "avoid_region_keys": set(), "avoid_event_types": set(),
            "promising_regions": [], "superseded_stages": set(),
        }

    def test_regime_candidates_injected_into_explore_adjacent(self):
        regime_df = pd.DataFrame([{
            "event_type": "VOL_SPIKE", "template_id": "mean_reversion",
            "direction": "long", "horizon": "12b",
            "trigger_key": "event:VOL_SPIKE", "t_stat": 1.1,
            "mean_return_bps": 4.5, "robustness_score": 0.6, "context_json": "{}",
        }])
        result = _build_next_actions(
            reflection={},
            tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=regime_df,
        )
        explore = result["explore_adjacent"]
        regime_entries = [e for e in explore if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert len(regime_entries) >= 1
        assert regime_entries[0]["proposed_scope"]["event_type"] == "VOL_SPIKE"

    def test_regime_entries_have_signal_field(self):
        regime_df = pd.DataFrame([{
            "event_type": "VOL_SPIKE", "template_id": "mean_reversion",
            "direction": "long", "horizon": "12b",
            "trigger_key": "event:VOL_SPIKE", "t_stat": 1.1,
            "mean_return_bps": 4.5, "robustness_score": 0.6, "context_json": "{}",
        }])
        result = _build_next_actions(
            reflection={}, tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(), exploit_top_k=3, repair_top_k=3,
            regime_conditional_candidates=regime_df,
        )
        regime_entries = [e for e in result["explore_adjacent"]
                          if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert len(regime_entries) >= 1
        assert "proposed_scope" in regime_entries[0]
        assert "event_type" in regime_entries[0]["proposed_scope"]

    def test_no_regime_candidates_preserves_existing_behaviour(self):
        result = _build_next_actions(
            reflection={"recommended_next_experiment": json.dumps({"event_type": "FND_DISLOC"})},
            tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        assert len(result["explore_adjacent"]) == 1
        assert result["explore_adjacent"][0]["proposed_scope"]["event_type"] == "FND_DISLOC"

    def test_empty_regime_df_does_not_add_entries(self):
        result = _build_next_actions(
            reflection={}, tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(), exploit_top_k=3, repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        regime_entries = [e for e in result["explore_adjacent"]
                          if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert len(regime_entries) == 0

    def test_regime_entries_capped_at_five(self):
        many_rows = pd.DataFrame([{
            "event_type": f"EVT_{i}", "template_id": "m", "direction": "long",
            "horizon": "12b", "trigger_key": f"event:EVT_{i}",
            "t_stat": 1.1, "mean_return_bps": 5.0, "robustness_score": 0.5, "context_json": "{}",
        } for i in range(20)])
        result = _build_next_actions(
            reflection={}, tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(), exploit_top_k=3, repair_top_k=3,
            regime_conditional_candidates=many_rows,
        )
        regime_entries = [e for e in result["explore_adjacent"]
                          if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert len(regime_entries) <= 5

    def test_regime_entries_are_deduped_and_blank_recommended_scope_is_skipped(self):
        regime_df = pd.DataFrame([
            {
                "event_type": "VOL_SHOCK", "template_id": "continuation", "direction": "short",
                "horizon": "24b", "entry_lag": 2, "trigger_key": "event:VOL_SHOCK",
                "t_stat": 0.9, "mean_return_bps": 4.1, "robustness_score": 0.3, "context_json": "{}",
            },
            {
                "event_type": "VOL_SHOCK", "template_id": "continuation", "direction": "short",
                "horizon": "24b", "entry_lag": 2, "trigger_key": "event:VOL_SHOCK",
                "t_stat": 0.8, "mean_return_bps": 3.8, "robustness_score": 0.3, "context_json": "{}",
            },
        ])
        result = _build_next_actions(
            reflection={"recommended_next_experiment": json.dumps({"event_type": "", "primary_fail_gate": ""})},
            tested_regions=pd.DataFrame(),
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=regime_df,
        )
        regime_entries = [e for e in result["explore_adjacent"]
                          if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert len(regime_entries) == 1
        assert regime_entries[0]["proposed_scope"]["event_type"] == "VOL_SHOCK"
        assert "best_regime" not in regime_entries[0]["proposed_scope"]
        assert "contexts" not in regime_entries[0]["proposed_scope"]

    def test_regime_entries_skip_already_tested_exact_scope_including_zero_entry_lag(self):
        regime_df = pd.DataFrame([
            {
                "event_type": "VOL_SHOCK", "template_id": "continuation", "direction": "short",
                "horizon": "24b", "entry_lag": 0, "trigger_key": "event:VOL_SHOCK",
                "t_stat": 0.9, "mean_return_bps": 4.1, "robustness_score": 0.3, "context_json": "{}",
            }
        ])
        tested_regions = pd.DataFrame([
            {
                "trigger_type": "EVENT",
                "event_type": "VOL_SHOCK",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 0,
                "context_json": "{}",
            }
        ])
        result = _build_next_actions(
            reflection={},
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=regime_df,
        )
        regime_entries = [e for e in result["explore_adjacent"]
                          if e.get("reason") == "strong regime slice despite weak aggregate result"]
        assert regime_entries == []

    def test_hold_reflection_suppresses_exploit_queue(self):
        tested_regions = pd.DataFrame([
            {
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 2,
                "context_json": "{}",
                "region_key": "rk1",
                "after_cost_expectancy": 1.0,
                "q_value": 0.2,
                "gate_promo_statistical": False,
            }
        ])
        result = _build_next_actions(
            reflection={"recommended_next_action": "hold"},
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        assert result["exploit"] == []


    def test_economics_policy_builds_exploit_queue_for_strengthening_event(self):
        tested_regions = pd.DataFrame([
            {
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "long",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_exploit",
                "after_cost_expectancy": 2.0,
                "stressed_after_cost_expectancy": 1.4,
                "q_value": 0.10,
                "eval_status": "promoted",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": True,
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "long",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_exploit_2",
                "after_cost_expectancy": 2.6,
                "stressed_after_cost_expectancy": 1.8,
                "q_value": 0.08,
                "eval_status": "promoted",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": True,
                "updated_at": "2026-01-02T00:00:00+00:00",
            },
            {
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "long",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_exploit_3",
                "after_cost_expectancy": 3.0,
                "stressed_after_cost_expectancy": 2.1,
                "q_value": 0.06,
                "eval_status": "promoted",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": True,
                "updated_at": "2026-01-03T00:00:00+00:00",
            },
        ])
        result = _build_next_actions(
            reflection={},
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        assert len(result["exploit"]) >= 1
        assert result["exploit"][0]["policy_action"] == "exploit"
        assert result["exploit"][0]["proposed_scope"]["event_type"] == "VOL_SHOCK"

    def test_economics_policy_builds_retest_queue_for_structurally_viable_unstable_event(self):
        tested_regions = pd.DataFrame([
            {
                "event_type": "FUNDING_FLIP",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "entry_lag": 2,
                "context_json": "{}",
                "region_key": "rk_retest_1",
                "after_cost_expectancy": -0.2,
                "stressed_after_cost_expectancy": -0.6,
                "q_value": 0.22,
                "eval_status": "evaluated",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": False,
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "event_type": "FUNDING_FLIP",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "entry_lag": 2,
                "context_json": "{}",
                "region_key": "rk_retest_2",
                "after_cost_expectancy": 1.2,
                "stressed_after_cost_expectancy": -0.2,
                "q_value": 0.18,
                "eval_status": "evaluated",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": True,
                "updated_at": "2026-01-02T00:00:00+00:00",
            },
            {
                "event_type": "FUNDING_FLIP",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "entry_lag": 2,
                "context_json": "{}",
                "region_key": "rk_retest_3",
                "after_cost_expectancy": 0.8,
                "stressed_after_cost_expectancy": -0.1,
                "q_value": 0.16,
                "eval_status": "evaluated",
                "gate_bridge_tradable": True,
                "gate_promo_statistical": True,
                "updated_at": "2026-01-03T00:00:00+00:00",
            },
        ])
        result = _build_next_actions(
            reflection={},
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        assert len(result["retest"]) >= 1
        assert result["retest"][0]["policy_action"] == "retest"
        assert result["retest"][0]["proposed_scope"]["event_type"] == "FUNDING_FLIP"

    def test_economics_policy_builds_hold_queue_for_repeated_cost_drag(self):
        tested_regions = pd.DataFrame([
            {
                "event_type": "VOL_SPIKE",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_hold_1",
                "after_cost_expectancy": -1.0,
                "stressed_after_cost_expectancy": -1.4,
                "q_value": 0.40,
                "eval_status": "evaluated",
                "gate_bridge_tradable": False,
                "gate_promo_statistical": False,
                "primary_fail_gate": "gate_after_cost_positive",
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "event_type": "VOL_SPIKE",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_hold_2",
                "after_cost_expectancy": -1.2,
                "stressed_after_cost_expectancy": -1.5,
                "q_value": 0.45,
                "eval_status": "evaluated",
                "gate_bridge_tradable": False,
                "gate_promo_statistical": False,
                "primary_fail_gate": "gate_after_cost_positive",
                "updated_at": "2026-01-02T00:00:00+00:00",
            },
            {
                "event_type": "VOL_SPIKE",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 1,
                "context_json": "{}",
                "region_key": "rk_hold_3",
                "after_cost_expectancy": -1.3,
                "stressed_after_cost_expectancy": -1.6,
                "q_value": 0.50,
                "eval_status": "evaluated",
                "gate_bridge_tradable": False,
                "gate_promo_statistical": False,
                "primary_fail_gate": "gate_after_cost_positive",
                "updated_at": "2026-01-03T00:00:00+00:00",
            },
        ])
        result = _build_next_actions(
            reflection={},
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
            regime_conditional_candidates=pd.DataFrame(),
        )
        assert len(result["hold"]) >= 1
        assert result["hold"][0]["policy_action"] == "hold"
        assert result["hold"][0]["proposed_scope"]["event_type"] == "VOL_SPIKE"


class TestBuildBeliefState:

    def test_hold_reflection_suppresses_promising_regions(self):
        tested_regions = pd.DataFrame([
            {
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag": 2,
                "context_json": "{}",
                "region_key": "rk1",
                "after_cost_expectancy": 1.0,
                "q_value": 0.2,
                "gate_promo_statistical": False,
            }
        ])
        result = _build_belief_state(
            tested_regions=tested_regions,
            failures=pd.DataFrame(),
            reflection={"recommended_next_action": "hold", "statistical_outcome": "no_signal"},
            promising_top_k=3,
            avoid_top_k=3,
            repair_top_k=3,
        )
        assert result["promising_regions"] == []

    def test_superseded_or_invalid_failures_do_not_surface_as_open_repairs(self):
        failures = pd.DataFrame([
            {
                "stage": "None",
                "failure_class": "run_failed_stage",
                "failure_detail": "",
                "superseded_by_run_id": "",
            },
            {
                "stage": "phase2_search_engine",
                "failure_class": "stage_failed",
                "failure_detail": "boom",
                "superseded_by_run_id": "resolved_run",
            },
        ])
        result = _build_belief_state(
            tested_regions=pd.DataFrame(),
            failures=failures,
            reflection={},
            promising_top_k=3,
            avoid_top_k=3,
            repair_top_k=3,
        )
        assert result["open_repairs"] == []


# ===========================================================================
# Phase 4.3 — Context complexity penalty
# ===========================================================================

class TestCountContextDimensions:

    def test_none_returns_zero(self):
        assert _count_context_dimensions(None) == 0

    def test_empty_dict_returns_zero(self):
        assert _count_context_dimensions({}) == 0

    def test_single_dimension_dict(self):
        assert _count_context_dimensions({"vol_regime": "high"}) == 1

    def test_two_dimension_dict(self):
        assert _count_context_dimensions({"vol": "high", "trend": "bull"}) == 2

    def test_four_dimension_dict(self):
        assert _count_context_dimensions({"a": 1, "b": 2, "c": 3, "d": 4}) == 4

    def test_json_string_parsed(self):
        assert _count_context_dimensions('{"vol": "high", "trend": "bull"}') == 2

    def test_empty_json_string_returns_zero(self):
        assert _count_context_dimensions("{}") == 0

    def test_malformed_json_returns_zero(self):
        assert _count_context_dimensions("NOT JSON {{{") == 0

    def test_non_dict_json_returns_zero(self):
        assert _count_context_dimensions("[1, 2, 3]") == 0


class TestContextComplexityPenalty:

    def test_zero_dimensions_no_penalty(self):
        assert _context_complexity_penalty(0) == pytest.approx(0.0)

    def test_one_dimension_no_penalty(self):
        assert _context_complexity_penalty(1) == pytest.approx(0.0)

    def test_two_dimensions(self):
        expected = math.log1p(1) * 0.05
        assert _context_complexity_penalty(2) == pytest.approx(expected, rel=1e-6)

    def test_four_dimensions(self):
        expected = math.log1p(3) * 0.05
        assert _context_complexity_penalty(4) == pytest.approx(expected, rel=1e-6)

    def test_penalty_monotonically_increasing(self):
        penalties = [_context_complexity_penalty(d) for d in range(8)]
        assert penalties == sorted(penalties)

    def test_four_dim_penalty_meaningful(self):
        """Four-condition conjunction should pay ~0.069 — meaningful vs typical scores."""
        p = _context_complexity_penalty(4)
        assert 0.05 < p < 0.15


class TestStabilityScoreWithPenalty:

    def _base_row(self, ctx: Any = None) -> dict[str, Any]:
        return {
            "effect_shrunk_state": 0.5,
            "std_return": 0.2,
            "context_json": ctx,
        }

    def test_no_context_zero_penalty(self):
        row = self._base_row(None)
        raw = stability_score(row, 0.8, apply_context_penalty=False)
        penalised = stability_score(row, 0.8, apply_context_penalty=True)
        assert raw == pytest.approx(penalised, rel=1e-9)

    def test_one_dim_context_zero_penalty(self):
        row = self._base_row({"vol": "high"})
        raw = stability_score(row, 0.8, apply_context_penalty=False)
        penalised = stability_score(row, 0.8, apply_context_penalty=True)
        assert raw == pytest.approx(penalised, rel=1e-9)

    def test_two_dim_context_reduces_score(self):
        row = self._base_row({"vol": "high", "trend": "bull"})
        raw = stability_score(row, 0.8, apply_context_penalty=False)
        penalised = stability_score(row, 0.8, apply_context_penalty=True)
        assert penalised < raw

    def test_four_dim_context_reduces_more_than_two(self):
        row2 = self._base_row({"a": 1, "b": 2})
        row4 = self._base_row({"a": 1, "b": 2, "c": 3, "d": 4})
        p2 = stability_score(row2, 0.8)
        p4 = stability_score(row4, 0.8)
        assert p4 < p2

    def test_penalty_flag_disables_penalty(self):
        row = self._base_row({"a": 1, "b": 2, "c": 3})
        penalised = stability_score(row, 0.8, apply_context_penalty=True)
        unpenalised = stability_score(row, 0.8, apply_context_penalty=False)
        assert unpenalised > penalised

    def test_nan_inputs_propagate(self):
        row = {"effect_shrunk_state": float("nan"), "std_return": 0.2}
        score = stability_score(row, 0.8)
        assert np.isnan(score)


# ===========================================================================
# Phase 4.4 — AllocationSpec handoff
# ===========================================================================

class TestResolveSizingInputs:

    def test_mean_return_bps_direct(self):
        row = {"mean_return_bps": 15.0, "stressed_after_cost_expectancy": None}
        ret, adv = _resolve_sizing_inputs(row)
        assert ret == pytest.approx(15.0)
        assert adv is None

    def test_stressed_after_cost_expectancy_populates_adverse(self):
        row = {"mean_return_bps": 10.0, "stressed_after_cost_expectancy": 5.0}
        ret, adv = _resolve_sizing_inputs(row)
        assert ret == pytest.approx(10.0)
        assert adv == pytest.approx(5.0 * 1.5)

    def test_stressed_decimal_converted_to_bps(self):
        """stressed_after_cost_expectancy as decimal (< 1.0) is converted × 10_000."""
        row = {"mean_return_bps": 10.0, "stressed_after_cost_expectancy": 0.0008}
        ret, adv = _resolve_sizing_inputs(row)
        assert adv == pytest.approx(0.0008 * 10_000 * 1.5, rel=1e-4)

    def test_fallback_to_after_cost_expectancy_decimal(self):
        """When mean_return_bps absent, falls back to after_cost_expectancy (decimal)."""
        row = {"after_cost_expectancy": 0.001}  # 10 bps
        ret, adv = _resolve_sizing_inputs(row)
        assert ret == pytest.approx(10.0, rel=1e-4)

    def test_empty_row_returns_none_none(self):
        ret, adv = _resolve_sizing_inputs({})
        assert ret is None
        assert adv is None

    def test_nan_mean_return_returns_none(self):
        row = {"mean_return_bps": float("nan")}
        ret, adv = _resolve_sizing_inputs(row)
        assert ret is None

    def test_adverse_is_absolute_value(self):
        """Negative stressed expectancy → adverse is still positive."""
        row = {"mean_return_bps": 5.0, "stressed_after_cost_expectancy": -3.0}
        _, adv = _resolve_sizing_inputs(row)
        assert adv is not None
        assert adv > 0


class TestSizingPolicySpecNewFields:

    def test_default_fields_are_none(self):
        spec = SizingPolicySpec(mode="kelly", max_gross_leverage=2.0)
        assert spec.expected_return_bps is None
        assert spec.expected_adverse_bps is None

    def test_fields_accept_float_values(self):
        spec = SizingPolicySpec(
            mode="kelly", max_gross_leverage=2.0,
            expected_return_bps=12.5, expected_adverse_bps=7.5,
        )
        assert spec.expected_return_bps == pytest.approx(12.5)
        assert spec.expected_adverse_bps == pytest.approx(7.5)

    def test_serialisation_includes_new_fields(self):
        spec = SizingPolicySpec(
            mode="kelly", max_gross_leverage=2.0,
            expected_return_bps=12.5, expected_adverse_bps=7.5,
        )
        d = spec.model_dump()
        assert "expected_return_bps" in d
        assert "expected_adverse_bps" in d


class TestAllocationSpecFromBlueprint:

    def _make_blueprint(self) -> MagicMock:
        bp = MagicMock()
        bp.id = "bp_001"
        bp.candidate_id = "cand_001"
        bp.event_type = "VOL_SPIKE"
        bp.sizing.mode = "kelly"
        bp.sizing.risk_per_trade = 0.01
        bp.sizing.max_gross_leverage = 2.0
        bp.sizing.portfolio_risk_budget = 1.0
        bp.sizing.symbol_risk_budget = 0.5
        bp.sizing.signal_scaling = {}
        bp.lineage.constraints = {}
        bp.symbol_scope.model_dump.return_value = {"symbols": ["BTCUSDT"]}
        return bp

    def test_sizing_inputs_populated_from_kwargs(self):
        bp = self._make_blueprint()
        spec = AllocationSpec.from_blueprint(
            blueprint=bp, run_id="r1", retail_profile="standard",
            low_capital_contract={}, effective_max_concurrent_positions=5,
            effective_per_position_notional_cap_usd=10000.0,
            default_fee_tier="tier1", fees_bps_per_side=4.0, slippage_bps_per_fill=2.0,
            expected_return_bps=12.5, expected_adverse_bps=6.0,
        )
        assert spec.sizing_policy.expected_return_bps == pytest.approx(12.5)
        assert spec.sizing_policy.expected_adverse_bps == pytest.approx(6.0)

    def test_defaults_preserved_when_not_provided(self):
        bp = self._make_blueprint()
        spec = AllocationSpec.from_blueprint(
            blueprint=bp, run_id="r1", retail_profile="standard",
            low_capital_contract={}, effective_max_concurrent_positions=5,
            effective_per_position_notional_cap_usd=10000.0,
            default_fee_tier="tier1", fees_bps_per_side=4.0, slippage_bps_per_fill=2.0,
        )
        assert spec.sizing_policy.expected_return_bps is None
        assert spec.sizing_policy.expected_adverse_bps is None


class TestBuildAllocationSpecWithAuditRow:

    def _make_blueprint(self) -> MagicMock:
        bp = MagicMock()
        bp.id = "bp_001"
        bp.candidate_id = "cand_001"
        bp.event_type = "VOL_SPIKE"
        bp.sizing.mode = "kelly"
        bp.sizing.risk_per_trade = 0.01
        bp.sizing.max_gross_leverage = 2.0
        bp.sizing.portfolio_risk_budget = 1.0
        bp.sizing.symbol_risk_budget = 0.5
        bp.sizing.signal_scaling = {}
        bp.lineage.constraints = {}
        bp.symbol_scope.model_dump.return_value = {"symbols": ["BTCUSDT"]}
        return bp

    def _kwargs(self) -> dict[str, Any]:
        return dict(
            run_id="r1", retail_profile="standard", low_capital_contract={},
            effective_max_concurrent_positions=5,
            effective_per_position_notional_cap_usd=10000.0,
            default_fee_tier="tier1", fees_bps_per_side=4.0, slippage_bps_per_fill=2.0,
        )

    def test_audit_row_populates_sizing(self):
        bp = self._make_blueprint()
        audit = {"mean_return_bps": 20.0, "stressed_after_cost_expectancy": 8.0}
        spec = _build_allocation_spec(blueprint=bp, audit_row=audit, **self._kwargs())
        assert spec.sizing_policy.expected_return_bps == pytest.approx(20.0)
        assert spec.sizing_policy.expected_adverse_bps == pytest.approx(8.0 * 1.5)

    def test_no_audit_row_leaves_sizing_none(self):
        bp = self._make_blueprint()
        spec = _build_allocation_spec(blueprint=bp, audit_row=None, **self._kwargs())
        assert spec.sizing_policy.expected_return_bps is None
        assert spec.sizing_policy.expected_adverse_bps is None

    def test_empty_audit_row_leaves_sizing_none(self):
        bp = self._make_blueprint()
        spec = _build_allocation_spec(blueprint=bp, audit_row={}, **self._kwargs())
        assert spec.sizing_policy.expected_return_bps is None


class TestCheckMarginalContribution:

    def _make_blueprint(self, rpt: float = 0.01, mgl: float = 2.0, prb: float = 1.0) -> MagicMock:
        bp = MagicMock()
        bp.sizing.risk_per_trade = rpt
        bp.sizing.max_gross_leverage = mgl
        bp.sizing.portfolio_risk_budget = prb
        return bp

    def test_empty_existing_always_passes(self):
        bp = self._make_blueprint()
        passes, max_sim = _check_marginal_contribution(bp, [])
        assert passes is True
        assert max_sim == pytest.approx(0.0)

    def test_identical_blueprint_fails(self):
        bp1 = self._make_blueprint(0.01, 2.0, 1.0)
        bp2 = self._make_blueprint(0.01, 2.0, 1.0)
        passes, max_sim = _check_marginal_contribution(bp2, [bp1])
        assert passes is False
        assert max_sim == pytest.approx(1.0, rel=1e-6)

    def test_orthogonal_blueprints_pass(self):
        # Different leverage/risk profiles → low cosine similarity
        bp1 = self._make_blueprint(0.01, 2.0, 0.0)
        bp2 = self._make_blueprint(0.0, 0.0, 1.0)
        passes, _ = _check_marginal_contribution(bp2, [bp1])
        assert passes is True

    def test_threshold_respected(self):
        bp1 = self._make_blueprint(0.01, 2.0, 1.0)
        bp2 = self._make_blueprint(0.015, 2.1, 1.0)  # very similar — cosine ~0.9998
        # With default threshold 0.8 → fails (similarity 0.9998 > 0.8)
        passes_default, max_sim = _check_marginal_contribution(bp2, [bp1])
        assert passes_default is False
        # With threshold above the actual similarity → passes
        passes_high, _ = _check_marginal_contribution(bp2, [bp1], max_correlation=0.9999)
        assert passes_high is True

    def test_returns_max_similarity_value(self):
        bp1 = self._make_blueprint(0.01, 2.0, 1.0)
        bp2 = self._make_blueprint(0.01, 2.0, 1.0)
        _, max_sim = _check_marginal_contribution(bp2, [bp1])
        assert 0.0 <= max_sim <= 1.0 + 1e-9

    def test_zero_vector_blueprint_passes_gracefully(self):
        bp_zero = self._make_blueprint(0.0, 0.0, 0.0)
        bp_normal = self._make_blueprint(0.01, 2.0, 1.0)
        passes, _ = _check_marginal_contribution(bp_zero, [bp_normal])
        assert passes is True  # Zero-norm new blueprint → trivially passes
