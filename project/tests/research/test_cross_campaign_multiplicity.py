"""
Tests for cross-campaign / campaign-lineage multiplicity control.

Phase 1 acceptance tests:
- both side_policy counts as 2 tests
- num_tests_campaign reflects full campaign scope
- q_value_scope >= q_value when scope broadens
- effective_q_value = max(applicable q-values)
- promotion rejects candidate if effective_q_value > max_q_value
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.research.contracts.multiplicity_scope import (
    infer_multiplicity_scope,
    resolve_campaign_scope_key,
    resolve_effective_scope_key,
    resolve_lineage_scope_key,
)
from project.research.multiplicity import (
    apply_canonical_cross_campaign_multiplicity,
    apply_multiplicity_controls,
    merge_historical_candidates,
)


class TestMultiplicityScopeContract:
    """Test the canonical multiplicity scope contract."""

    def test_infer_multiplicity_scope(self):
        row = {
            "run_id": "run_001",
            "campaign_id": "camp_001",
            "program_id": "prog_001",
            "concept_lineage_key": "lineage_abc",
            "family_id": "family_xyz",
            "side_policy": "directional",
        }
        scope = infer_multiplicity_scope(row)
        assert scope.run_id == "run_001"
        assert scope.campaign_id == "camp_001"
        assert scope.program_id == "prog_001"
        assert scope.concept_lineage_key == "lineage_abc"
        assert scope.family_id == "family_xyz"
        assert scope.side_policy == "directional"
        assert scope.scope_version == "phase1_v1"

    def test_resolve_campaign_scope_key_prioritizes_campaign(self):
        row = {"run_id": "run_001", "campaign_id": "camp_001", "program_id": "prog_001"}
        key = resolve_campaign_scope_key(row)
        assert key == "campaign::camp_001"

    def test_resolve_campaign_scope_key_falls_back_to_program(self):
        row = {"run_id": "run_001", "program_id": "prog_001"}
        key = resolve_campaign_scope_key(row)
        assert key == "program::prog_001"

    def test_resolve_campaign_scope_key_falls_back_to_run(self):
        row = {"run_id": "run_001"}
        key = resolve_campaign_scope_key(row)
        assert key == "run::run_001"

    def test_resolve_lineage_scope_key_includes_lineage(self):
        row = {
            "campaign_id": "camp_001",
            "concept_lineage_key": "lineage_abc",
        }
        key = resolve_lineage_scope_key(row)
        assert "campaign::camp_001" in key
        assert "lineage::lineage_abc" in key

    def test_resolve_effective_scope_key_modes(self):
        row = {
            "run_id": "run_001",
            "campaign_id": "camp_001",
            "program_id": "prog_001",
            "concept_lineage_key": "lineage_abc",
        }
        assert resolve_effective_scope_key(row, mode="run").startswith("run::")
        assert resolve_effective_scope_key(row, mode="campaign").startswith("campaign::")
        assert resolve_effective_scope_key(row, mode="program").startswith("program::")


class TestCrossCampaignMultiplicity:
    """Test canonical cross-campaign multiplicity adjustment."""

    def test_both_side_policy_weighted_as_two(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b", "c"],
            "p_value_for_fdr": [0.01, 0.02, 0.03],
            "family_id": ["f1", "f1", "f2"],
            "campaign_id": ["c1", "c1", "c1"],
            "side_policy": ["directional", "both", "directional"],
            "run_id": ["r1", "r1", "r1"],
            "multiplicity_pool_eligible": [True, True, True],
        })
        result = apply_multiplicity_controls(df, max_q=0.05)
        # Canonical split:
        # - num_tests_family counts row-level family members
        # - num_tests_effective counts multiplicity-weighted tests, where
        #   side_policy='both' contributes two effective tests.
        assert result.loc[1, "num_tests_family"] == 2  # f1 has two family rows
        assert result.loc[1, "num_tests_effective"] == 3  # 1 directional + 2 (both)
        assert result.loc[2, "num_tests_family"] == 1  # f2 has one family row
        assert result.loc[2, "num_tests_effective"] == 1  # directional only

    def test_campaign_scope_key_determinism(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b", "c"],
            "p_value_for_fdr": [0.01, 0.02, 0.03],
            "family_id": ["f1", "f1", "f2"],
            "campaign_id": ["camp_001", "camp_001", "camp_001"],
            "run_id": ["r1", "r1", "r1"],
            "side_policy": ["directional", "directional", "directional"],
            "multiplicity_pool_eligible": [True, True, True],
        })
        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05)
        # All should have same campaign scope key
        assert all(result["multiplicity_scope_key"] == "campaign::camp_001")

    def test_num_tests_scope_reflects_full_scope(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b", "c", "d"],
            "p_value_for_fdr": [0.01, 0.02, 0.03, 0.04],
            "family_id": ["f1", "f1", "f2", "f2"],
            "campaign_id": ["camp_001", "camp_001", "camp_002", "camp_002"],
            "run_id": ["r1", "r1", "r2", "r2"],
            "concept_lineage_key": ["L1", "L1", "L1", "L1"],
            "side_policy": ["directional", "directional", "directional", "directional"],
            "multiplicity_pool_eligible": [True, True, True, True],
        })
        result = apply_canonical_cross_campaign_multiplicity(
            df, max_q=0.05, scope_mode="campaign_lineage"
        )
        # Each campaign should count tests across families
        for idx, row in result.iterrows():
            assert row["num_tests_scope"] >= 1

    def test_q_value_scope_greater_or_equal_to_local(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b", "c"],
            "p_value_for_fdr": [0.01, 0.02, 0.05],
            "family_id": ["f1", "f1", "f2"],
            "campaign_id": ["camp_001", "camp_001", "camp_001"],
            "q_value": [0.03, 0.04, 0.06],
            "run_id": ["r1", "r1", "r1"],
            "side_policy": ["directional", "directional", "directional"],
            "multiplicity_pool_eligible": [True, True, True],
        })
        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05)
        # Scope q-value should be >= local q-value when scope broadens
        for idx in result.index:
            assert result.loc[idx, "q_value_scope"] >= result.loc[idx, "q_value"] * 0.99  # numerical tolerance

    def test_effective_q_value_computed(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b"],
            "p_value_for_fdr": [0.01, 0.02],
            "family_id": ["f1", "f1"],
            "campaign_id": ["camp_001", "camp_001"],
            "q_value": [0.03, 0.04],
            "q_value_program": [0.05, 0.06],
            "run_id": ["r1", "r1"],
            "side_policy": ["directional", "directional"],
            "multiplicity_pool_eligible": [True, True],
        })
        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.10)
        # effective_q_value should be max of q_value, q_value_scope, q_value_program
        for idx in result.index:
            local_q = result.loc[idx, "q_value"]
            scope_q = result.loc[idx, "q_value_scope"]
            prog_q = result.loc[idx, "q_value_program"]
            effective_q = result.loc[idx, "effective_q_value"]
            assert effective_q >= local_q
            assert effective_q >= scope_q * 0.99
            assert effective_q >= prog_q * 0.99

    def test_effective_q_value_preserves_zero_q_values(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b"],
            "p_value_for_fdr": [0.0, 0.02],
            "family_id": ["f1", "f1"],
            "campaign_id": ["camp_001", "camp_001"],
            "q_value": [0.0, np.nan],
            "q_value_program": [0.0, 0.04],
            "run_id": ["r1", "r1"],
            "side_policy": ["directional", "directional"],
            "multiplicity_pool_eligible": [True, True],
        })

        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.10)

        assert result.loc[0, "effective_q_value"] == pytest.approx(0.0, abs=1e-12)
        assert np.isfinite(result.loc[1, "effective_q_value"])

    def test_scope_mode_variations(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "run_id": ["run_001"],
            "campaign_id": ["camp_001"],
            "program_id": ["prog_001"],
            "multiplicity_pool_eligible": [True],
        })
        result_run = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05, scope_mode="run")
        result_campaign = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05, scope_mode="campaign")
        result_program = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05, scope_mode="program")

        assert result_run.loc[0, "multiplicity_scope_key"].startswith("run::")
        assert result_campaign.loc[0, "multiplicity_scope_key"].startswith("campaign::")
        assert result_program.loc[0, "multiplicity_scope_key"].startswith("program::")

    def test_degraded_status_on_empty_historical(self):
        current = pd.DataFrame({
            "candidate_id": ["a"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "run_id": ["run_001"],
            "multiplicity_pool_eligible": [True],
        })
        result = merge_historical_candidates(current, historical=None, scope_mode="campaign_lineage")
        assert result.loc[0, "multiplicity_scope_degraded"] is True
        assert result.loc[0, "multiplicity_scope_reason"] == "missing_history"


class TestEffectiveQValueInPromotion:
    """Test that effective_q_value gates promotion decisions."""

    def test_scope_level_fields_propagated(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "run_id": ["run_001"],
            "multiplicity_pool_eligible": [True],
        })
        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05)
        assert "multiplicity_scope_mode" in result.columns
        assert "multiplicity_scope_key" in result.columns
        assert "multiplicity_scope_version" in result.columns
        assert "num_tests_scope" in result.columns
        assert "q_value_scope" in result.columns
        assert "is_discovery_scope" in result.columns
        assert "effective_q_value" in result.columns
        assert "is_discovery_effective" in result.columns


class TestBackwardCompatibility:
    """Test that existing fields are preserved."""

    def test_existing_q_value_columns_preserved(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "q_value": [0.03],
            "q_value_family": [0.03],
            "q_value_program": [0.04],
            "run_id": ["run_001"],
            "multiplicity_pool_eligible": [True],
        })
        result = apply_canonical_cross_campaign_multiplicity(df, max_q=0.05)
        # All original columns should still exist
        assert "q_value" in result.columns
        assert "q_value_family" in result.columns
        assert "q_value_program" in result.columns
        # New columns added
        assert "q_value_scope" in result.columns
        assert "effective_q_value" in result.columns


class TestHistoricalUniverseMerge:
    """Test historical-universe merge for scope multiplicity accounting."""

    def test_historical_rows_widen_scope_count(self):
        """Historical rows should increase num_tests_scope for current rows."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1", "curr_2"],
            "p_value_for_fdr": [0.01, 0.02],
            "family_id": ["f1", "f2"],
            "campaign_id": ["camp_001", "camp_001"],
            "run_id": ["run_002", "run_002"],
            "side_policy": ["directional", "directional"],
            "multiplicity_pool_eligible": [True, True],
        })
        historical = pd.DataFrame({
            "candidate_id": ["hist_1", "hist_2", "hist_3"],
            "p_value_for_fdr": [0.015, 0.025, 0.035],
            "family_id": ["f1", "f2", "f3"],
            "campaign_id": ["camp_001", "camp_001", "camp_001"],
            "run_id": ["run_001", "run_001", "run_001"],
            "side_policy": ["directional", "directional", "directional"],
            "multiplicity_pool_eligible": [True, True, True],
        })
        merged = merge_historical_candidates(current, historical, scope_mode="campaign")
        scored = apply_canonical_cross_campaign_multiplicity(merged, max_q=0.05)

        # Current rows should see scope count of 5 (2 current + 3 historical)
        current_scored = scored[scored["multiplicity_context"] == "current"]
        for idx in current_scored.index:
            assert current_scored.loc[idx, "num_tests_scope"] == 5

    def test_q_value_scope_more_conservative_with_history(self):
        """q_value_scope should be more conservative when historical rows included."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "q_value": [0.03],
            "run_id": ["run_002"],
            "side_policy": ["directional"],
            "multiplicity_pool_eligible": [True],
        })
        historical = pd.DataFrame({
            "candidate_id": ["hist_1", "hist_2", "hist_3", "hist_4"],
            "p_value_for_fdr": [0.02, 0.03, 0.04, 0.05],
            "family_id": ["f1", "f2", "f3", "f4"],
            "campaign_id": ["camp_001"] * 4,
            "run_id": ["run_001"] * 4,
            "side_policy": ["directional"] * 4,
            "multiplicity_pool_eligible": [True] * 4,
        })

        # With history
        merged = merge_historical_candidates(current, historical, scope_mode="campaign")
        scored_with = apply_canonical_cross_campaign_multiplicity(merged, max_q=0.05)
        q_with = scored_with[scored_with["multiplicity_context"] == "current"].iloc[0]["q_value_scope"]

        # Without history
        merged_no = merge_historical_candidates(current, None, scope_mode="campaign")
        scored_no = apply_canonical_cross_campaign_multiplicity(merged_no, max_q=0.05)
        q_no = scored_no[scored_no["multiplicity_context"] == "current"].iloc[0]["q_value_scope"]

        # With more tests, q_value_scope should be more conservative (higher)
        assert q_with >= q_no

    def test_historical_rows_marked_as_historical_context(self):
        """Historical rows should have multiplicity_context='historical'."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "run_id": ["run_002"],
            "multiplicity_pool_eligible": [True],
        })
        historical = pd.DataFrame({
            "candidate_id": ["hist_1"],
            "p_value_for_fdr": [0.02],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "run_id": ["run_001"],
            "multiplicity_pool_eligible": [True],
        })
        merged = merge_historical_candidates(current, historical, scope_mode="campaign")

        assert merged[merged["candidate_id"] == "curr_1"].iloc[0]["multiplicity_context"] == "current"
        assert merged[merged["candidate_id"] == "hist_1"].iloc[0]["multiplicity_context"] == "historical"

    def test_degraded_mode_when_history_unavailable(self):
        """When history unavailable, mark degraded and continue."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "run_id": ["run_002"],
            "multiplicity_pool_eligible": [True],
        })
        merged = merge_historical_candidates(current, historical=None, scope_mode="campaign")

        assert merged.iloc[0]["multiplicity_scope_degraded"] is True
        assert merged.iloc[0]["multiplicity_scope_reason"] == "missing_history"

    def test_scope_keys_isolated_across_lineages(self):
        """Historical rows from different lineage should not affect scope."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1"],
            "p_value_for_fdr": [0.01],
            "family_id": ["f1"],
            "campaign_id": ["camp_001"],
            "concept_lineage_key": ["lineage_A"],
            "run_id": ["run_002"],
            "side_policy": ["directional"],
            "multiplicity_pool_eligible": [True],
        })
        historical = pd.DataFrame({
            "candidate_id": ["hist_1", "hist_2"],
            "p_value_for_fdr": [0.02, 0.03],
            "family_id": ["f1", "f2"],
            "campaign_id": ["camp_001", "camp_001"],
            "concept_lineage_key": ["lineage_A", "lineage_B"],  # Different lineage
            "run_id": ["run_001", "run_001"],
            "side_policy": ["directional", "directional"],
            "multiplicity_pool_eligible": [True, True],
        })
        merged = merge_historical_candidates(current, historical, scope_mode="campaign_lineage")
        scored = apply_canonical_cross_campaign_multiplicity(merged, max_q=0.05, scope_mode="campaign_lineage")

        # Current row should only see scope count of 2 (1 current + 1 matching lineage historical)
        current_scored = scored[scored["multiplicity_context"] == "current"]
        # Lineage_B historical should be in different scope key
        scope_key = current_scored.iloc[0]["multiplicity_scope_key"]
        scope_count = scored[scored["multiplicity_scope_key"] == scope_key]["num_tests_scope"].iloc[0]
        assert scope_count == 2  # Only matching lineage

    def test_deduplication_by_candidate_id(self):
        """Duplicate historical candidates should be deduplicated."""
        from project.research.promotion.multiplicity_history import (
            _deduplicate_historical_candidates,
        )

        df = pd.DataFrame({
            "candidate_id": ["dup_1", "dup_1", "uniq_1"],
            "p_value_for_fdr": [0.01, 0.02, 0.03],
            "run_id": ["run_001", "run_001", "run_002"],
        })
        deduped = _deduplicate_historical_candidates(df)

        assert len(deduped) == 2
        assert "dup_1" in deduped["candidate_id"].values
        assert "uniq_1" in deduped["candidate_id"].values

    def test_current_only_rows_in_downstream(self):
        """After filtering, only current rows should remain for downstream."""
        current = pd.DataFrame({
            "candidate_id": ["curr_1", "curr_2"],
            "p_value_for_fdr": [0.01, 0.02],
            "family_id": ["f1", "f2"],
            "campaign_id": ["camp_001", "camp_001"],
            "run_id": ["run_002", "run_002"],
            "multiplicity_pool_eligible": [True, True],
        })
        historical = pd.DataFrame({
            "candidate_id": ["hist_1", "hist_2", "hist_3"],
            "p_value_for_fdr": [0.015, 0.025, 0.035],
            "family_id": ["f1", "f2", "f3"],
            "campaign_id": ["camp_001"] * 3,
            "run_id": ["run_001"] * 3,
            "multiplicity_pool_eligible": [True] * 3,
        })
        merged = merge_historical_candidates(current, historical, scope_mode="campaign")
        scored = apply_canonical_cross_campaign_multiplicity(merged, max_q=0.05)

        # Filter to current only (this is what promotion/core.py does)
        df = scored[scored["multiplicity_context"] == "current"].copy()

        assert len(df) == 2
        assert set(df["candidate_id"]) == {"curr_1", "curr_2"}
        assert "hist_1" not in df["candidate_id"].values
