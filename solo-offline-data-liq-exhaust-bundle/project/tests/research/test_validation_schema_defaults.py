"""Schema regression tests for mutable default values (Phase 2).

These tests verify that creating multiple model instances does not result in
shared mutable state due to improper default value handling.
"""

import pytest

from project.research.validation.schemas import (
    EvidenceBundle,
    FalsificationResult,
    PromotionDecision,
    SampleDefinition,
    SearchBurden,
    StabilityResult,
)


class TestMutableDefaultsIsolation:
    """Test that mutable defaults are properly isolated between instances."""

    def test_stability_result_details_isolated(self) -> None:
        """Mutating details on one StabilityResult should not affect another."""
        result1 = StabilityResult(
            sign_consistency=0.8,
            stability_score=0.7,
            regime_stability_pass=True,
            timeframe_consensus_pass=True,
            delay_robustness_pass=True,
        )
        result2 = StabilityResult(
            sign_consistency=0.6,
            stability_score=0.5,
            regime_stability_pass=False,
            timeframe_consensus_pass=False,
            delay_robustness_pass=False,
        )

        # Mutate result1's details
        result1.details["custom_key"] = "custom_value"
        result1.details["regime_analysis"] = {"trend": "up"}

        # Verify result2 is unaffected
        assert "custom_key" not in result2.details
        assert "regime_analysis" not in result2.details
        assert result2.details == {}

    def test_falsification_result_details_isolated(self) -> None:
        """Mutating details on one FalsificationResult should not affect another."""
        result1 = FalsificationResult(
            shift_placebo_pass=True,
            random_placebo_pass=True,
            direction_reversal_pass=True,
            negative_control_pass=True,
        )
        result2 = FalsificationResult(
            shift_placebo_pass=False,
            random_placebo_pass=False,
            direction_reversal_pass=False,
            negative_control_pass=False,
        )

        # Mutate result1's details
        result1.details["exceedance_details"] = {"count": 5}

        # Verify result2 is unaffected
        assert "exceedance_details" not in result2.details
        assert result2.details == {}

    def test_promotion_decision_lists_isolated(self) -> None:
        """Mutating rejection_reasons on one PromotionDecision should not affect another."""
        decision1 = PromotionDecision(
            eligible=False,
            promotion_status="rejected",
            promotion_track="fallback_only",
            rank_score=0.3,
            rejection_reasons=["stability", "cost"],
        )
        decision2 = PromotionDecision(
            eligible=True,
            promotion_status="promoted",
            promotion_track="standard",
            rank_score=0.8,
        )

        # Mutate decision1's rejection_reasons
        decision1.rejection_reasons.append("new_reason")

        # Verify decision2 is unaffected
        assert "new_reason" not in decision2.rejection_reasons
        assert decision2.rejection_reasons == []

    def test_promotion_decision_gate_results_isolated(self) -> None:
        """Mutating gate_results on one PromotionDecision should not affect another."""
        decision1 = PromotionDecision(
            eligible=False,
            promotion_status="rejected",
            promotion_track="fallback_only",
            rank_score=0.3,
            gate_results={"statistical": "pass", "stability": "fail"},
        )
        decision2 = PromotionDecision(
            eligible=True,
            promotion_status="promoted",
            promotion_track="standard",
            rank_score=0.8,
        )

        # Mutate decision1's gate_results
        decision1.gate_results["new_gate"] = "fail"

        # Verify decision2 is unaffected
        assert "new_gate" not in decision2.gate_results
        assert decision2.gate_results == {}

    def test_evidence_bundle_rejection_reasons_isolated(self) -> None:
        """Mutating rejection_reasons on one EvidenceBundle should not affect another."""
        from project.research.validation.schemas import (
            CostRobustness,
            EffectEstimates,
            EvidenceMetadata,
            MultiplicityAdjustment,
            SampleDefinition,
            SearchBurden,
            SplitDefinition,
            UncertaintyEstimates,
        )

        bundle1 = EvidenceBundle(
            candidate_id="c1",
            primary_event_id="e1",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r1",
            sample_definition=SampleDefinition(n_events=100),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.001, estimate_bps=10.0, stderr=0.0005, stderr_bps=5.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.002, ci_low_bps=0.0, ci_high_bps=20.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=100, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.5, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.8, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.8, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
            rejection_reasons=["reason1", "reason2"],
        )
        bundle2 = EvidenceBundle(
            candidate_id="c2",
            primary_event_id="e2",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r2",
            sample_definition=SampleDefinition(n_events=200),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.002, estimate_bps=20.0, stderr=0.001, stderr_bps=10.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.004, ci_low_bps=0.0, ci_high_bps=40.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=200, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.6, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.9, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.9, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
        )

        # Mutate bundle1's rejection_reasons
        bundle1.rejection_reasons.append("new_reason")

        # Verify bundle2 is unaffected
        assert "new_reason" not in bundle2.rejection_reasons
        assert bundle2.rejection_reasons == []

    def test_evidence_bundle_search_burden_isolated(self) -> None:
        """Mutating search_burden on one EvidenceBundle should not affect another."""
        from project.research.validation.schemas import (
            CostRobustness,
            EffectEstimates,
            EvidenceMetadata,
            MultiplicityAdjustment,
            SampleDefinition,
            SplitDefinition,
            UncertaintyEstimates,
        )

        bundle1 = EvidenceBundle(
            candidate_id="c1",
            primary_event_id="e1",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r1",
            sample_definition=SampleDefinition(n_events=100),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.001, estimate_bps=10.0, stderr=0.0005, stderr_bps=5.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.002, ci_low_bps=0.0, ci_high_bps=20.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=100, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.5, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.8, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.8, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
            search_burden=SearchBurden(search_candidates_generated=100),
        )
        bundle2 = EvidenceBundle(
            candidate_id="c2",
            primary_event_id="e2",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r2",
            sample_definition=SampleDefinition(n_events=200),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.002, estimate_bps=20.0, stderr=0.001, stderr_bps=10.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.004, ci_low_bps=0.0, ci_high_bps=40.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=200, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.6, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.9, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.9, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
        )

        # Mutate bundle1's search_burden
        bundle1.search_burden.search_candidates_generated = 999

        # Verify bundle2 is unaffected
        assert bundle2.search_burden.search_candidates_generated == 0

    def test_search_burden_default_isolated(self) -> None:
        """Two EvidenceBundles with default search_burden should have independent instances."""
        from project.research.validation.schemas import (
            CostRobustness,
            EffectEstimates,
            EvidenceMetadata,
            MultiplicityAdjustment,
            SampleDefinition,
            SplitDefinition,
            UncertaintyEstimates,
        )

        bundle1 = EvidenceBundle(
            candidate_id="c1",
            primary_event_id="e1",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r1",
            sample_definition=SampleDefinition(n_events=100),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.001, estimate_bps=10.0, stderr=0.0005, stderr_bps=5.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.002, ci_low_bps=0.0, ci_high_bps=20.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=100, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.5, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.8, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.8, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
            # Using default search_burden
        )
        bundle2 = EvidenceBundle(
            candidate_id="c2",
            primary_event_id="e2",
            event_family="FAM",
            event_type="VOL_SPIKE",
            run_id="r2",
            sample_definition=SampleDefinition(n_events=200),
            split_definition=SplitDefinition(),
            effect_estimates=EffectEstimates(
                estimate=0.002, estimate_bps=20.0, stderr=0.001, stderr_bps=10.0
            ),
            uncertainty_estimates=UncertaintyEstimates(
                ci_low=0.0, ci_high=0.004, ci_low_bps=0.0, ci_high_bps=40.0,
                p_value_raw=0.05, q_value=0.1, q_value_by=0.1, q_value_cluster=0.1,
                n_obs=200, n_clusters=1
            ),
            stability_tests={},
            falsification_results={},
            cost_robustness=CostRobustness(
                cost_survival_ratio=0.6, net_expectancy_bps=0.0, effective_cost_bps=0.0,
                turnover_proxy_mean=0.0, tob_coverage=0.9, tob_coverage_pass=True,
                stressed_cost_pass=True, retail_net_expectancy_pass=True,
                retail_cost_budget_pass=True, retail_turnover_pass=True
            ),
            multiplicity_adjustment=MultiplicityAdjustment(
                p_value_adj=0.05, p_value_adj_by=0.05, p_value_adj_holm=0.05,
                q_value_program=0.05, q_value_scope=0.05, effective_q_value=0.05
            ),
            metadata=EvidenceMetadata(
                tob_coverage=0.9, repeated_fold_consistency=0.0, structural_robustness_score=0.0
            ),
            # Using default search_burden
        )

        # Mutate bundle1's default search_burden
        bundle1.search_burden.search_candidates_generated = 500

        # Verify bundle2's search_burden is still at default
        assert bundle2.search_burden.search_candidates_generated == 0
