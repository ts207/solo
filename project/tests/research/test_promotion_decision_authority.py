"""Tests for promotion decision authority inversion (Phase 2).

These tests verify that evaluate_row() uses the bundle decision as the
authoritative source for final promotion outcome fields, not the row-level
assembly logic.
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from project.core.exceptions import PromotionDecisionError
from project.research.promotion.promotion_decisions import (
    _apply_authoritative_bundle_decision,
    evaluate_row,
)

# Minimal valid row for testing
MINIMAL_VALID_ROW: Dict[str, Any] = {
    "candidate_id": "test_cand_001",
    "event_type": "VOL_SPIKE",
    "run_id": "test_run_001",
    "primary_event_id": "event_001",
    "event_family": "VOLATILITY",
    "n_events": 100,
    "q_value": 0.05,
    "sample_size": 100,
    "symbol": "BTCUSDT",
    "estimate": 0.001,
    "estimate_bps": 10.0,
    "stderr": 0.0005,
    "stderr_bps": 5.0,
    "ci_low": 0.0,
    "ci_high": 0.002,
    "ci_low_bps": 0.0,
    "ci_high_bps": 20.0,
    "p_value_raw": 0.05,
    "q_value_by": 0.1,
    "q_value_cluster": 0.1,
    "q_value_program": 0.1,
    "q_value_scope": 0.1,
    "effective_q_value": 0.05,
    "n_obs": 100,
    "n_clusters": 1,
    "stability_score": 0.8,
    "sign_consistency": 0.7,
    "regime_stability_pass": True,
    "timeframe_consensus_pass": True,
    "delay_robustness_pass": True,
    "cost_survival_ratio": 0.5,
    "net_expectancy_bps": 5.0,
    "effective_cost_bps": 5.0,
    "turnover_proxy_mean": 0.1,
    "tob_coverage": 0.8,
    "gate_promo_tob_coverage": True,
    "gate_after_cost_stressed_positive": True,
    "gate_promo_retail_net_expectancy": True,
    "gate_promo_retail_cost_budget": True,
    "gate_promo_retail_turnover": True,
    "control_pass_rate": 0.0,
    "negative_control_pass": True,
    "passes_control": True,
    "shift_placebo_pass": True,
    "random_placebo_pass": True,
    "direction_reversal_pass": True,
    "plan_row_id": "plan_001",
}


# Stub bundle decision for rejection
REJECTED_BUNDLE_DECISION: Dict[str, Any] = {
    "eligible": False,
    "promotion_status": "rejected",
    "promotion_track": "fallback_only",
    "rank_score": 0.25,
    "rejection_reasons": ["stability", "oos_validation"],
    "gate_results": {
        "statistical": "pass",
        "stability": "fail",
        "oos_validation": "fail",
    },
}


# Stub bundle decision for promotion
PROMOTED_BUNDLE_DECISION: Dict[str, Any] = {
    "eligible": True,
    "promotion_status": "promoted",
    "promotion_track": "standard",
    "rank_score": 0.85,
    "rejection_reasons": [],
    "gate_results": {
        "statistical": "pass",
        "stability": "pass",
        "cost_survival": "pass",
        "falsification": "pass",
    },
}


class TestPromotionDecisionAuthority:
    """Test that bundle decision is authoritative for final outcome fields."""

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_rejected_status_comes_from_bundle_decision(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Final status fields must come from bundle decision, not row assembly."""
        mock_evaluate.return_value = REJECTED_BUNDLE_DECISION.copy()

        result = evaluate_row(
            row=MINIMAL_VALID_ROW.copy(),
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        # Verify bundle decision fields are authoritative
        assert result["promotion_status"] == "rejected"
        assert result["promotion_decision"] == "rejected"
        assert result["promotion_track"] == "fallback_only"
        assert result["rank_score"] == 0.25
        assert result["rejection_reasons"] == ["stability", "oos_validation"]
        assert {"stability", "oos_validation"}.issubset(set(result["reject_reason"].split("|")))
        assert result["gate_results"]["stability"] == "fail"
        assert result["gate_results"]["oos_validation"] == "fail"
        assert result["eligible"] is False

        mock_validate.assert_called_once()
        mock_evaluate.assert_called_once()

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_promoted_status_comes_from_bundle_decision(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Final eligible/promoted path must come from bundle decision."""
        mock_evaluate.return_value = PROMOTED_BUNDLE_DECISION.copy()

        result = evaluate_row(
            row=MINIMAL_VALID_ROW.copy(),
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        # Verify promotion fields come from bundle
        assert result["eligible"] is True
        assert result["promotion_status"] == "promoted"
        assert result["promotion_track"] == "standard"
        assert result["rank_score"] == 0.85
        assert result["rejection_reasons"] == []
        assert isinstance(result["reject_reason"], str)

        mock_validate.assert_called_once()
        mock_evaluate.assert_called_once()

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.build_evidence_bundle")
    def test_malformed_bundle_fails_before_decision(
        self,
        mock_build: MagicMock,
        mock_evaluate: MagicMock,
    ) -> None:
        """Malformed bundle must fail during validation before decision evaluation."""
        # Return an invalid bundle (missing required field)
        invalid_bundle = {
            "candidate_id": "test",
            "event_type": "VOL_SPIKE",
            "run_id": "r1",
            "primary_event_id": "e1",
            "event_family": "FAM",
            "sample_definition": {"n_events": -1},
        }
        mock_build.return_value = invalid_bundle

        with pytest.raises(PromotionDecisionError):
            evaluate_row(
                row=MINIMAL_VALID_ROW.copy(),
                hypothesis_index={},
                negative_control_summary={},
                max_q_value=0.1,
                min_events=50,
                min_stability_score=0.5,
                min_sign_consistency=0.5,
                min_cost_survival_ratio=0.3,
                max_negative_control_pass_rate=0.01,
                min_tob_coverage=0.5,
                require_hypothesis_audit=False,
                allow_missing_negative_controls=True,
            )

        # Verify evaluate_promotion_bundle was never called due to validation failure
        mock_evaluate.assert_not_called()

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_bundle_gate_results_are_authoritative(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Gate results in output must exactly match bundle decision gate_results."""
        custom_decision = REJECTED_BUNDLE_DECISION.copy()
        custom_decision["gate_results"] = {
            "statistical": "pass",
            "stability": "fail",
            "cost_survival": "pass",
            "falsification": "missing_evidence",
            "custom_gate": "fail",
        }
        mock_evaluate.return_value = custom_decision

        result = evaluate_row(
            row=MINIMAL_VALID_ROW.copy(),
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        # Verify gate_results is exactly what bundle returned
        assert result["gate_results"] == custom_decision["gate_results"]

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_row_level_calculations_preserved_for_diagnostics(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Row-level diagnostic fields should still be present for debugging."""
        mock_evaluate.return_value = REJECTED_BUNDLE_DECISION.copy()

        result = evaluate_row(
            row=MINIMAL_VALID_ROW.copy(),
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        # Row-level calculated fields should still be present
        assert "stability_score" in result or "ss" in result
        assert "sign_consistency" in result or "sc" in result
        assert "cost_survival_ratio" in result or "csr" in result

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_cell_origin_governance_overrides_forged_bundle_promotion(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """A cell-origin row cannot promote without representative/forward/contrast/mapping proof."""
        mock_evaluate.return_value = PROMOTED_BUNDLE_DECISION.copy()
        row = MINIMAL_VALID_ROW.copy()
        row.update(
            {
                "source_discovery_mode": "edge_cells",
                "source_cell_id": "cell_001",
                "is_representative": False,
                "forward_pass": False,
                "contrast_pass": False,
                "runtime_executable": False,
                "context_translation": "",
                "context_dimension_count": 1,
            }
        )

        result = evaluate_row(
            row=row,
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        assert result["promotion_decision"] == "rejected"
        assert result["eligible"] is False
        assert result["gate_promo_cell_origin"] == "fail"
        assert "cell_origin_not_cluster_representative" in result["reject_reason"]
        assert "cell_origin_forward_missing" in result["reject_reason"]
        assert "cell_origin_contrast_missing" in result["reject_reason"]
        assert "cell_origin_runtime_mapping_missing" in result["reject_reason"]

    @patch("project.research.promotion.promotion_decisions.evaluate_promotion_bundle")
    @patch("project.research.promotion.promotion_decisions.validate_evidence_bundle")
    def test_cell_origin_governance_accepts_explicit_mapped_representative(
        self, mock_validate: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        mock_evaluate.return_value = PROMOTED_BUNDLE_DECISION.copy()
        row = MINIMAL_VALID_ROW.copy()
        row.update(
            {
                "source_discovery_mode": "edge_cells",
                "source_cell_id": "cell_001",
                "is_representative": True,
                "forward_pass": True,
                "contrast_pass": True,
                "runtime_executable": False,
                "context_translation": "supportive_only_context_downgraded",
                "supportive_context": {"canonical_regime": "VOLATILITY"},
                "context_dimension_count": 1,
            }
        )

        result = evaluate_row(
            row=row,
            hypothesis_index={},
            negative_control_summary={},
            max_q_value=0.1,
            min_events=50,
            min_stability_score=0.5,
            min_sign_consistency=0.5,
            min_cost_survival_ratio=0.3,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.5,
            require_hypothesis_audit=False,
            allow_missing_negative_controls=True,
        )

        assert result["promotion_decision"] == "promoted"
        assert result["gate_promo_cell_origin"] == "pass"
        assert result["cell_origin_pass"] is True
        assert result["cell_origin_gate_reasons"] == ""


class TestApplyAuthoritativeBundleDecision:
    """Direct unit tests for _apply_authoritative_bundle_decision helper."""

    def test_helper_overrides_contradictory_row_values(self) -> None:
        """Bundle decision fields must override contradictory row-level values."""
        # Row result with contradictory promoted status
        row_result: Dict[str, Any] = {
            "eligible": True,
            "promotion_status": "promoted",
            "promotion_decision": "promoted",
            "promotion_track": "standard",
            "rank_score": 0.95,
            "rejection_reasons": [],
            "reject_reason": "",
            "gate_results": {"statistical": "pass", "stability": "pass"},
        }

        # Bundle decision says rejected
        bundle_decision: Dict[str, Any] = {
            "eligible": False,
            "promotion_status": "rejected",
            "promotion_track": "fallback_only",
            "rank_score": 0.25,
            "rejection_reasons": ["stability", "oos_validation"],
            "gate_results": {
                "statistical": "pass",
                "stability": "fail",
                "oos_validation": "fail",
            },
        }

        result = _apply_authoritative_bundle_decision(row_result, bundle_decision)

        # Verify bundle values are authoritative
        assert result["eligible"] is False
        assert result["promotion_status"] == "rejected"
        assert result["promotion_decision"] == "rejected"
        assert result["promotion_track"] == "fallback_only"
        assert result["rank_score"] == 0.25
        assert result["rejection_reasons"] == ["stability", "oos_validation"]
        assert set(result["reject_reason"].split("|")) == {"stability", "oos_validation"}
        assert result["gate_results"] == bundle_decision["gate_results"]

    def test_helper_preserves_non_conflict_fields(self) -> None:
        """Non-conflicting fields from row result should be preserved."""
        row_result: Dict[str, Any] = {
            "eligible": False,
            "promotion_status": "rejected",
            "promotion_track": "fallback_only",
            "rank_score": 0.3,
            "rejection_reasons": ["cost"],
            "custom_diagnostic": "preserved_value",
            "stability_score": 0.5,
        }

        bundle_decision: Dict[str, Any] = {
            "eligible": False,
            "promotion_status": "rejected",
            "promotion_track": "fallback_only",
            "rank_score": 0.3,
            "rejection_reasons": ["cost"],
            "gate_results": {"cost_survival": "fail"},
        }

        result = _apply_authoritative_bundle_decision(row_result, bundle_decision)

        # Bundle-decided fields
        assert result["eligible"] is False
        assert result["gate_results"] == {"cost_survival": "fail"}

        # Non-conflicting row fields preserved
        assert result["custom_diagnostic"] == "preserved_value"
        assert result["stability_score"] == 0.5
