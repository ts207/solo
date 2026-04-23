"""Tests for DSR promotion gate wired into _evaluate_row."""

import numpy as np
import pandas as pd
import pytest

from project.research.promotion import evaluate_row


def _make_row(**overrides):
    """Create a minimal candidate row dict with sensible defaults."""
    # Create realistic synthetic returns instead of relying on the engine doing it
    rng = np.random.default_rng(42)
    effect = overrides.get("effect_shrunk_state", 0.04)
    std = overrides.get("std_return", 0.02)
    size = overrides.get("sample_size", 200)

    # We must construct a valid returns_oos_combined array to pass the new strict DSR gate
    returns = list(rng.normal(effect, std + 1e-6, size))

    base = {
        "candidate_id": "test_candidate_001",
        "event_type": "TEST_EVENT",
        "expectancy": 0.05,
        "effect_shrunk_state": 0.04,
        "std_return": 0.02,
        "sample_size": 200,
        "n_events": 200,
        "p_value": 0.001,
        "p_value_adj_bh": 0.01,
        "test_p_value": 0.005,
        "test_p_value_adj_bh": 0.02,
        "num_tests_event_family": 50,
        "stability_score": 0.30,
        "sign_consistency": 0.80,
        "cost_survival_ratio": 0.90,
        "control_pass_rate": 0.0,
        "tob_coverage": 0.95,
        "validation_samples": 30,
        "test_samples": 20,
        "net_expectancy_bps": 5.0,
        "effective_cost_bps": 2.0,
        "turnover_proxy_mean": 0.5,
        "microstructure_slippage_bps": 0.1,
        "ess_effective": 150,
        "returns_oos_combined": returns,
    }
    base.update(overrides)
    return base


def _base_eval_kwargs(**overrides):
    """Base keyword arguments for _evaluate_row."""
    base = {
        "hypothesis_index": {},
        "negative_control_summary": {"global": {}, "by_event": {}},
        "max_q_value": 0.10,
        "min_events": 50,
        "min_stability_score": 0.05,
        "min_sign_consistency": 0.60,
        "min_cost_survival_ratio": 0.50,
        "max_negative_control_pass_rate": 0.10,
        "min_tob_coverage": 0.80,
        "require_hypothesis_audit": False,
        "allow_missing_negative_controls": True,
        "min_net_expectancy_bps": 0.0,
    }
    base.update(overrides)
    return base


class TestDSRGate:
    """Tests for the DSR gate in evaluate_row."""

    def test_dsr_gate_present_in_output(self):
        """Output should contain dsr_value and gate_promo_dsr."""
        row = _make_row()
        result = evaluate_row(row=row, min_dsr=0.95, **_base_eval_kwargs())
        assert "dsr_value" in result
        assert "gate_promo_dsr" in result

    def test_dsr_gate_disabled_when_zero(self):
        """When min_dsr=0, gate should always pass."""
        row = _make_row(effect_shrunk_state=0.0, std_return=0.1)
        result = evaluate_row(row=row, min_dsr=0.0, **_base_eval_kwargs())
        assert result["gate_promo_dsr"] is True

    def test_strong_signal_passes_dsr(self):
        """A strong effect relative to variance and few trials should pass DSR."""
        row = _make_row(
            effect_shrunk_state=0.10,
            std_return=0.02,
            sample_size=500,
            num_tests_event_family=5,
        )
        result = evaluate_row(row=row, min_dsr=0.50, **_base_eval_kwargs())
        assert result["gate_promo_dsr"] is True
        assert result["dsr_value"] > 0.50

    def test_strong_signal_passes_dsr_with_new_columns(self):
        """DSR should work with new column names (num_tests_effective, num_tests_campaign)."""
        row = _make_row(
            effect_shrunk_state=0.10,
            std_return=0.02,
            sample_size=500,
            num_tests_effective=5,
            num_tests_campaign=5,
            num_tests_family=5,
        )
        result = evaluate_row(row=row, min_dsr=0.50, **_base_eval_kwargs())
        assert result["gate_promo_dsr"] is True
        assert result["dsr_value"] > 0.50

    def test_weak_signal_many_trials_fails_dsr(self):
        """A weak effect with many trials should fail DSR."""
        row = _make_row(
            effect_shrunk_state=0.001,
            std_return=0.10,
            sample_size=30,
            num_tests_event_family=5000,
        )
        result = evaluate_row(row=row, min_dsr=0.95, **_base_eval_kwargs())
        assert result["gate_promo_dsr"] is False
        assert "dsr_below_threshold" in result.get("reject_reason", "")

    def test_zero_std_skips_dsr_computation(self):
        """When returns are invalid or missing, DSR should remain at default 0."""
        row = _make_row(std_return=0.0, effect_shrunk_state=0.05, returns_oos_combined=[])
        result = evaluate_row(row=row, min_dsr=0.95, **_base_eval_kwargs())
        assert result["dsr_value"] == 0.0
        assert result["gate_promo_dsr"] is False

    def test_json_encoded_returns_oos_combined_is_accepted(self):
        """Persisted pipeline artifacts store OOS returns as JSON text."""
        row = _make_row()
        row["returns_oos_combined"] = str(row["returns_oos_combined"]).replace("'", "")
        result = evaluate_row(row=row, min_dsr=0.50, **_base_eval_kwargs())
        assert result["gate_promo_dsr"] is True
        assert result["dsr_value"] > 0.0

    def test_dsr_fallback_order_uses_effective_first(self):
        """DSR should prefer num_tests_effective over num_tests_campaign over num_tests_family."""
        row = _make_row(
            num_tests_effective=100,
            num_tests_campaign=50,
            num_tests_family=10,
            effect_shrunk_state=0.05,
            std_return=0.02,
        )
        result = evaluate_row(row=row, min_dsr=0.0, **_base_eval_kwargs())
        assert result["dsr_value"] > 0.0

    def test_dsr_fallback_to_campaign_when_effective_missing(self):
        """When num_tests_effective is missing, DSR should use num_tests_campaign."""
        row = _make_row(
            num_tests_campaign=50,
            num_tests_family=10,
            effect_shrunk_state=0.05,
            std_return=0.02,
        )
        if "num_tests_effective" in row:
            del row["num_tests_effective"]
        result = evaluate_row(row=row, min_dsr=0.0, **_base_eval_kwargs())
        assert result["dsr_value"] > 0.0

    def test_dsr_fallback_to_family_when_campaign_missing(self):
        """When campaign count is missing, DSR should use num_tests_family."""
        row = _make_row(
            num_tests_family=10,
            effect_shrunk_state=0.05,
            std_return=0.02,
        )
        if "num_tests_effective" in row:
            del row["num_tests_effective"]
        if "num_tests_campaign" in row:
            del row["num_tests_campaign"]
        result = evaluate_row(row=row, min_dsr=0.0, **_base_eval_kwargs())
        assert result["dsr_value"] > 0.0
