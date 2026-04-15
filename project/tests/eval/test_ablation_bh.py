"""
F-2: ablation must expose condition-significance BH outputs and must not
misstate them as lift-vs-baseline significance.
"""

from __future__ import annotations

import pandas as pd
import pytest

from project.eval.ablation import calculate_lift


def _make_group(conditions: list[dict]) -> pd.DataFrame:
    """Build a group_df as ablation.py would receive it."""
    return pd.DataFrame(conditions)


class TestAblationBH:
    def test_output_has_condition_q_value_column(self):
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "vol_regime_high",
                    "expectancy": 0.003,
                    "n_events": 50,
                    "p_value": 0.01,
                },
                {
                    "candidate_id": "c3",
                    "condition_key": "vol_regime_low",
                    "expectancy": 0.001,
                    "n_events": 50,
                    "p_value": 0.80,
                },
            ]
        )
        result = calculate_lift(group)
        assert "condition_q_value" in result.columns, (
            "condition_q_value column missing from ablation output"
        )
        assert "lift_q_value" in result.columns
        assert "lift_significance_available" in result.columns

    def test_output_has_is_condition_discovery_column(self):
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "vol_regime_high",
                    "expectancy": 0.003,
                    "n_events": 50,
                    "p_value": 0.01,
                },
            ]
        )
        result = calculate_lift(group)
        assert "is_condition_discovery" in result.columns
        assert "is_lift_discovery" in result.columns

    def test_significant_condition_is_flagged_but_not_as_lift_discovery(self):
        """A condition with p=0.001 should survive BH at alpha=0.10."""
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 200,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "vol_regime_high",
                    "expectancy": 0.005,
                    "n_events": 100,
                    "p_value": 0.001,
                },
                {
                    "candidate_id": "c3",
                    "condition_key": "vol_regime_low",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 0.90,
                },
            ]
        )
        result = calculate_lift(group)
        sig = result[result["condition"] == "vol_regime_high"]
        assert not sig.empty
        assert bool(sig.iloc[0]["is_condition_discovery"]) is True
        assert bool(sig.iloc[0]["is_lift_discovery"]) is False

    def test_noise_condition_is_not_flagged_as_discovery(self):
        """A condition with p=0.90 should NOT survive BH at alpha=0.10."""
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 200,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "vol_regime_low",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 0.90,
                },
            ]
        )
        result = calculate_lift(group)
        noise = result[result["condition"] == "vol_regime_low"]
        assert not noise.empty
        assert bool(noise.iloc[0]["is_condition_discovery"]) is False
        assert bool(noise.iloc[0]["is_lift_discovery"]) is False

    def test_bh_uses_alpha_010_threshold_for_condition_significance(self):
        """condition_q_value ≤ 0.10 iff is_condition_discovery is True."""
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 200,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "cond_a",
                    "expectancy": 0.005,
                    "n_events": 100,
                    "p_value": 0.02,
                },
                {
                    "candidate_id": "c3",
                    "condition_key": "cond_b",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 0.80,
                },
            ]
        )
        result = calculate_lift(group)
        for _, row in result.iterrows():
            assert pd.isna(row["lift_q_value"])
            assert bool(row["is_lift_discovery"]) is False
            if row["condition_q_value"] <= 0.10:
                assert (
                    row["is_condition_discovery"] is True or row["is_condition_discovery"] == True
                )
            else:
                assert (
                    row["is_condition_discovery"] is False or row["is_condition_discovery"] == False
                )

    def test_output_has_condition_p_value_raw_column(self):
        group = _make_group(
            [
                {
                    "candidate_id": "c1",
                    "condition_key": "all",
                    "expectancy": 0.001,
                    "n_events": 100,
                    "p_value": 1.0,
                },
                {
                    "candidate_id": "c2",
                    "condition_key": "cond_a",
                    "expectancy": 0.002,
                    "n_events": 40,
                    "p_value": 0.03,
                },
            ]
        )
        result = calculate_lift(group)
        assert "condition_p_value_raw" in result.columns
        assert float(result.iloc[0]["condition_p_value_raw"]) == pytest.approx(0.03)
        assert str(result.iloc[0]["condition_significance_basis"]) == "inherited_condition_p_value"

    def test_empty_result_when_no_baseline(self):
        """Group with no 'all' condition returns empty DataFrame (existing behavior preserved)."""
        group = _make_group(
            [
                {
                    "candidate_id": "c2",
                    "condition_key": "vol_regime_high",
                    "expectancy": 0.003,
                    "n_events": 50,
                    "p_value": 0.01,
                },
            ]
        )
        result = calculate_lift(group)
        assert result.empty
