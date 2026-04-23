"""
E4-T3: Expanded cost stress matrix (1×/2×/5×/10×).

evaluate_structural_robustness() must test at all four multipliers and return
results keyed by multiplier, so promotion artifacts show which stress levels a
candidate survives.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.eval.robustness import evaluate_structural_robustness


def _make_pnl(n: int = 500, mean_bps: float = 5.0, seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(mean_bps / 10000, 0.001, n))


def _make_costs(n: int = 500, cost_bps: float = 2.0, seed: int = 42) -> pd.Series:
    return pd.Series(np.full(n, cost_bps))


class TestCostStressMatrix:
    def test_returns_result_for_each_multiplier(self):
        """Output must contain a result for each of the four standard multipliers."""
        pnl = _make_pnl(mean_bps=10.0)
        costs = _make_costs(cost_bps=1.0)

        result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=costs,
        )

        for mult in (1, 2, 5, 10):
            key = f"cost_stress_{mult}x_pass"
            assert key in result, f"Missing key: {key}"
            assert isinstance(result[key], bool), f"{key} must be bool"

    def test_high_cost_multiple_fails_low_edge_candidate(self):
        """A candidate with thin edge must fail 10× cost stress but pass 1× cost stress."""
        # Edge = 3 bps, cost = 2 bps. After 10× cost stress: 3 - 20 = -17 bps → fail
        pnl = _make_pnl(mean_bps=3.0)
        costs = _make_costs(cost_bps=2.0)

        result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=costs,
        )

        assert result["cost_stress_1x_pass"], "1× stress should pass for thin edge"
        assert not result["cost_stress_10x_pass"], "10× stress should fail for thin edge"

    def test_robust_candidate_passes_all_multipliers(self):
        """A candidate with large edge relative to cost must pass all stress levels."""
        # Edge = 50 bps, cost = 0.5 bps. Even 10× cost = 5 bps << 50 bps edge
        pnl = _make_pnl(mean_bps=50.0)
        costs = _make_costs(cost_bps=0.5)

        result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=costs,
        )

        for mult in (1, 2, 5, 10):
            assert result[f"cost_stress_{mult}x_pass"], f"Robust candidate must pass {mult}× stress"

    def test_cost_stress_2x_pass_backward_compatible(self):
        """cost_stress_pass (the old 2× result) must still be present for backward compat."""
        pnl = _make_pnl(mean_bps=10.0)
        costs = _make_costs(cost_bps=1.0)

        result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=costs,
        )

        # The old key must still exist and equal cost_stress_2x_pass
        assert "cost_stress_pass" in result, "Legacy cost_stress_pass key must be preserved"
        assert result["cost_stress_pass"] == result["cost_stress_2x_pass"], (
            "cost_stress_pass must equal cost_stress_2x_pass for backward compat"
        )

    def test_custom_multipliers_respected(self):
        """If cost_stress_multipliers is passed explicitly, results are keyed by those values."""
        pnl = _make_pnl(mean_bps=10.0)
        costs = _make_costs(cost_bps=1.0)

        result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=costs,
            cost_stress_multipliers=(1.0, 3.0, 7.0),
        )

        assert "cost_stress_1x_pass" in result
        assert "cost_stress_3x_pass" in result
        assert "cost_stress_7x_pass" in result
        assert "cost_stress_5x_pass" not in result  # not requested

    def test_negative_costs_do_not_improve_stress_results(self):
        """Negative transaction costs must be clipped so stress cannot look better than zero cost."""
        pnl = _make_pnl(mean_bps=10.0)
        negative_costs = pd.Series(np.full(len(pnl), -5.0))
        zero_costs = pd.Series(np.zeros(len(pnl)))

        negative_result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=negative_costs,
        )
        zero_result = evaluate_structural_robustness(
            base_pnl=pnl,
            returns_raw=pnl,
            costs_bps=zero_costs,
        )

        for mult in (1, 2, 5, 10):
            key = f"cost_stress_{mult}x_retention"
            assert negative_result[key] == pytest.approx(zero_result[key], abs=1e-12)
            assert negative_result[f"cost_stress_{mult}x_pass"] == zero_result[f"cost_stress_{mult}x_pass"]
