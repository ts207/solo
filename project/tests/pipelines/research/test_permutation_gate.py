"""Tests for the permutation test gate."""

import numpy as np
import pytest

from project.research.permutation_gate import permutation_test_expectancy


class TestPermutationGate:
    """Tests for permutation_test_expectancy."""

    def test_strong_positive_signal_passes(self):
        """A strong positive-mean signal should pass the gate."""
        rng = np.random.default_rng(123)
        returns = rng.normal(loc=0.05, scale=0.02, size=200)
        result = permutation_test_expectancy(returns, n_permutations=500, rng_seed=99)
        assert result["gate_permutation"] is True
        assert result["observed_rank_pct"] >= 95.0
        assert result["permutation_n_returns"] == 200
        assert result["permutation_n_shuffles"] == 500

    def test_zero_mean_signal_fails(self):
        """A zero-mean signal should fail the gate (most of the time)."""
        rng = np.random.default_rng(456)
        returns = rng.normal(loc=0.0, scale=0.1, size=200)
        result = permutation_test_expectancy(returns, n_permutations=1000, rng_seed=42)
        assert result["gate_permutation"] is False
        assert result["observed_rank_pct"] < 95.0

    def test_too_few_returns_fails(self):
        """With fewer than 5 returns, the gate should fail gracefully."""
        result = permutation_test_expectancy(np.array([0.01, 0.02]), rng_seed=1)
        assert result["gate_permutation"] is False
        assert result["permutation_n_returns"] == 2
        assert result["permutation_n_shuffles"] == 0

    def test_nan_returns_filtered(self):
        """NaN values in returns should be filtered before testing."""
        returns = np.array([0.1, np.nan, 0.2, 0.15, np.nan, 0.3, 0.05, 0.12, 0.08, 0.11])
        result = permutation_test_expectancy(returns, n_permutations=200, rng_seed=7)
        assert result["permutation_n_returns"] == 8
        assert isinstance(result["gate_permutation"], bool)

    def test_custom_threshold(self):
        """A lower threshold should be easier to pass."""
        rng = np.random.default_rng(789)
        returns = rng.normal(loc=0.01, scale=0.1, size=100)
        result_strict = permutation_test_expectancy(
            returns, threshold_pct=99.0, n_permutations=500, rng_seed=42
        )
        result_lenient = permutation_test_expectancy(
            returns, threshold_pct=50.0, n_permutations=500, rng_seed=42
        )
        # Lenient should be easier to pass than strict
        assert result_lenient["observed_rank_pct"] == result_strict["observed_rank_pct"]
        # If strict fails, lenient may still pass (or at least not be worse)
        if not result_strict["gate_permutation"]:
            # The rank pct is the same; only the threshold differs
            assert result_lenient["gate_permutation"] or result_lenient["observed_rank_pct"] < 50.0

    def test_output_shape(self):
        """Return dict should have exactly 4 keys."""
        returns = np.array([0.1] * 20)
        result = permutation_test_expectancy(returns, n_permutations=10, rng_seed=1)
        assert set(result.keys()) == {
            "observed_rank_pct",
            "gate_permutation",
            "permutation_n_returns",
            "permutation_n_shuffles",
        }
