"""Tests for the max drawdown gate."""

import numpy as np
import pytest

from project.research.gating import max_drawdown_gate


class TestMaxDrawdownGate:
    """Tests for max_drawdown_gate."""

    def test_low_drawdown_passes(self):
        """Consistent positive returns should have low DD ratio and pass."""
        returns = [0.01] * 50
        result = max_drawdown_gate(returns, max_dd_ratio=3.0)
        assert result["gate_max_drawdown"] is True
        assert result["max_drawdown"] == 0.0
        assert result["dd_to_expectancy_ratio"] == 0.0

    def test_high_drawdown_fails(self):
        """A large drawdown relative to mean should fail the gate."""
        # Mean ~0.001, but a big drop in the middle creates a large drawdown
        returns = [0.01] * 10 + [-0.5] + [0.01] * 10
        result = max_drawdown_gate(returns, max_dd_ratio=3.0)
        # The max drawdown should be ~0.5, mean ~0.004, ratio ~125
        assert result["gate_max_drawdown"] is False
        assert result["dd_to_expectancy_ratio"] > 3.0

    def test_empty_returns(self):
        """With fewer than 2 returns, gate passes by default."""
        result = max_drawdown_gate([0.1])
        assert result["gate_max_drawdown"] is True
        assert result["max_drawdown"] == 0.0

    def test_all_negative_returns(self):
        """All negative returns: drawdown equals total loss, ratio computed correctly."""
        returns = [-0.01] * 20
        result = max_drawdown_gate(returns, max_dd_ratio=3.0)
        # Include the initial zero-equity point so the first loss is counted.
        assert result["max_drawdown"] == pytest.approx(0.20)
        assert result["gate_max_drawdown"] is False

    def test_custom_max_dd_ratio(self):
        """With a very large max_dd_ratio, even high-DD strategies pass."""
        returns = [0.01] * 10 + [-0.5] + [0.01] * 10
        result = max_drawdown_gate(returns, max_dd_ratio=1000.0)
        assert result["gate_max_drawdown"] is True

    def test_nan_values_filtered(self):
        """NaN values should be filtered before computation."""
        returns = [0.01, np.nan, 0.02, 0.03, np.nan, 0.01, 0.02, 0.01, 0.03, 0.02]
        result = max_drawdown_gate(returns, max_dd_ratio=3.0)
        assert result["gate_max_drawdown"] is True
        assert result["max_drawdown"] == 0.0  # All positive, no drawdown
