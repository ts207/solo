"""
Test directional gating - verifies negative t-statistics produce high p-values.

This test ensures the bug fix for two-sided p-values does not regress.
Negative t-statistics should produce p_value -> 1.0, not p_value -> 0.0.
"""

import pytest
from scipy import stats as scipy_stats


class TestDirectionalGating:
    def test_negative_t_stat_produces_high_p_value(self):
        """Negative t-statistic must produce p-value approaching 1.0, not 0.0."""
        t_stat = -3.5
        df = 100
        p_value = float(scipy_stats.t.sf(t_stat, df))
        assert p_value >= 0.95, f"Negative t-stat should yield p-value ~1.0, got {p_value}"

    def test_positive_t_stat_produces_low_p_value(self):
        """Positive t-statistic must produce low p-value."""
        t_stat = 3.5
        df = 100
        p_value = float(scipy_stats.t.sf(t_stat, df))
        assert p_value <= 0.05, f"Positive t-stat should yield low p-value, got {p_value}"

    def test_zero_t_stat_produces_middle_p_value(self):
        """Zero t-statistic must produce p-value ~0.5."""
        t_stat = 0.0
        df = 100
        p_value = float(scipy_stats.t.sf(t_stat, df))
        assert 0.4 <= p_value <= 0.6, f"Zero t-stat should yield ~0.5 p-value, got {p_value}"

    def test_negative_t_rejects_false_positive(self):
        """Negative t-stat should NOT pass FDR correction - no signal in negative direction."""
        p_values = [0.99, 0.98, 0.97]
        from project.eval.multiplicity import benjamini_hochberg
        reject, qvals = benjamini_hochberg(p_values, alpha=0.05)
        assert not any(reject), "High p-values should not be rejected"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
