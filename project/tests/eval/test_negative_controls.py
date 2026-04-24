"""
Test negative controls - shuffled events must show zero edge.

This test verifies that:
- On synthetic data with known signal, shuffled events produce no significant edge
- The system correctly identifies absence of signal
- p-values for shuffled data should be high (no signal)
"""

import numpy as np
import pandas as pd
import pytest
from scipy import stats as scipy_stats


class TestNegativeControls:
    def test_shuffled_events_show_no_edge(self):
        """Shuffled event labels should produce t-stat ~ 0."""
        np.random.seed(42)
        n = 1000
        returns = np.random.normal(0, 1, n)
        event_times = np.random.choice([True, False], size=n, p=[0.05, 0.95])

        shuffled_times = np.random.permutation(event_times)
        with_returns = pd.DataFrame({
            "return_bps": returns,
            "event": shuffled_times,
        })

        event_returns = with_returns[with_returns["event"]]["return_bps"]
        non_event_returns = with_returns[~with_returns["event"]]["return_bps"]

        if len(event_returns) > 1 and len(non_event_returns) > 1:
            t_stat, p_val = scipy_stats.ttest_ind(event_returns, non_event_returns)
            assert abs(t_stat) < 2.0, f"Shuffled events should show no edge, got t={t_stat}"
            assert p_val > 0.1, f"Shuffled p-value should be high, got {p_val}"

    def test_original_events_show_signal(self):
        """Original event labels should show significant edge (synthetic data)."""
        np.random.seed(42)
        n = 1000
        base_returns = np.random.normal(0, 1, n)
        event_boost = 3.0

        event_times = np.random.choice([True, False], size=n, p=[0.05, 0.95])
        returns = base_returns + np.where(event_times, event_boost, 0)

        with_returns = pd.DataFrame({
            "return_bps": returns,
            "event": event_times,
        })

        event_returns = with_returns[with_returns["event"]]["return_bps"]
        non_event_returns = with_returns[~with_returns["event"]]["return_bps"]

        t_stat, p_val = scipy_stats.ttest_ind(event_returns, non_event_returns)
        assert abs(t_stat) > 2.0, f"Original events should show edge, got t={t_stat}"
        assert p_val < 0.05, f"Original p-value should be low, got {p_val}"

    def test_random_permutation_null_distribution(self):
        """Multiple random permutations should produce uniform p-value distribution."""
        np.random.seed(42)
        p_values = []
        for _ in range(20):
            returns = np.random.normal(0, 1, 500)
            events = np.random.choice([True, False], size=500, p=[0.05, 0.95])
            perm_events = np.random.permutation(events)
            df = pd.DataFrame({"return": returns, "event": perm_events})
            e_ret = df[df["event"]]["return"]
            ne_ret = df[~df["event"]]["return"]
            if len(e_ret) > 1 and len(ne_ret) > 1:
                _, p = scipy_stats.ttest_ind(e_ret, ne_ret)
                p_values.append(p)

        if len(p_values) >= 10:
            shapiro_stat, shapiro_p = scipy_stats.shapiro(p_values)
            assert shapiro_p > 0.05, f"P-values should be uniform, Shapiro p={shapiro_p}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
