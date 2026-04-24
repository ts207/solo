from __future__ import annotations

import sys
import types

import numpy as np
import pandas as pd

import project.core.stats as core_stats


def test_cointegration_uses_statsmodels_pvalue_when_available(monkeypatch):
    statsmodels_mod = types.ModuleType("statsmodels")
    tsa_mod = types.ModuleType("statsmodels.tsa")
    stattools_mod = types.ModuleType("statsmodels.tsa.stattools")

    def fake_coint(x, y):
        return -2.0, 0.5553, [-3.9, -3.3, -3.0]

    stattools_mod.coint = fake_coint
    tsa_mod.stattools = stattools_mod
    statsmodels_mod.tsa = tsa_mod

    monkeypatch.setitem(sys.modules, "statsmodels", statsmodels_mod)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa", tsa_mod)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa.stattools", stattools_mod)

    x = pd.Series(range(100), dtype=float)
    y = x + 0.5

    assert core_stats.test_cointegration(x, y) == 0.5553


class TestBHAdjustWithExplicitNTests:
    def test_bh_adjust_default_uses_array_length(self):
        p_vals = np.array([0.01, 0.04, 0.03])
        adj = core_stats.bh_adjust(p_vals)
        assert np.isclose(adj[0], 0.03)
        assert np.isclose(adj[1], 0.04)
        assert np.isclose(adj[2], 0.04)

    def test_bh_adjust_with_explicit_n_tests_larger_is_more_conservative(self):
        p_vals = np.array([0.01, 0.04, 0.03])
        adj_default = core_stats.bh_adjust(p_vals)
        adj_larger = core_stats.bh_adjust(p_vals, n_tests=6)
        assert adj_larger[0] > adj_default[0], "Larger n_tests should be more conservative"
        assert adj_larger[1] > adj_default[1], "Larger n_tests should be more conservative"
        assert np.isclose(adj_larger[0], 0.06)
        assert np.isclose(adj_larger[1], 0.08)
        assert np.isclose(adj_larger[2], 0.08)

    def test_bh_adjust_with_explicit_n_tests_smaller_is_less_conservative(self):
        p_vals = np.array([0.01, 0.04, 0.03])
        adj_default = core_stats.bh_adjust(p_vals)
        adj_smaller = core_stats.bh_adjust(p_vals, n_tests=1)
        assert adj_smaller[0] < adj_default[0], "Smaller n_tests should be less conservative"
        assert np.isclose(adj_smaller[0], 0.01)
        assert np.isclose(adj_smaller[2], 0.01333, atol=1e-5)

    def test_bh_adjust_empty_array(self):
        adj = core_stats.bh_adjust(np.array([]))
        assert len(adj) == 0

    def test_bh_adjust_empty_array_with_n_tests(self):
        adj = core_stats.bh_adjust(np.array([]), n_tests=5)
        assert len(adj) == 0

    def test_bh_adjust_two_sided_interpretation(self):
        p_vals = np.array([0.01, 0.02])
        adj_default = core_stats.bh_adjust(p_vals)
        adj_doubled = core_stats.bh_adjust(p_vals, n_tests=4)
        assert adj_doubled[0] > adj_default[0], "More tests should yield larger (more conservative) q-values"
        assert adj_doubled[1] > adj_default[1], "More tests should yield larger (more conservative) q-values"

    def test_bh_adjust_monotonic_conservatism(self):
        p_vals = np.array([0.01, 0.02, 0.03, 0.04, 0.05])
        baseline = core_stats.bh_adjust(p_vals, n_tests=5)
        larger = core_stats.bh_adjust(p_vals, n_tests=10)
        for i in range(len(p_vals)):
            assert larger[i] >= baseline[i], f"Larger n_tests must yield >= baseline at index {i}"
