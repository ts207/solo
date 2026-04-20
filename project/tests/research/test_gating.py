from __future__ import annotations

import numpy as np
import pytest
from project.research.gating import one_sided_p_from_t, distribution_stats

def test_one_sided_p_from_t():
    # Large positive t-stat (strong winner) -> low p-value (rejected H0: mu <= 0)
    # n=100, df=99, t=3.26 -> p ~= 0.0007
    p_winner = one_sided_p_from_t(3.26, df=99)
    assert p_winner < 0.001
    
    # Large negative t-stat (strong loser) -> high p-value (cannot reject H0: mu <= 0)
    # n=100, df=99, t=-3.26 -> p ~= 1 - 0.0007 = 0.9993
    p_loser = one_sided_p_from_t(-3.26, df=99)
    assert p_loser > 0.999
    
    # t=0 -> p=0.5
    p_zero = one_sided_p_from_t(0.0, df=99)
    assert abs(p_zero - 0.5) < 1e-9

def test_one_sided_p_from_t_treats_negative_t_as_losing():
    # Large negative t-stat should give a high p-value (cannot reject H0: mu <= 0)
    p_loser = one_sided_p_from_t(-10.0, df=99)
    assert p_loser > 0.99

def test_distribution_stats_basic():
    # Strong winner: all positive returns
    returns = np.array([0.01, 0.02, 0.015, 0.025, 0.01])
    stats = distribution_stats(returns)
    assert stats["mean"] > 0
    assert stats["t_stat"] > 0
    assert stats["p_value"] < 0.05
    
    # Strong loser: all negative returns
    returns = np.array([-0.01, -0.02, -0.015, -0.025, -0.01])
    stats = distribution_stats(returns)
    assert stats["mean"] < 0
    assert stats["t_stat"] < 0
    assert stats["p_value"] > 0.95  # Directional gating rejects losers

def test_distribution_stats_edge_cases():
    # Single sample
    stats = distribution_stats(np.array([0.01]))
    assert stats["mean"] == 0.0
    assert stats["t_stat"] == 0.0
    assert stats["p_value"] == 1.0
    
    # Empty
    stats = distribution_stats(np.array([]))
    assert stats["mean"] == 0.0
    assert stats["t_stat"] == 0.0
    assert stats["p_value"] == 1.0
    
    # Zero variance
    stats = distribution_stats(np.array([0.01, 0.01, 0.01]))
    assert stats["mean"] == 0.01
    assert stats["t_stat"] == 0.0
    assert stats["p_value"] == 1.0
    
    # NaNs and Inf
    stats = distribution_stats(np.array([0.01, np.nan, 0.02, np.inf, -np.inf]))
    # After cleaning, should have [0.01, 0.02]
    assert stats["mean"] == 0.015
    assert stats["std"] > 0
    # For n=2, Newey-West might fail or use OLS fallback, but p-value should be reasonable.
    assert 0 < stats["p_value"] <= 1.0
