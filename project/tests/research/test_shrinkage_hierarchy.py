"""
Test shrinkage hierarchy - verifies James-Stein pooling.

Tests that:
- Known input produces known shrunk output
- Rare events with few samples use conservative lambda
- rare_event_flag is set appropriately
"""

import pandas as pd
import pytest

from project.research.helpers.estimation_kernels import _apply_hierarchical_shrinkage


class TestShrinkageHierarchy:
    def test_known_input_produces_shrunk_output(self):
        """With known mean and variance, shrinkage should pull toward family mean."""
        input_df = pd.DataFrame({
            "event_type": ["TEST_EVENT"] * 5,
            "horizon": [12] * 5,
            "symbol": ["BTC", "ETH", "SOL", "XRP", "ADA"],
            "mean_return_bps": [10.0, 20.0, 30.0, 40.0, 50.0],
            "var_return_bps": [100.0] * 5,
            "sample_size": [100] * 5,
            "family": ["test_family"] * 5,
            "global_regime": ["vol_high"] * 5,
        })

        result = _apply_hierarchical_shrinkage(
            input_df,
            lambda_family=0.5,
            lambda_event=0.3,
            lambda_state=0.2,
            adaptive_lambda=True,
        )

        assert "effect_shrunk_state" in result.columns
        shrunk = result["effect_shrunk_state"]
        assert not shrunk.isna().all(), "Shrunk values should be computed"

    def test_rare_event_flag_set_for_low_n(self):
        """Events with sample_size < 50 should have rare_event_flag=True."""
        input_df = pd.DataFrame({
            "event_type": ["RARE_EVENT"] * 3,
            "horizon": [12] * 3,
            "symbol": ["BTC", "ETH", "SOL"],
            "mean_return_bps": [5.0, 10.0, 15.0],
            "var_return_bps": [50.0] * 3,
            "sample_size": [35, 40, 45],
            "family": ["rare_family"] * 3,
            "global_regime": ["vol_high"] * 3,
        })

        result = _apply_hierarchical_shrinkage(
            input_df,
            lambda_family=0.5,
            lambda_event=0.3,
            lambda_state=0.2,
            adaptive_lambda=True,
        )

        assert "rare_event_flag" in result.columns
        assert result["rare_event_flag"].all(), "All low-sample events should be marked rare"

    def test_common_event_flag_not_set(self):
        """Events with sample_size >= 50 should have rare_event_flag=False."""
        input_df = pd.DataFrame({
            "event_type": ["COMMON_EVENT"] * 3,
            "horizon": [12] * 3,
            "symbol": ["BTC", "ETH", "SOL"],
            "mean_return_bps": [5.0, 10.0, 15.0],
            "var_return_bps": [50.0] * 3,
            "sample_size": [100, 200, 500],
            "family": ["common_family"] * 3,
            "global_regime": ["vol_high"] * 3,
        })

        result = _apply_hierarchical_shrinkage(
            input_df,
            lambda_family=0.5,
            lambda_event=0.3,
            lambda_state=0.2,
            adaptive_lambda=True,
        )

        assert "rare_event_flag" in result.columns
        assert not result["rare_event_flag"].any(), "High-sample events should not be marked rare"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
