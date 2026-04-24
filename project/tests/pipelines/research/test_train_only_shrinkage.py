"""Tests for train-only shrinkage lambda estimation."""

import numpy as np
import pandas as pd

from project.research.helpers.shrinkage import _apply_hierarchical_shrinkage


def _build_test_df(
    n_train=100, n_val=30, n_test=20, effect_train=0.05, effect_val=0.10, effect_test=0.15
):
    """Build a synthetic candidate DataFrame with split labels."""
    rng = np.random.default_rng(42)
    rows = []
    for i, (split, n, effect) in enumerate(
        [
            ("train", n_train, effect_train),
            ("validation", n_val, effect_val),
            ("test", n_test, effect_test),
        ]
    ):
        for j in range(n):
            rows.append(
                {
                    "canonical_family": "FAM_A",
                    "canonical_event_type": "EVT_X" if j % 2 == 0 else "EVT_Y",
                    "template_verb": "buy",
                    "horizon": "4h",
                    "symbol": "BTCUSDT",
                    "state_id": "S1",
                    "expectancy": float(effect + rng.normal(0, 0.02)),
                    "p_value": float(rng.uniform(0.001, 0.1)),
                    "effective_sample_size": 50,
                    "n_events": 50,
                    "std_return": 0.03,
                    "split_label": split,
                }
            )
    return pd.DataFrame(rows)


class TestTrainOnlyShrinkage:
    """Tests for train_only_lambda parameter in hierarchical shrinkage."""

    def test_train_only_produces_different_lambdas(self):
        """When train and holdout effects differ, train_only_lambda should produce
        different lambdas compared to using all data."""
        df = _build_test_df(effect_train=0.02, effect_val=0.20, effect_test=0.30)

        result_all = _apply_hierarchical_shrinkage(
            df.copy(),
            adaptive_lambda=True,
            train_only_lambda=False,
        )
        result_train = _apply_hierarchical_shrinkage(
            df.copy(),
            adaptive_lambda=True,
            train_only_lambda=True,
            split_col="split_label",
        )

        # Both should have the output columns
        for col in ["effect_shrunk_state", "lambda_family", "lambda_event"]:
            assert col in result_all.columns
            assert col in result_train.columns

        # Shrunk effects should differ because lambdas are estimated from different data
        # (This tests the core mechanism — lambdas estimated from train-only should
        # produce different shrinkage vs lambdas estimated from all data.)
        all_shrunk = result_all["effect_shrunk_state"].values
        train_shrunk = result_train["effect_shrunk_state"].values
        # They should not be identical
        assert not np.allclose(all_shrunk, train_shrunk, atol=1e-6), (
            "Train-only and all-data shrinkage should produce different results when splits have different effects"
        )

    def test_train_only_all_rows_get_shrunk(self):
        """Even with train_only_lambda, all rows (incl. val/test) should be shrunk."""
        df = _build_test_df()
        result = _apply_hierarchical_shrinkage(
            df, adaptive_lambda=True, train_only_lambda=True, split_col="split_label"
        )
        # All rows should have non-NaN shrunk values
        assert result["effect_shrunk_state"].notna().all()
        assert len(result) == len(df)

    def test_fallback_when_no_train_rows(self):
        """If no train rows exist, should fall back to using all data."""
        df = _build_test_df()
        df["split_label"] = "validation"  # No train rows
        result = _apply_hierarchical_shrinkage(
            df, adaptive_lambda=True, train_only_lambda=True, split_col="split_label"
        )
        assert result["effect_shrunk_state"].notna().all()

    def test_disabled_train_only_uses_all_data(self):
        """When train_only_lambda=False, result should be identical regardless of split_col."""
        df = _build_test_df()
        result_no_col = _apply_hierarchical_shrinkage(
            df.copy(), adaptive_lambda=True, train_only_lambda=False
        )
        result_with_col = _apply_hierarchical_shrinkage(
            df.copy(), adaptive_lambda=True, train_only_lambda=False, split_col="split_label"
        )
        np.testing.assert_array_almost_equal(
            result_no_col["effect_shrunk_state"].values,
            result_with_col["effect_shrunk_state"].values,
        )

    def test_empty_df_handled(self):
        """Empty DataFrame should return without error."""
        df = pd.DataFrame()
        result = _apply_hierarchical_shrinkage(df, train_only_lambda=True, split_col="split_label")
        assert result.empty

    def test_confirmatory_uses_aggregate_train_counts_without_split_labels(self, caplog):
        df = pd.DataFrame(
            [
                {
                    "canonical_family": "FAM_A",
                    "canonical_event_type": "EVT_X",
                    "template_verb": "buy",
                    "horizon": "4h",
                    "symbol": "BTCUSDT",
                    "state_id": "S1",
                    "expectancy": 0.05,
                    "p_value": 0.01,
                    "effective_sample_size": 100,
                    "n_events": 100,
                    "std_return": 0.03,
                    "train_n_obs": 60,
                    "validation_n_obs": 25,
                    "test_n_obs": 15,
                },
                {
                    "canonical_family": "FAM_A",
                    "canonical_event_type": "EVT_Y",
                    "template_verb": "buy",
                    "horizon": "4h",
                    "symbol": "BTCUSDT",
                    "state_id": "S1",
                    "expectancy": 0.02,
                    "p_value": 0.03,
                    "effective_sample_size": 80,
                    "n_events": 80,
                    "std_return": 0.02,
                    "train_n_obs": 50,
                    "validation_n_obs": 20,
                    "test_n_obs": 10,
                },
            ]
        )

        with caplog.at_level("WARNING"):
            result = _apply_hierarchical_shrinkage(
                df,
                adaptive_lambda=True,
                train_only_lambda=True,
                split_col="split_label",
                run_mode="production",
            )

        assert result["effect_shrunk_state"].notna().all()
        assert set(result["shrinkage_scope"]) == {"train_only_aggregate_counts"}
        assert "Confirmatory mode requested but split_col is missing" not in caplog.text
