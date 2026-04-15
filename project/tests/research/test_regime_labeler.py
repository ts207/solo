from __future__ import annotations

import pandas as pd

from project.research.robustness.regime_labeler import label_regimes


def test_label_regimes_uses_search_prepared_alias_columns():
    features = pd.DataFrame(
        {
            "high_vol_regime": [1.0, 0.0],
            "low_vol_regime": [0.0, 1.0],
            "funding_positive": [1.0, 0.0],
            "funding_negative": [0.0, 1.0],
            "trending_state": [1.0, 0.0],
            "chop_state": [0.0, 1.0],
            "prob_spread_tight": [0.9, 0.1],
            "prob_spread_wide": [0.1, 0.9],
        }
    )

    labels = label_regimes(features)

    assert labels.tolist() == [
        "high_vol.funding_pos.trend.tight",
        "low_vol.funding_neg.chop.wide",
    ]
