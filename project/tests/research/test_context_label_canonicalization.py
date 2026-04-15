from __future__ import annotations

import pandas as pd

from project.research.search.evaluator_utils import context_mask


def test_context_mask_accepts_legacy_carry_state_aliases() -> None:
    features = pd.DataFrame(
        {
            "funding_positive": [1.0, 0.0],
            "funding_negative": [0.0, 1.0],
        }
    )

    positive_mask = context_mask({"carry_state": "positive"}, features, use_context_quality=False)
    negative_mask = context_mask({"carry_state": "negative"}, features, use_context_quality=False)

    assert positive_mask is not None
    assert negative_mask is not None
    assert positive_mask.tolist() == [True, False]
    assert negative_mask.tolist() == [False, True]
