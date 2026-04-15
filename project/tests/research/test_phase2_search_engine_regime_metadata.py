import pandas as pd

from project.research.phase2_search_engine import _annotate_candidate_regime_metadata


def test_annotate_candidate_regime_metadata_adds_regime_columns():
    frame = pd.DataFrame(
        [
            {
                "event_type": "LIQUIDITY_GAP_PRINT",
                "candidate_id": "c1",
                "after_cost_expectancy": 5.0,
            }
        ]
    )

    annotated = _annotate_candidate_regime_metadata(frame)

    assert annotated.loc[0, "canonical_regime"] == "LIQUIDITY_STRESS"
    assert annotated.loc[0, "recommended_bucket"] == "trade_generating"
    assert annotated.loc[0, "regime_bucket"] == "trade_generating"
    assert annotated.loc[0, "routing_profile_id"]
