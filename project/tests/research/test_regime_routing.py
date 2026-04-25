from __future__ import annotations

import pandas as pd

from project.research.regime_routing import (
    annotate_regime_metadata,
    routing_entry_for_regime,
    validate_regime_routing_spec,
)


def test_regime_routing_spec_covers_all_executable_canonical_regimes():
    payload = validate_regime_routing_spec()
    assert payload["is_valid"] is True
    assert payload["missing_regimes"] == []
    assert payload["invalid_templates"] == {}
    assert payload["empty_intersection_regimes"] == []
    assert payload["eligible_templates_without_event_support"] == {}
    assert payload["events_without_supported_templates"] == {}
    assert payload["event_template_support"]["POSITIONING_EXPANSION"]["OI_SPIKE_POSITIVE"] == [
        "carry_continuation_confirmed",
        "squeeze_followthrough_confirmed",
    ]
    assert payload["event_template_support"]["REGIME_TRANSITION"]["VOL_REGIME_SHIFT_EVENT"] == [
        "continuation",
        "drawdown_filter",
        "only_if_regime",
    ]


def test_annotate_regime_metadata_adds_bucket_and_routing_profile():
    frame = pd.DataFrame(
        [
            {"event_type": "LIQUIDITY_STRESS_DIRECT"},
            {"event_type": "LIQUIDITY_STRESS_PROXY"},
            {"event_type": "SESSION_OPEN_EVENT"},
        ]
    )

    annotated = annotate_regime_metadata(frame)

    assert annotated.loc[0, "canonical_regime"] == "LIQUIDITY_STRESS"
    assert annotated.loc[0, "regime_bucket"] == "trade_generating"
    assert annotated.loc[0, "routing_profile_id"] == routing_entry_for_regime("LIQUIDITY_STRESS").routing_profile_id
    assert annotated.loc[1, "evidence_mode"] == "hybrid"
    assert annotated.loc[2, "recommended_bucket"] == "context_only"
