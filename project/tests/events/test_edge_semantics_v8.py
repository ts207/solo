from __future__ import annotations

import pandas as pd

from project.events.event_output_schema import DetectedEvent, normalize_event_output_frame
from project.events.polarity import (
    infer_semantics_from_event,
    normalize_event_side,
    side_to_order_side,
)
from project.events.shared import emit_event
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.compatibility import event_template_compatibility_verdict


def test_canonical_polarity_semantics_aliases_and_liquidation_mapping():
    assert normalize_event_side("shorts_liquidated") == "bullish"
    assert normalize_event_side("longs_liquidated") == "bearish"
    assert side_to_order_side("bullish") == "buy"
    assert side_to_order_side("bearish") == "sell"
    assert infer_semantics_from_event("BASIS_DISLOC", "STATISTICAL_DISLOCATION", "", "trigger") == "basis_spread_direction"
    assert infer_semantics_from_event("LIQUIDATION_CASCADE", "FORCED_FLOW_AND_EXHAUSTION", "", "trigger") == "liquidation_side"


def test_emit_event_carries_edge_semantics_contract():
    row = emit_event(
        event_type="LIQUIDATION_CASCADE",
        symbol="BTCUSDT",
        event_id="liq_1",
        eval_bar_ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        event_side="longs_liquidated",
        magnitude=42.0,
        polarity_semantics="liquidation_side",
        polarity_source="cascade_side",
        magnitude_source="liquidation_notional",
        anchor_role="alpha_anchor",
    )
    assert row["event_side"] == "bearish"
    assert row["event_direction"] == -1
    assert row["magnitude"] == 42.0
    assert row["polarity_semantics"] == "liquidation_side"
    assert row["polarity_source"] == "cascade_side"
    assert row["magnitude_source"] == "liquidation_notional"
    assert row["anchor_role"] == "alpha_anchor"


def test_detected_event_and_frame_normalization_include_semantics():
    event = DetectedEvent(
        event_name="BAND_BREAK",
        event_version="v2",
        detector_class="BandBreakDetector",
        symbol="BTCUSDT",
        timeframe="5m",
        ts_start=pd.Timestamp("2024-01-01T00:00:00Z"),
        ts_end=pd.Timestamp("2024-01-01T00:00:00Z"),
        canonical_family="STATISTICAL_DISLOCATION",
        subtype="band_break",
        phase="trigger",
        evidence_mode="statistical",
        role="trigger",
        confidence=0.8,
        severity=0.7,
        event_side="up",
        event_direction=0,
        magnitude=2.5,
        severity_bucket="high",
        polarity_semantics="deviation_direction",
        polarity_source="zscore",
        magnitude_source="zscore",
        anchor_role="alpha_anchor",
        trigger_value=1.0,
        threshold_snapshot={},
        source_features={},
        detector_metadata={},
        required_context_present=True,
        data_quality_flag="ok",
        merge_key=None,
        cooldown_until=None,
    )
    assert event.event_side == "bullish"
    assert event.event_direction == 1
    frame = normalize_event_output_frame(pd.DataFrame([event.as_dict()]))
    assert frame.loc[0, "polarity_semantics"] == "deviation_direction"
    assert frame.loc[0, "anchor_role"] == "alpha_anchor"


def test_semantics_aware_compatibility_blocks_basis_as_price_template():
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("BASIS_DISLOC"),
        direction="long",
        horizon="15m",
        template_id="trend_continuation",
        context=None,
    )
    verdict = event_template_compatibility_verdict(spec)
    assert verdict.status == "forbidden"
    assert "basis_side_is_not_price_side" in verdict.reason_codes


def test_semantics_aware_compatibility_allows_basis_convergence_family():
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("BASIS_DISLOC"),
        direction="long",
        horizon="15m",
        template_id="basis_convergence",
        context=None,
    )
    verdict = event_template_compatibility_verdict(spec)
    assert verdict.status in {"allowed", "allowed_with_required_context", "research_only"}
    assert verdict.polarity_semantics == "basis_spread_direction"
