from __future__ import annotations

import pytest

from project.research.models import (
    CandidateRecord,
    HypothesisRecord,
    PromotionRecord,
    ResearchDecisionTrace,
    ValidationRecord,
)


def test_hypothesis_record_is_frozen():
    h = HypothesisRecord(
        hypothesis_id="h1",
        event_type="VOL_SPIKE",
        template_id="mean_reversion",
        direction="long",
        horizon="24b",
        entry_lag_bars=1,
        symbol="BTCUSDT",
        run_id="run_001",
    )
    assert h.event_type == "VOL_SPIKE"
    with pytest.raises(Exception):
        h.event_type = "OTHER"  # type: ignore[misc]


def test_decision_trace_final_decision_filtered():
    h = HypothesisRecord(
        hypothesis_id="h1", event_type="VOL_SPIKE", template_id="mr",
        direction="long", horizon="24b", entry_lag_bars=1, symbol="BTC", run_id="r1",
    )
    trace = ResearchDecisionTrace(hypothesis=h, candidate=None)
    assert trace.final_decision == "filtered"
    assert not trace.reached_promotion


def test_decision_trace_final_decision_promotion():
    h = HypothesisRecord(
        hypothesis_id="h1", event_type="VOL_SPIKE", template_id="mr",
        direction="long", horizon="24b", entry_lag_bars=1, symbol="BTC", run_id="r1",
    )
    c = CandidateRecord(
        candidate_id="c1", hypothesis_id="h1", event_type="VOL_SPIKE",
        symbol="BTC", run_id="r1", estimate_bps=5.0, t_stat=2.5, robustness=0.75,
        n_obs=100, direction="long", horizon="24b", template_id="mr",
    )
    p = PromotionRecord(
        candidate_id="c1", event_type="VOL_SPIKE", symbol="BTC", run_id="r1",
        promotion_decision="promoted", promotion_track="standard",
        policy_version="v1", bundle_version="v1",
    )
    trace = ResearchDecisionTrace(hypothesis=h, candidate=c, promotion=p)
    assert trace.final_decision == "promoted"
    assert trace.reached_promotion


def test_validation_record_rejection_reasons_tuple():
    v = ValidationRecord(
        candidate_id="c1", hypothesis_id="h1", event_type="VOL_SPIKE",
        symbol="BTC", run_id="r1", passed=False,
        rejection_reasons=("low_t_stat", "low_robustness"),
    )
    assert v.rejection_reasons == ("low_t_stat", "low_robustness")
