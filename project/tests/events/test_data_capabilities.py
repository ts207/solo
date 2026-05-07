from __future__ import annotations

from project.events.data_capabilities import detector_trade_eligible, load_data_capability_profile


def test_no_liquidation_profile_allows_only_composite_trade_candidates() -> None:
    profile = load_data_capability_profile("no_liquidations_v1")

    assert profile.trade_candidate("OI_FLUSH_REVERSAL")
    assert profile.trade_candidate("FUNDING_CROWDING_BREAK")
    assert not profile.paper_approved("OI_FLUSH_REVERSAL")
    assert not profile.live_approved("FUNDING_CROWDING_BREAK")
    assert profile.feed_available("ohlcv")
    assert profile.feed_available("open_interest")
    assert profile.feed_available("funding")
    assert not profile.feed_available("spread_bps")
    assert not profile.feed_available("depth_usd")
    assert not profile.research_only("FUNDING_CROWDING_BREAK_H96_ETHUSDT")
    assert profile.rejected("FUNDING_CROWDING_BREAK_H96_ETHUSDT")
    assert profile.rejected("MTF_SHORT_BUILD_HIGH_VOL_DOWNTREND")
    assert profile.rejected("FUNDING_WINDOW_DRIFT_AS_FUNDING_SPECIFIC_EDGE")
    assert profile.research_only("FUNDING_WINDOW_DRIFT_PLACEBO_FAILED")
    assert profile.family_frozen("SHORT_BUILD_CONTINUATION")
    assert profile.killed("SQUEEZE_RISK_REVERSAL")
    assert not profile.trade_candidate("SQUEEZE_RISK_REVERSAL")
    assert not detector_trade_eligible("SQUEEZE_RISK_REVERSAL", profile=profile)
    variant = profile.research_candidate_variants["FUNDING_CROWDING_BREAK_H96_ETHUSDT"]
    assert variant["base_event"] == "FUNDING_CROWDING_BREAK"
    assert variant["status"] == "rejected_fresh_validation"
    drift_variant = profile.research_candidate_variants["FUNDING_WINDOW_DRIFT"]
    assert drift_variant["status"] == "placebo_failed_research_only"
    assert drift_variant["reason"] == "shifted_plus_1_day_placebo_passed_core_gates"
    assert drift_variant["paper_approved"] is False
    assert drift_variant["live_approved"] is False
    assert not profile.trade_candidate("OI_FLUSH")
    assert not profile.trade_candidate("VOL_SHOCK")
    assert not profile.trade_candidate("FAILED_CONTINUATION")
    assert not detector_trade_eligible("LIQUIDITY_VACUUM_RECOVERY", profile=profile)
