from __future__ import annotations

from project.live.execution_schedule import build_execution_schedule
from project.live.trade_valuator import TradeValuation


def _valuation(*, net: float, downside: float, fill: float) -> TradeValuation:
    return TradeValuation(
        expected_gross_edge_bps=net + 2.0,
        expected_cost_bps=2.0,
        expected_net_edge_bps=net,
        expected_downside_bps=downside,
        fill_probability=fill,
        win_probability=0.60,
        edge_confidence=0.8,
        utility_score=net,
        should_trade=True,
    )


def test_execution_schedule_changes_with_urgency_and_liquidity() -> None:
    passive = build_execution_schedule(
        valuation=_valuation(net=5.0, downside=20.0, fill=0.35),
        notional=20_000.0,
        market_state={"spread_bps": 8.0, "depth_usd": 100_000.0},
    )
    aggressive = build_execution_schedule(
        valuation=_valuation(net=30.0, downside=10.0, fill=0.90),
        notional=2_000.0,
        market_state={"spread_bps": 1.0, "depth_usd": 100_000.0},
    )

    assert passive.route_preference == "passive"
    assert passive.post_only is True
    assert passive.child_order_count > aggressive.child_order_count
    assert aggressive.route_preference == "aggressive"
    assert aggressive.cancel_after_seconds < passive.cancel_after_seconds
