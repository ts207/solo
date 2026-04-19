from __future__ import annotations

from project.live.sizing_allocator import allocate_trade_size
from project.live.trade_valuator import TradeValuation


def _valuation(*, downside: float = 10.0) -> TradeValuation:
    return TradeValuation(
        expected_gross_edge_bps=20.0,
        expected_cost_bps=2.0,
        expected_net_edge_bps=18.0,
        expected_downside_bps=downside,
        fill_probability=0.9,
        edge_confidence=0.9,
        utility_score=12.0,
        should_trade=True,
    )


def test_sizing_allocator_shrinks_under_slippage_overlap_and_downside() -> None:
    base = allocate_trade_size(
        valuation=_valuation(),
        market_state={"depth_usd": 1_000_000.0, "expected_cost_bps": 1.0},
        portfolio_state={"available_balance": 10_000.0},
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
    )
    stressed = allocate_trade_size(
        valuation=_valuation(downside=60.0),
        market_state={"depth_usd": 1_000_000.0, "expected_cost_bps": 15.0},
        portfolio_state={"available_balance": 10_000.0, "marginal_overlap": 0.8},
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
    )

    assert base.accepted is True
    assert stressed.accepted is True
    assert stressed.notional < base.notional
    assert stressed.size_fraction < base.size_fraction
