import pytest
from project.portfolio.orchestration import (
    ThesisIntent, 
    PortfolioContext, 
    TargetPortfolioState, 
    calculate_priority_score,
    generate_target_portfolio
)

def test_schemas_instantiate():
    intent = ThesisIntent(
        strategy_id="strat_1",
        family_id="momentum",
        symbol="BTC",
        requested_notional=10000.0,
        setup_match=0.9,
        thesis_strength=0.8,
        freshness=1.0,
        execution_quality=0.95,
        capital_efficiency=1.2
    )
    context = PortfolioContext(
        max_portfolio_notional=100000.0,
        family_caps={"momentum": 20000.0},
        symbol_caps={"BTC": 30000.0}
    )
    state = TargetPortfolioState(allocations={"strat_1": 5000.0})
    
    assert intent.strategy_id == "strat_1"
    assert context.max_portfolio_notional == 100000.0
    assert state.allocations["strat_1"] == 5000.0

def test_calculate_priority_score():
    intent = ThesisIntent(
        strategy_id="strat_1",
        family_id="momentum",
        symbol="BTC",
        requested_notional=10000.0,
        setup_match=0.9,
        thesis_strength=0.8,
        freshness=1.0,
        execution_quality=0.95,
        capital_efficiency=1.2
    )
    # score = 0.9 * 0.8 * 1.0 * 0.95 * 1.2 * 1.0 (diversification mult) = 0.8208
    score = calculate_priority_score(intent, diversification_multiplier=1.0)
    assert abs(score - 0.8208) < 1e-6

def test_generate_target_portfolio_greedy_with_dynamic_penalty():
    intent1 = ThesisIntent(strategy_id="strat_1", family_id="momentum", symbol="BTC", requested_notional=10000.0, setup_match=1.0, thesis_strength=1.0, freshness=1.0, execution_quality=1.0, capital_efficiency=1.0)
    # intent2 has lower setup match, but same family
    intent2 = ThesisIntent(strategy_id="strat_2", family_id="momentum", symbol="ETH", requested_notional=10000.0, setup_match=0.9, thesis_strength=1.0, freshness=1.0, execution_quality=1.0, capital_efficiency=1.0)
    # intent3 is a different family, slightly lower setup match than intent1 but higher than intent2
    intent3 = ThesisIntent(strategy_id="strat_3", family_id="mean_reversion", symbol="BTC", requested_notional=10000.0, setup_match=0.95, thesis_strength=1.0, freshness=1.0, execution_quality=1.0, capital_efficiency=1.0)
    
    context = PortfolioContext(
        max_portfolio_notional=15000.0, # Only enough capital for 1.5 intents
        family_caps={"momentum": 12000.0}, # Momentum is capped
        symbol_caps={"BTC": 20000.0}
    )
    
    state = generate_target_portfolio([intent1, intent2, intent3], context)
    
    # strat_1 should get 10000 (highest score, first to allocate)
    assert state.allocations["strat_1"] == 10000.0
    
    # Remaining capital = 5000.
    # strat_3 (score 0.95) vs strat_2 (score 0.9). 
    # Because strat_1 allocated 10k to momentum, strat_2's diversification multiplier drops, making strat_3 win the rest.
    assert state.allocations["strat_3"] == 5000.0
    
    # strat_2 gets nothing because capital is exhausted.
    assert state.allocations.get("strat_2", 0.0) == 0.0
