from typing import Dict, List

from pydantic import BaseModel, Field

from project.portfolio.engine import PortfolioDecisionEngine
from project.portfolio.engine import ThesisIntent as EngineThesisIntent


class ThesisIntent(BaseModel):
    strategy_id: str
    family_id: str
    symbol: str
    requested_notional: float
    setup_match: float
    thesis_strength: float
    freshness: float
    execution_quality: float
    capital_efficiency: float


class PortfolioContext(BaseModel):
    max_portfolio_notional: float
    family_caps: Dict[str, float] = Field(default_factory=dict)
    symbol_caps: Dict[str, float] = Field(default_factory=dict)


class TargetPortfolioState(BaseModel):
    allocations: Dict[str, float]


def calculate_priority_score(intent: ThesisIntent, diversification_multiplier: float) -> float:
    return (
        intent.setup_match
        * intent.thesis_strength
        * intent.freshness
        * intent.execution_quality
        * intent.capital_efficiency
        * diversification_multiplier
    )


def generate_target_portfolio(intents: List[ThesisIntent], context: PortfolioContext) -> TargetPortfolioState:
    engine_intents = [
        EngineThesisIntent(
            thesis_id=intent.strategy_id,
            family=intent.family_id,
            symbol=intent.symbol,
            overlap_group_id=f"legacy::{intent.strategy_id}",
            requested_notional=float(intent.requested_notional),
            support_score=calculate_priority_score(intent, diversification_multiplier=1.0),
            execution_quality=float(intent.execution_quality),
            evidence_multiplier=float(intent.freshness) * float(intent.capital_efficiency),
        )
        for intent in intents
    ]
    engine = PortfolioDecisionEngine(
        family_budgets=dict(context.family_caps),
        symbol_caps=dict(context.symbol_caps),
        max_portfolio_notional=float(context.max_portfolio_notional),
        correlation_limit=1_000_000.0,
    )
    decisions = engine.decide(engine_intents, available_portfolio_notional=float(context.max_portfolio_notional))
    allocations = {decision.thesis_id: float(decision.allocated_notional) for decision in decisions}
    for intent in intents:
        allocations.setdefault(intent.strategy_id, 0.0)
    return TargetPortfolioState(allocations=allocations)
