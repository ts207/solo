from pydantic import BaseModel, Field
from typing import Dict, Optional, List

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
        intent.setup_match *
        intent.thesis_strength *
        intent.freshness *
        intent.execution_quality *
        intent.capital_efficiency *
        diversification_multiplier
    )

def generate_target_portfolio(intents: List[ThesisIntent], context: PortfolioContext) -> TargetPortfolioState:
    allocations = {}
    current_portfolio_notional = 0.0
    current_family_exposure = {k: 0.0 for k in context.family_caps.keys()}
    current_symbol_exposure = {k: 0.0 for k in context.symbol_caps.keys()}
    
    # Track remaining intents
    remaining_intents = intents.copy()
    
    while remaining_intents and current_portfolio_notional < context.max_portfolio_notional:
        best_intent = None
        best_score = -1.0
        
        for intent in remaining_intents:
            # Calculate dynamic diversification multiplier.
            # Simple heuristic: penalty scales with how close the family is to its cap.
            family_cap = context.family_caps.get(intent.family_id, context.max_portfolio_notional)
            current_family = current_family_exposure.get(intent.family_id, 0.0)
            
            if current_family >= family_cap:
                div_multiplier = 0.0
            else:
                # E.g., if 0% used -> 1.0. If 50% used -> 0.5
                div_multiplier = 1.0 - (current_family / family_cap)
            
            score = calculate_priority_score(intent, div_multiplier)
            if score > best_score:
                best_score = score
                best_intent = intent
                
        if not best_intent or best_score <= 0.0:
            break # No valid intents left or caps hit
            
        remaining_intents.remove(best_intent)
        
        # Determine max allocatable
        available_portfolio = context.max_portfolio_notional - current_portfolio_notional
        family_cap = context.family_caps.get(best_intent.family_id, context.max_portfolio_notional)
        available_family = family_cap - current_family_exposure.get(best_intent.family_id, 0.0)
        
        symbol_cap = context.symbol_caps.get(best_intent.symbol, context.max_portfolio_notional)
        available_symbol = symbol_cap - current_symbol_exposure.get(best_intent.symbol, 0.0)
        
        max_allocatable = min(best_intent.requested_notional, available_portfolio, available_family, available_symbol)
        
        if max_allocatable > 0:
            allocations[best_intent.strategy_id] = max_allocatable
            current_portfolio_notional += max_allocatable
            current_family_exposure[best_intent.family_id] = current_family_exposure.get(best_intent.family_id, 0.0) + max_allocatable
            current_symbol_exposure[best_intent.symbol] = current_symbol_exposure.get(best_intent.symbol, 0.0) + max_allocatable

    # Ensure all missing intents have 0.0
    for intent in intents:
        if intent.strategy_id not in allocations:
            allocations[intent.strategy_id] = 0.0
            
    return TargetPortfolioState(allocations=allocations)
