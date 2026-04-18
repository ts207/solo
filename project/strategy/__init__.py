"""Canonical umbrella namespace for research-time strategy templates."""

from project.strategy.templates.compiler import compile_positions
from project.strategy.templates.data_bundle import DataBundle
from project.strategy.templates.evaluate import evaluate_candidates
from project.strategy.templates.generator import generate_candidates, generate_from_concept
from project.strategy.templates.spec import StrategySpec
from project.strategy.templates.validation import check_closed_left_rolling, validate_pit_invariants

evaluate = evaluate_candidates

__all__ = [
    "DataBundle",
    "StrategySpec",
    "check_closed_left_rolling",
    "compile_positions",
    "evaluate",
    "evaluate_candidates",
    "generate_candidates",
    "generate_from_concept",
    "validate_pit_invariants",
]
