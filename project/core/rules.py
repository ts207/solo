from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type


class BaseRuleHandler(ABC):
    """Abstract base for custom logic rules."""

    @abstractmethod
    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Return True if the rule passes based on the given context."""
        pass


class RuleRegistry:
    """Registry for domain-specific logic rules."""

    _RULES: Dict[str, Type[BaseRuleHandler]] = {}

    @classmethod
    def register(cls, name: str, rule_cls: Type[BaseRuleHandler]) -> None:
        cls._RULES[name.lower()] = rule_cls

    @classmethod
    def get_rule(cls, name: str) -> Optional[BaseRuleHandler]:
        cls_type = cls._RULES.get(name.lower())
        return cls_type() if cls_type else None


# --- Built-in Quality Rules ---


class MinTradeRule(BaseRuleHandler):
    def evaluate(self, context):
        trades = context.get("n_trades", 0)
        threshold = context.get("min_trades_threshold", 20)
        return trades >= threshold


class PositiveExpectancyRule(BaseRuleHandler):
    def evaluate(self, context):
        exp = context.get("expectancy", 0.0)
        return exp > 0


# Register defaults
RuleRegistry.register("min_trades", MinTradeRule)
RuleRegistry.register("positive_expectancy", PositiveExpectancyRule)
