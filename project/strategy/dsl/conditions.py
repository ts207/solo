from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import List, Optional

from project.strategy.dsl.schema import ConditionNodeSpec


class BaseConditionHandler(ABC):
    """Abstract base for custom DSL condition handlers."""

    @abstractmethod
    def handles(self, condition: str) -> bool:
        """Return True if this class can handle the given condition string."""
        pass

    @abstractmethod
    def normalize(self, condition: str) -> List[ConditionNodeSpec]:
        """Convert the condition string into executable runtime nodes."""
        pass


class ConditionRegistry:
    """Registry for DSL condition handlers."""

    _HANDLERS: List[BaseConditionHandler] = []

    @classmethod
    def register(cls, handler: BaseConditionHandler) -> None:
        cls._HANDLERS.append(handler)

    @classmethod
    def resolve(cls, condition: str) -> Optional[List[ConditionNodeSpec]]:
        for handler in cls._HANDLERS:
            if handler.handles(condition):
                return handler.normalize(condition)
        return None


# --- Built-in Handlers ---


class SessionConditionHandler(BaseConditionHandler):
    MAP = {
        "session_asia": (0, 7),
        "session_eu": (8, 15),
        "session_us": (16, 23),
    }

    def handles(self, c):
        return c.lower() in self.MAP

    def normalize(self, c):
        start, end = self.MAP[c.lower()]
        return [
            ConditionNodeSpec(
                feature="session_hour_utc",
                operator="in_range",
                value=float(start),
                value_high=float(end),
            )
        ]


class VolRegimeConditionHandler(BaseConditionHandler):
    MAP = {
        "vol_regime_low": 0.0,
        "vol_regime_mid": 1.0,
        "vol_regime_medium": 1.0,
        "vol_regime_high": 2.0,
    }

    def handles(self, c):
        return c.lower() in self.MAP

    def normalize(self, c):
        return [
            ConditionNodeSpec(feature="vol_regime_code", operator="==", value=self.MAP[c.lower()])
        ]


class BullBearConditionHandler(BaseConditionHandler):
    MAP = {
        "bull_bear_bull": 1.0,
        "bull_bear_bear": -1.0,
    }

    def handles(self, c):
        return c.lower() in self.MAP

    def normalize(self, c):
        return [
            ConditionNodeSpec(feature="bull_bear_flag", operator="==", value=self.MAP[c.lower()])
        ]


class NumericConditionHandler(BaseConditionHandler):
    PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")

    def handles(self, c):
        return bool(self.PATTERN.match(c))

    def normalize(self, c):
        m = self.PATTERN.match(c)
        feature, operator, value = m.groups()
        return [ConditionNodeSpec(feature=feature, operator=operator, value=float(value))]


# Register defaults
ConditionRegistry.register(SessionConditionHandler())
ConditionRegistry.register(VolRegimeConditionHandler())
ConditionRegistry.register(BullBearConditionHandler())
ConditionRegistry.register(NumericConditionHandler())
