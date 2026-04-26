from .definitions.basis import BASIS_SCENARIOS
from .definitions.coverage_gaps import COVERAGE_GAP_SCENARIOS
from .definitions.funding import FUNDING_SCENARIOS
from .definitions.liquidity import LIQUIDITY_SCENARIOS
from .definitions.orderflow import ORDERFLOW_SCENARIOS
from .definitions.regime import REGIME_SCENARIOS
from .definitions.temporal import TEMPORAL_SCENARIOS
from .definitions.trend import TREND_SCENARIOS
from .definitions.volatility import VOLATILITY_SCENARIOS

ALL_SCENARIOS = {}
for module in [
    LIQUIDITY_SCENARIOS,
    ORDERFLOW_SCENARIOS,
    VOLATILITY_SCENARIOS,
    BASIS_SCENARIOS,
    FUNDING_SCENARIOS,
    TREND_SCENARIOS,
    REGIME_SCENARIOS,
    TEMPORAL_SCENARIOS,
    COVERAGE_GAP_SCENARIOS,
]:
    if module:
        ALL_SCENARIOS.update(module)

__all__ = [
    "ALL_SCENARIOS",
    "BASIS_SCENARIOS",
    "COVERAGE_GAP_SCENARIOS",
    "FUNDING_SCENARIOS",
    "LIQUIDITY_SCENARIOS",
    "ORDERFLOW_SCENARIOS",
    "REGIME_SCENARIOS",
    "TEMPORAL_SCENARIOS",
    "TREND_SCENARIOS",
    "VOLATILITY_SCENARIOS",
]
