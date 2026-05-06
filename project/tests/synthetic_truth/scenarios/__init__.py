try:
    from .definitions.basis import BASIS_SCENARIOS
except ModuleNotFoundError:
    BASIS_SCENARIOS = {}
try:
    from .definitions.coverage_gaps import COVERAGE_GAP_SCENARIOS
except ModuleNotFoundError:
    COVERAGE_GAP_SCENARIOS = {}
try:
    from .definitions.funding import FUNDING_SCENARIOS
except ModuleNotFoundError:
    FUNDING_SCENARIOS = {}
try:
    from .definitions.liquidity import LIQUIDITY_SCENARIOS
except ModuleNotFoundError:
    LIQUIDITY_SCENARIOS = {}
try:
    from .definitions.orderflow import ORDERFLOW_SCENARIOS
except ModuleNotFoundError:
    ORDERFLOW_SCENARIOS = {}
try:
    from .definitions.regime import REGIME_SCENARIOS
except ModuleNotFoundError:
    REGIME_SCENARIOS = {}
try:
    from .definitions.temporal import TEMPORAL_SCENARIOS
except ModuleNotFoundError:
    TEMPORAL_SCENARIOS = {}
try:
    from .definitions.trend import TREND_SCENARIOS
except ModuleNotFoundError:
    TREND_SCENARIOS = {}
try:
    from .definitions.volatility import VOLATILITY_SCENARIOS
except ModuleNotFoundError:
    VOLATILITY_SCENARIOS = {}

ALL_SCENARIOS = {}
for module in [LIQUIDITY_SCENARIOS, ORDERFLOW_SCENARIOS, VOLATILITY_SCENARIOS,
               BASIS_SCENARIOS, FUNDING_SCENARIOS, TREND_SCENARIOS,
               REGIME_SCENARIOS, TEMPORAL_SCENARIOS, COVERAGE_GAP_SCENARIOS]:
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
