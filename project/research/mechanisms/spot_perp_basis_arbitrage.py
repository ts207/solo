from project.research.mechanisms.core import MechanismSpec

SPEC = MechanismSpec(
    mechanism_id="spot_perp_basis_arbitrage",
    version=1,
    claim="""Extreme dislocations between the Binance Spot index and Bybit Perp instrument represent transient venue-specific liquidations or forced flow that revert quickly as cross-exchange arbitrageurs step in to collapse the premium/discount.

    Because the `basis_bps` feature is now built natively by merging actual `binance/spot` and `bybit/perp` OHLCV streams (99.96% coverage), it represents a mathematically real, non-synthetic execution gap.""",
    required_observables=[
        "basis_bps",
        "basis_zscore",
        "spread_bps",
        "volume"
    ],
    falsified_by=[],
    allowed_templates=[
        "exhaustion_reversal",
        "momentum_continuation"
    ],
    required_falsification=[
        "The gross basis convergence fails to overcome the two-leg execution cost (spread * 2 + fees)",
        "The basis expands persistently due to structural holding costs (funding/carry) rather than transient flow",
        "The reversion is asymmetric, occurring only when the perp is at a premium (long-side liquidation) but not discount (short-side liquidation)"
    ],
    forbidden_rescue_actions=[
        "Using the legacy EMA proxy basis instead of the true cross-venue spot-perp basis",
        "Assuming maker-maker execution where the arb leg explicitly requires taking liquidity on at least one venue to hedge"
    ]
)
