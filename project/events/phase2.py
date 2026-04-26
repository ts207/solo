"""
Phase 2 conditional pipeline execution mapping.

Defines the list of canonical Phase 1 event types and the corresponding analyzer
scripts and arguments required to process them in Phase 2 candidate discovery.
"""


PHASE2_EVENT_CHAIN: list[tuple[str, str, list[str]]] = [
    ("VOL_SHOCK", "analyze_events.py", ["--event_type", "VOL_SHOCK", "--timeframe", "5m"]),
    (
        "LIQUIDITY_VACUUM",
        "analyze_events.py",
        ["--event_type", "LIQUIDITY_VACUUM", "--timeframe", "5m"],
    ),
    ("FORCED_FLOW_EXHAUSTION", "analyze_events.py", ["--event_type", "FORCED_FLOW_EXHAUSTION"]),
    ("CROSS_VENUE_DESYNC", "analyze_events.py", ["--event_type", "CROSS_VENUE_DESYNC"]),
    (
        "CROSS_ASSET_DESYNC_EVENT",
        "analyze_events.py",
        ["--event_type", "CROSS_ASSET_DESYNC_EVENT"],
    ),
    ("FUNDING_EXTREME_ONSET", "analyze_events.py", ["--event_type", "FUNDING_EXTREME_ONSET"]),
    (
        "FUNDING_PERSISTENCE_TRIGGER",
        "analyze_events.py",
        ["--event_type", "FUNDING_PERSISTENCE_TRIGGER"],
    ),
    (
        "FUNDING_NORMALIZATION_TRIGGER",
        "analyze_events.py",
        ["--event_type", "FUNDING_NORMALIZATION_TRIGGER"],
    ),
    ("OI_SPIKE_POSITIVE", "analyze_events.py", ["--event_type", "OI_SPIKE_POSITIVE"]),
    ("OI_SPIKE_NEGATIVE", "analyze_events.py", ["--event_type", "OI_SPIKE_NEGATIVE"]),
    ("OI_FLUSH", "analyze_events.py", ["--event_type", "OI_FLUSH"]),
    ("LIQUIDATION_CASCADE", "analyze_events.py", ["--event_type", "LIQUIDATION_CASCADE"]),
    ("LIQUIDATION_CASCADE_PROXY", "analyze_events.py", ["--event_type", "LIQUIDATION_CASCADE_PROXY"]),
    ("LIQUIDITY_SHOCK", "analyze_events.py", ["--event_type", "LIQUIDITY_SHOCK"]),
    ("LIQUIDITY_STRESS_DIRECT", "analyze_events.py", ["--event_type", "LIQUIDITY_STRESS_DIRECT"]),
    ("LIQUIDITY_STRESS_PROXY", "analyze_events.py", ["--event_type", "LIQUIDITY_STRESS_PROXY"]),
    (
        "DEPTH_COLLAPSE",
        "analyze_events.py",
        ["--event_type", "DEPTH_COLLAPSE", "--timeframe", "5m"],
    ),
    (
        "DEPTH_STRESS_PROXY",
        "analyze_events.py",
        ["--event_type", "DEPTH_STRESS_PROXY", "--timeframe", "5m"],
    ),
    (
        "SPREAD_BLOWOUT",
        "analyze_events.py",
        ["--event_type", "SPREAD_BLOWOUT", "--timeframe", "5m"],
    ),
    (
        "ORDERFLOW_IMBALANCE_SHOCK",
        "analyze_events.py",
        ["--event_type", "ORDERFLOW_IMBALANCE_SHOCK", "--timeframe", "5m"],
    ),
    (
        "PRICE_VOL_IMBALANCE_PROXY",
        "analyze_events.py",
        ["--event_type", "PRICE_VOL_IMBALANCE_PROXY", "--timeframe", "5m"],
    ),
    ("SWEEP_STOPRUN", "analyze_events.py", ["--event_type", "SWEEP_STOPRUN", "--timeframe", "5m"]),
    (
        "WICK_REVERSAL_PROXY",
        "analyze_events.py",
        ["--event_type", "WICK_REVERSAL_PROXY", "--timeframe", "5m"],
    ),
    (
        "ABSORPTION_PROXY",
        "analyze_events.py",
        ["--event_type", "ABSORPTION_PROXY", "--timeframe", "5m"],
    ),
    (
        "LIQUIDITY_GAP_PRINT",
        "analyze_events.py",
        ["--event_type", "LIQUIDITY_GAP_PRINT", "--timeframe", "5m"],
    ),
    ("VOL_SPIKE", "analyze_events.py", ["--event_type", "VOL_SPIKE", "--timeframe", "5m"]),
    (
        "VOL_RELAXATION_START",
        "analyze_events.py",
        ["--event_type", "VOL_RELAXATION_START", "--timeframe", "5m"],
    ),
    (
        "VOL_CLUSTER_SHIFT",
        "analyze_events.py",
        ["--event_type", "VOL_CLUSTER_SHIFT", "--timeframe", "5m"],
    ),
    (
        "RANGE_COMPRESSION_END",
        "analyze_events.py",
        ["--event_type", "RANGE_COMPRESSION_END", "--timeframe", "5m"],
    ),
    (
        "BREAKOUT_TRIGGER",
        "analyze_events.py",
        ["--event_type", "BREAKOUT_TRIGGER", "--timeframe", "5m"],
    ),
    ("FUNDING_FLIP", "analyze_events.py", ["--event_type", "FUNDING_FLIP", "--timeframe", "5m"]),
    (
        "DELEVERAGING_WAVE",
        "analyze_events.py",
        ["--event_type", "DELEVERAGING_WAVE", "--timeframe", "5m"],
    ),
    (
        "TREND_EXHAUSTION_TRIGGER",
        "analyze_events.py",
        ["--event_type", "TREND_EXHAUSTION_TRIGGER", "--timeframe", "5m"],
    ),
    (
        "MOMENTUM_DIVERGENCE_TRIGGER",
        "analyze_events.py",
        ["--event_type", "MOMENTUM_DIVERGENCE_TRIGGER", "--timeframe", "5m"],
    ),
    (
        "CLIMAX_VOLUME_BAR",
        "analyze_events.py",
        ["--event_type", "CLIMAX_VOLUME_BAR", "--timeframe", "5m"],
    ),
    (
        "FAILED_CONTINUATION",
        "analyze_events.py",
        ["--event_type", "FAILED_CONTINUATION", "--timeframe", "5m"],
    ),
    (
        "LIQUIDATION_EXHAUSTION_REVERSAL",
        "analyze_events.py",
        ["--event_type", "LIQUIDATION_EXHAUSTION_REVERSAL", "--timeframe", "5m"],
    ),
    (
        "FLOW_EXHAUSTION_PROXY",
        "analyze_events.py",
        ["--event_type", "FLOW_EXHAUSTION_PROXY", "--timeframe", "5m"],
    ),
    (
        "POST_DELEVERAGING_REBOUND",
        "analyze_events.py",
        ["--event_type", "POST_DELEVERAGING_REBOUND", "--timeframe", "5m"],
    ),
    (
        "RANGE_BREAKOUT",
        "analyze_events.py",
        ["--event_type", "RANGE_BREAKOUT", "--timeframe", "5m"],
    ),
    (
        "FALSE_BREAKOUT",
        "analyze_events.py",
        ["--event_type", "FALSE_BREAKOUT", "--timeframe", "5m"],
    ),
    (
        "TREND_ACCELERATION",
        "analyze_events.py",
        ["--event_type", "TREND_ACCELERATION", "--timeframe", "5m"],
    ),
    (
        "TREND_DECELERATION",
        "analyze_events.py",
        ["--event_type", "TREND_DECELERATION", "--timeframe", "5m"],
    ),
    (
        "PULLBACK_PIVOT",
        "analyze_events.py",
        ["--event_type", "PULLBACK_PIVOT", "--timeframe", "5m"],
    ),
    (
        "SUPPORT_RESISTANCE_BREAK",
        "analyze_events.py",
        ["--event_type", "SUPPORT_RESISTANCE_BREAK", "--timeframe", "5m"],
    ),
    (
        "ZSCORE_STRETCH",
        "analyze_events.py",
        ["--event_type", "ZSCORE_STRETCH", "--timeframe", "5m"],
    ),
    ("BAND_BREAK", "analyze_events.py", ["--event_type", "BAND_BREAK", "--timeframe", "5m"]),
    (
        "OVERSHOOT_AFTER_SHOCK",
        "analyze_events.py",
        ["--event_type", "OVERSHOOT_AFTER_SHOCK", "--timeframe", "5m"],
    ),
    ("GAP_OVERSHOOT", "analyze_events.py", ["--event_type", "GAP_OVERSHOOT", "--timeframe", "5m"]),
    (
        "VOL_REGIME_SHIFT_EVENT",
        "analyze_events.py",
        ["--event_type", "VOL_REGIME_SHIFT_EVENT", "--timeframe", "5m"],
    ),
    (
        "TREND_TO_CHOP_SHIFT",
        "analyze_events.py",
        ["--event_type", "TREND_TO_CHOP_SHIFT", "--timeframe", "5m"],
    ),
    (
        "CHOP_TO_TREND_SHIFT",
        "analyze_events.py",
        ["--event_type", "CHOP_TO_TREND_SHIFT", "--timeframe", "5m"],
    ),
    (
        "CORRELATION_BREAKDOWN_EVENT",
        "analyze_events.py",
        ["--event_type", "CORRELATION_BREAKDOWN_EVENT", "--timeframe", "5m"],
    ),
    (
        "BETA_SPIKE_EVENT",
        "analyze_events.py",
        ["--event_type", "BETA_SPIKE_EVENT", "--timeframe", "5m"],
    ),
    (
        "INDEX_COMPONENT_DIVERGENCE",
        "analyze_events.py",
        ["--event_type", "INDEX_COMPONENT_DIVERGENCE", "--timeframe", "5m"],
    ),
    (
        "SPOT_PERP_BASIS_SHOCK",
        "analyze_events.py",
        ["--event_type", "SPOT_PERP_BASIS_SHOCK", "--timeframe", "5m"],
    ),
    (
        "LEAD_LAG_BREAK",
        "analyze_events.py",
        ["--event_type", "LEAD_LAG_BREAK", "--timeframe", "5m"],
    ),
    (
        "SESSION_OPEN_EVENT",
        "analyze_events.py",
        ["--event_type", "SESSION_OPEN_EVENT", "--timeframe", "5m"],
    ),
    (
        "SESSION_CLOSE_EVENT",
        "analyze_events.py",
        ["--event_type", "SESSION_CLOSE_EVENT", "--timeframe", "5m"],
    ),
    (
        "FUNDING_TIMESTAMP_EVENT",
        "analyze_events.py",
        ["--event_type", "FUNDING_TIMESTAMP_EVENT", "--timeframe", "5m"],
    ),
    (
        "SCHEDULED_NEWS_WINDOW_EVENT",
        "analyze_events.py",
        ["--event_type", "SCHEDULED_NEWS_WINDOW_EVENT", "--timeframe", "5m"],
    ),
    (
        "SPREAD_REGIME_WIDENING_EVENT",
        "analyze_events.py",
        ["--event_type", "SPREAD_REGIME_WIDENING_EVENT", "--timeframe", "5m"],
    ),
    (
        "SLIPPAGE_SPIKE_EVENT",
        "analyze_events.py",
        ["--event_type", "SLIPPAGE_SPIKE_EVENT", "--timeframe", "5m"],
    ),
    (
        "FEE_REGIME_CHANGE_EVENT",
        "analyze_events.py",
        ["--event_type", "FEE_REGIME_CHANGE_EVENT", "--timeframe", "5m"],
    ),
    ("COPULA_PAIRS_TRADING", "analyze_events.py", ["--event_type", "COPULA_PAIRS_TRADING"]),
    ("FND_DISLOC", "analyze_events.py", ["--event_type", "FND_DISLOC", "--timeframe", "5m"]),
    ("BASIS_DISLOC", "analyze_events.py", ["--event_type", "BASIS_DISLOC", "--timeframe", "5m"]),
    (
        "POST_DELEVERAGING_REBOUND",
        "analyze_events.py",
        ["--event_type", "POST_DELEVERAGING_REBOUND", "--timeframe", "5m"],
    ),
    (
        "SEQ_FND_EXTREME_THEN_BREAKOUT",
        "analyze_events.py",
        ["--event_type", "SEQ_FND_EXTREME_THEN_BREAKOUT", "--timeframe", "5m"],
    ),
    (
        "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY",
        "analyze_events.py",
        ["--event_type", "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY", "--timeframe", "5m"],
    ),
    (
        "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE",
        "analyze_events.py",
        ["--event_type", "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE", "--timeframe", "5m"],
    ),
    (
        "SEQ_VOL_COMP_THEN_BREAKOUT",
        "analyze_events.py",
        ["--event_type", "SEQ_VOL_COMP_THEN_BREAKOUT", "--timeframe", "5m"],
    ),
]
