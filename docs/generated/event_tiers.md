# Event tiers

## Tier A

- `BASIS_DISLOC` — role `trigger`, disposition `primary_trigger_candidate`
- `FND_DISLOC` — role `trigger`, disposition `primary_trigger_candidate`
- `LIQUIDATION_CASCADE` — role `trigger`, disposition `primary_trigger_candidate`
- `LIQUIDITY_SHOCK` — role `trigger`, disposition `primary_trigger_candidate`
- `LIQUIDITY_VACUUM` — role `trigger`, disposition `primary_trigger_candidate`
- `SPOT_PERP_BASIS_SHOCK` — role `trigger`, disposition `primary_trigger_candidate`
- `VOL_SHOCK` — role `trigger`, disposition `primary_trigger_candidate`
- `VOL_SPIKE` — role `trigger`, disposition `primary_trigger_candidate`

## Tier B

- `ABSORPTION_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `BAND_BREAK` — role `trigger`, disposition `secondary_or_confirm`
- `BETA_SPIKE_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `BREAKOUT_TRIGGER` — role `trigger`, disposition `secondary_or_confirm`
- `CHOP_TO_TREND_SHIFT` — role `trigger`, disposition `secondary_or_confirm`
- `CLIMAX_VOLUME_BAR` — role `trigger`, disposition `secondary_or_confirm`
- `CORRELATION_BREAKDOWN_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `CROSS_VENUE_DESYNC` — role `trigger`, disposition `secondary_or_confirm`
- `DELEVERAGING_WAVE` — role `trigger`, disposition `secondary_or_confirm`
- `DEPTH_COLLAPSE` — role `trigger`, disposition `secondary_or_confirm`
- `DEPTH_STRESS_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `FAILED_CONTINUATION` — role `trigger`, disposition `secondary_or_confirm`
- `FALSE_BREAKOUT` — role `trigger`, disposition `secondary_or_confirm`
- `FEE_REGIME_CHANGE_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `FLOW_EXHAUSTION_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `FORCED_FLOW_EXHAUSTION` — role `trigger`, disposition `secondary_or_confirm`
- `FUNDING_EXTREME_ONSET` — role `trigger`, disposition `secondary_or_confirm`
- `FUNDING_FLIP` — role `trigger`, disposition `secondary_or_confirm`
- `FUNDING_NORMALIZATION_TRIGGER` — role `trigger`, disposition `secondary_or_confirm`
- `FUNDING_PERSISTENCE_TRIGGER` — role `trigger`, disposition `secondary_or_confirm`
- `GAP_OVERSHOOT` — role `trigger`, disposition `secondary_or_confirm`
- `INDEX_COMPONENT_DIVERGENCE` — role `trigger`, disposition `secondary_or_confirm`
- `LEAD_LAG_BREAK` — role `trigger`, disposition `secondary_or_confirm`
- `LIQUIDATION_EXHAUSTION_REVERSAL` — role `trigger`, disposition `secondary_or_confirm`
- `LIQUIDITY_GAP_PRINT` — role `trigger`, disposition `secondary_or_confirm`
- `LIQUIDITY_STRESS_DIRECT` — role `trigger`, disposition `secondary_or_confirm`
- `LIQUIDITY_STRESS_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `MOMENTUM_DIVERGENCE_TRIGGER` — role `trigger`, disposition `secondary_or_confirm`
- `OI_FLUSH` — role `trigger`, disposition `secondary_or_confirm`
- `OI_SPIKE_NEGATIVE` — role `trigger`, disposition `primary`
- `OI_SPIKE_POSITIVE` — role `trigger`, disposition `secondary_or_confirm`
- `ORDERFLOW_IMBALANCE_SHOCK` — role `trigger`, disposition `secondary_or_confirm`
- `OVERSHOOT_AFTER_SHOCK` — role `trigger`, disposition `secondary_or_confirm`
- `POST_DELEVERAGING_REBOUND` — role `trigger`, disposition `secondary_or_confirm`
- `PRICE_VOL_IMBALANCE_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `PULLBACK_PIVOT` — role `trigger`, disposition `secondary_or_confirm`
- `RANGE_BREAKOUT` — role `trigger`, disposition `secondary_or_confirm`
- `RANGE_COMPRESSION_END` — role `trigger`, disposition `secondary_or_confirm`
- `SLIPPAGE_SPIKE_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `SPREAD_BLOWOUT` — role `trigger`, disposition `secondary_or_confirm`
- `SPREAD_REGIME_WIDENING_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `SUPPORT_RESISTANCE_BREAK` — role `trigger`, disposition `secondary_or_confirm`
- `SWEEP_STOPRUN` — role `trigger`, disposition `secondary_or_confirm`
- `TREND_ACCELERATION` — role `trigger`, disposition `secondary_or_confirm`
- `TREND_DECELERATION` — role `trigger`, disposition `secondary_or_confirm`
- `TREND_EXHAUSTION_TRIGGER` — role `trigger`, disposition `secondary_or_confirm`
- `TREND_TO_CHOP_SHIFT` — role `trigger`, disposition `secondary_or_confirm`
- `VOL_CLUSTER_SHIFT` — role `trigger`, disposition `secondary_or_confirm`
- `VOL_REGIME_SHIFT_EVENT` — role `trigger`, disposition `secondary_or_confirm`
- `VOL_RELAXATION_START` — role `trigger`, disposition `secondary_or_confirm`
- `WICK_REVERSAL_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `ZSCORE_STRETCH` — role `trigger`, disposition `secondary_or_confirm`

## Tier C

- `CROSS_ASSET_DESYNC_EVENT` — role `filter`, disposition `repair_before_promotion`
- `FUNDING_TIMESTAMP_EVENT` — role `context`, disposition `context_only`
- `LIQUIDATION_CASCADE_PROXY` — role `trigger`, disposition `secondary_or_confirm`
- `SCHEDULED_NEWS_WINDOW_EVENT` — role `context`, disposition `context_only`
- `SESSION_CLOSE_EVENT` — role `context`, disposition `context_only`
- `SESSION_OPEN_EVENT` — role `context`, disposition `context_only`

## Tier D

- `COPULA_PAIRS_TRADING` — role `research_only`, disposition `research_only`
- `SEQ_FND_EXTREME_THEN_BREAKOUT` — role `sequence_component`, disposition `research_only`
- `SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY` — role `sequence_component`, disposition `research_only`
- `SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE` — role `sequence_component`, disposition `research_only`
- `SEQ_VOL_COMP_THEN_BREAKOUT` — role `sequence_component`, disposition `research_only`
