# Detector Coverage Audit

- Status: `passed`
- Active event specs: `71`
- Registered detectors: `71`
- Raw registered detector entries: `73`
- Issues: `0`

## Maturity Counts

- `production`: 6
- `specialized`: 4
- `standard`: 61

## Evidence Tier Counts

- `contextual`: 4
- `direct`: 20
- `hybrid`: 26
- `inferred_cross_asset`: 7
- `proxy`: 1
- `sequence_confirmed`: 4
- `statistical`: 9

## Issues

- None

## Detector Inventory

- `ABSORPTION_PROXY`: maturity=`standard`, evidence=`hybrid` via `AbsorptionProxyDetector`
- `BAND_BREAK`: maturity=`standard`, evidence=`statistical` via `BandBreakDetector`
- `BASIS_DISLOC`: maturity=`production`, evidence=`statistical` via `BasisDislocationDetector`
- `BETA_SPIKE_EVENT`: maturity=`standard`, evidence=`inferred_cross_asset` via `BetaSpikeDetector`
- `BREAKOUT_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `BreakoutTriggerDetector`
- `CHOP_TO_TREND_SHIFT`: maturity=`standard`, evidence=`hybrid` via `ChopToTrendDetector`
- `CLIMAX_VOLUME_BAR`: maturity=`standard`, evidence=`hybrid` via `ClimaxVolumeDetector`
- `COPULA_PAIRS_TRADING`: maturity=`standard`, evidence=`statistical` via `CopulaPairsTradingDetector`
- `CORRELATION_BREAKDOWN_EVENT`: maturity=`standard`, evidence=`inferred_cross_asset` via `CorrelationBreakdownDetector`
- `CROSS_ASSET_DESYNC_EVENT`: maturity=`standard`, evidence=`inferred_cross_asset` via `CrossAssetDesyncDetector`
- `CROSS_VENUE_DESYNC`: maturity=`standard`, evidence=`inferred_cross_asset` via `CrossVenueDesyncDetector`
- `DELEVERAGING_WAVE`: maturity=`standard`, evidence=`hybrid` via `DeleveragingWaveDetector`
- `DEPTH_COLLAPSE`: maturity=`standard`, evidence=`direct` via `DepthCollapseDetector`
- `DEPTH_STRESS_PROXY`: maturity=`standard`, evidence=`hybrid` via `DepthStressProxyDetector`
- `FAILED_CONTINUATION`: maturity=`standard`, evidence=`hybrid` via `FailedContinuationDetector`
- `FALSE_BREAKOUT`: maturity=`standard`, evidence=`hybrid` via `FalseBreakoutDetector`
- `FEE_REGIME_CHANGE_EVENT`: maturity=`standard`, evidence=`direct` via `FeeRegimeChangeDetector`
- `FLOW_EXHAUSTION_PROXY`: maturity=`standard`, evidence=`hybrid` via `FlowExhaustionDetector`
- `FND_DISLOC`: maturity=`production`, evidence=`direct` via `FndDislocDetector`
- `FORCED_FLOW_EXHAUSTION`: maturity=`standard`, evidence=`hybrid` via `ForcedFlowExhaustionDetector`
- `FUNDING_EXTREME_ONSET`: maturity=`standard`, evidence=`direct` via `FundingExtremeOnsetDetector`
- `FUNDING_FLIP`: maturity=`standard`, evidence=`direct` via `FundingFlipDetector`
- `FUNDING_NORMALIZATION_TRIGGER`: maturity=`standard`, evidence=`direct` via `FundingNormalizationDetector`
- `FUNDING_PERSISTENCE_TRIGGER`: maturity=`standard`, evidence=`direct` via `FundingPersistenceDetector`
- `FUNDING_TIMESTAMP_EVENT`: maturity=`standard`, evidence=`contextual` via `FundingTimestampDetector`
- `GAP_OVERSHOOT`: maturity=`standard`, evidence=`statistical` via `GapOvershootDetector`
- `INDEX_COMPONENT_DIVERGENCE`: maturity=`standard`, evidence=`inferred_cross_asset` via `IndexComponentDivergenceDetector`
- `LEAD_LAG_BREAK`: maturity=`standard`, evidence=`inferred_cross_asset` via `LeadLagBreakDetector`
- `LIQUIDATION_CASCADE`: maturity=`specialized`, evidence=`direct` via `LiquidationCascadeDetector`
- `LIQUIDATION_CASCADE_PROXY`: maturity=`specialized`, evidence=`proxy` via `LiquidationCascadeProxyDetector`
- `LIQUIDATION_EXHAUSTION_REVERSAL`: maturity=`standard`, evidence=`hybrid` via `LiquidationExhaustionReversalDetector`
- `LIQUIDITY_GAP_PRINT`: maturity=`standard`, evidence=`direct` via `LiquidityGapDetector`
- `LIQUIDITY_SHOCK`: maturity=`production`, evidence=`hybrid` via `LiquidityStressDetector`
- `LIQUIDITY_STRESS_DIRECT`: maturity=`production`, evidence=`direct` via `DirectLiquidityStressDetector`
- `LIQUIDITY_STRESS_PROXY`: maturity=`standard`, evidence=`hybrid` via `ProxyLiquidityStressDetector`
- `LIQUIDITY_VACUUM`: maturity=`specialized`, evidence=`direct` via `LiquidityVacuumDetector`
- `MOMENTUM_DIVERGENCE_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `MomentumDivergenceDetector`
- `OI_FLUSH`: maturity=`standard`, evidence=`direct` via `OIFlushDetector`
- `OI_SPIKE_NEGATIVE`: maturity=`standard`, evidence=`direct` via `OISpikeNegativeDetector`
- `OI_SPIKE_POSITIVE`: maturity=`standard`, evidence=`direct` via `OISpikePositiveDetector`
- `ORDERFLOW_IMBALANCE_SHOCK`: maturity=`standard`, evidence=`direct` via `OrderflowImbalanceShockDetector`
- `OVERSHOOT_AFTER_SHOCK`: maturity=`standard`, evidence=`statistical` via `OvershootDetector`
- `POST_DELEVERAGING_REBOUND`: maturity=`standard`, evidence=`hybrid` via `PostDeleveragingReboundDetector`
- `PRICE_VOL_IMBALANCE_PROXY`: maturity=`standard`, evidence=`hybrid` via `PriceVolImbalanceProxyDetector`
- `PULLBACK_PIVOT`: maturity=`standard`, evidence=`hybrid` via `PullbackPivotDetector`
- `RANGE_BREAKOUT`: maturity=`standard`, evidence=`hybrid` via `RangeBreakoutDetector`
- `RANGE_COMPRESSION_END`: maturity=`standard`, evidence=`hybrid` via `RangeCompressionDetector`
- `SCHEDULED_NEWS_WINDOW_EVENT`: maturity=`standard`, evidence=`contextual` via `ScheduledNewsDetector`
- `SEQ_FND_EXTREME_THEN_BREAKOUT`: maturity=`standard`, evidence=`sequence_confirmed` via `EventSequenceDetector`
- `SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY`: maturity=`standard`, evidence=`sequence_confirmed` via `EventSequenceDetector`
- `SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE`: maturity=`standard`, evidence=`sequence_confirmed` via `EventSequenceDetector`
- `SEQ_VOL_COMP_THEN_BREAKOUT`: maturity=`standard`, evidence=`sequence_confirmed` via `EventSequenceDetector`
- `SESSION_CLOSE_EVENT`: maturity=`standard`, evidence=`contextual` via `SessionCloseDetector`
- `SESSION_OPEN_EVENT`: maturity=`standard`, evidence=`contextual` via `SessionOpenDetector`
- `SLIPPAGE_SPIKE_EVENT`: maturity=`standard`, evidence=`direct` via `SlippageSpikeDetector`
- `SPOT_PERP_BASIS_SHOCK`: maturity=`production`, evidence=`inferred_cross_asset` via `SpotPerpBasisShockDetector`
- `SPREAD_BLOWOUT`: maturity=`standard`, evidence=`direct` via `SpreadBlowoutDetector`
- `SPREAD_REGIME_WIDENING_EVENT`: maturity=`standard`, evidence=`direct` via `SpreadRegimeWideningDetector`
- `SUPPORT_RESISTANCE_BREAK`: maturity=`standard`, evidence=`hybrid` via `SREventDetector`
- `SWEEP_STOPRUN`: maturity=`standard`, evidence=`hybrid` via `SweepStopRunDetector`
- `TREND_ACCELERATION`: maturity=`standard`, evidence=`hybrid` via `TrendAccelerationDetector`
- `TREND_DECELERATION`: maturity=`standard`, evidence=`hybrid` via `TrendDecelerationDetector`
- `TREND_EXHAUSTION_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `TrendExhaustionDetector`
- `TREND_TO_CHOP_SHIFT`: maturity=`standard`, evidence=`hybrid` via `TrendToChopDetector`
- `VOL_CLUSTER_SHIFT`: maturity=`standard`, evidence=`statistical` via `VolClusterShiftDetector`
- `VOL_REGIME_SHIFT_EVENT`: maturity=`standard`, evidence=`statistical` via `VolRegimeShiftDetector`
- `VOL_RELAXATION_START`: maturity=`standard`, evidence=`direct` via `VolRelaxationDetector`
- `VOL_SHOCK`: maturity=`specialized`, evidence=`statistical` via `VolShockRelaxationDetector`
- `VOL_SPIKE`: maturity=`production`, evidence=`direct` via `VolSpikeDetector`
- `WICK_REVERSAL_PROXY`: maturity=`standard`, evidence=`hybrid` via `WickReversalProxyDetector`
- `ZSCORE_STRETCH`: maturity=`standard`, evidence=`statistical` via `ZScoreStretchDetector`
