# Detector Coverage Audit

- Status: `passed`
- Active event specs: `71`
- Registered detectors: `71`
- Raw registered detector entries: `73`
- Issues: `7`

## Maturity Counts

- `production`: 9
- `standard`: 62

## Evidence Tier Counts

- `contextual`: 4
- `direct`: 20
- `hybrid`: 26
- `inferred_cross_asset`: 7
- `proxy`: 1
- `sequence_confirmed`: 4
- `statistical`: 9

## Issues

- [warning] Detector implementation has hardcoded numerical thresholds: BASIS_DISLOC (project/events/detectors/dislocation_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: CROSS_ASSET_DESYNC_EVENT (project/events/detectors/desync_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: CROSS_VENUE_DESYNC (project/events/detectors/desync_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: FUNDING_EXTREME_ONSET (project/events/detectors/positioning_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: FUNDING_NORMALIZATION_TRIGGER (project/events/detectors/positioning_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: FUNDING_PERSISTENCE_TRIGGER (project/events/detectors/positioning_base.py)
- [warning] Detector implementation has hardcoded numerical thresholds: LIQUIDITY_GAP_PRINT (project/events/detectors/liquidity_base.py)

## Detector Inventory

- `ABSORPTION_PROXY`: maturity=`standard`, evidence=`hybrid` via `AbsorptionProxyDetector`
- `BAND_BREAK`: maturity=`standard`, evidence=`statistical` via `BandBreakDetector`
- `BASIS_DISLOC`: maturity=`standard`, evidence=`statistical` via `BasisDislocationDetectorV2`
- `BETA_SPIKE_EVENT`: maturity=`production`, evidence=`inferred_cross_asset` via `BetaSpikeDetectorV2`
- `BREAKOUT_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `BreakoutTriggerDetectorV2`
- `CHOP_TO_TREND_SHIFT`: maturity=`standard`, evidence=`hybrid` via `ChopToTrendDetector`
- `CLIMAX_VOLUME_BAR`: maturity=`standard`, evidence=`hybrid` via `ClimaxVolumeDetector`
- `COPULA_PAIRS_TRADING`: maturity=`standard`, evidence=`statistical` via `CopulaPairsTradingDetector`
- `CORRELATION_BREAKDOWN_EVENT`: maturity=`production`, evidence=`inferred_cross_asset` via `CorrelationBreakdownDetectorV2`
- `CROSS_ASSET_DESYNC_EVENT`: maturity=`production`, evidence=`inferred_cross_asset` via `CrossAssetDesyncDetectorV2`
- `CROSS_VENUE_DESYNC`: maturity=`standard`, evidence=`inferred_cross_asset` via `CrossVenueDesyncDetectorV2`
- `DELEVERAGING_WAVE`: maturity=`standard`, evidence=`hybrid` via `DeleveragingWaveDetector`
- `DEPTH_COLLAPSE`: maturity=`standard`, evidence=`direct` via `DepthCollapseDetectorV2`
- `DEPTH_STRESS_PROXY`: maturity=`standard`, evidence=`hybrid` via `DepthStressProxyDetector`
- `FAILED_CONTINUATION`: maturity=`standard`, evidence=`hybrid` via `FailedContinuationDetector`
- `FALSE_BREAKOUT`: maturity=`standard`, evidence=`hybrid` via `FalseBreakoutDetector`
- `FEE_REGIME_CHANGE_EVENT`: maturity=`standard`, evidence=`direct` via `FeeRegimeChangeDetector`
- `FLOW_EXHAUSTION_PROXY`: maturity=`standard`, evidence=`hybrid` via `FlowExhaustionDetector`
- `FND_DISLOC`: maturity=`production`, evidence=`direct` via `FndDislocDetectorV2`
- `FORCED_FLOW_EXHAUSTION`: maturity=`standard`, evidence=`hybrid` via `ForcedFlowExhaustionDetector`
- `FUNDING_EXTREME_ONSET`: maturity=`standard`, evidence=`direct` via `FundingExtremeOnsetDetectorV2`
- `FUNDING_FLIP`: maturity=`standard`, evidence=`direct` via `FundingFlipDetectorV2`
- `FUNDING_NORMALIZATION_TRIGGER`: maturity=`standard`, evidence=`direct` via `FundingNormalizationDetectorV2`
- `FUNDING_PERSISTENCE_TRIGGER`: maturity=`standard`, evidence=`direct` via `FundingPersistenceDetectorV2`
- `FUNDING_TIMESTAMP_EVENT`: maturity=`standard`, evidence=`contextual` via `FundingTimestampDetector`
- `GAP_OVERSHOOT`: maturity=`standard`, evidence=`statistical` via `GapOvershootDetector`
- `INDEX_COMPONENT_DIVERGENCE`: maturity=`production`, evidence=`inferred_cross_asset` via `IndexComponentDivergenceDetectorV2`
- `LEAD_LAG_BREAK`: maturity=`production`, evidence=`inferred_cross_asset` via `LeadLagBreakDetectorV2`
- `LIQUIDATION_CASCADE`: maturity=`standard`, evidence=`direct` via `LiquidationCascadeDetectorV2`
- `LIQUIDATION_CASCADE_PROXY`: maturity=`standard`, evidence=`proxy` via `LiquidationCascadeProxyDetectorV2`
- `LIQUIDATION_EXHAUSTION_REVERSAL`: maturity=`standard`, evidence=`hybrid` via `LiquidationExhaustionReversalDetector`
- `LIQUIDITY_GAP_PRINT`: maturity=`standard`, evidence=`direct` via `LiquidityGapDetectorV2`
- `LIQUIDITY_SHOCK`: maturity=`standard`, evidence=`hybrid` via `LiquidityShockDetectorV2`
- `LIQUIDITY_STRESS_DIRECT`: maturity=`standard`, evidence=`direct` via `DirectLiquidityStressDetectorV2`
- `LIQUIDITY_STRESS_PROXY`: maturity=`standard`, evidence=`hybrid` via `ProxyLiquidityStressDetectorV2`
- `LIQUIDITY_VACUUM`: maturity=`standard`, evidence=`direct` via `LiquidityVacuumDetectorV2`
- `MOMENTUM_DIVERGENCE_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `MomentumDivergenceDetector`
- `OI_FLUSH`: maturity=`standard`, evidence=`direct` via `OIFlushDetectorV2`
- `OI_SPIKE_NEGATIVE`: maturity=`standard`, evidence=`direct` via `OISpikeNegativeDetectorV2`
- `OI_SPIKE_POSITIVE`: maturity=`standard`, evidence=`direct` via `OISpikePositiveDetectorV2`
- `ORDERFLOW_IMBALANCE_SHOCK`: maturity=`standard`, evidence=`direct` via `OrderflowImbalanceDetector`
- `OVERSHOOT_AFTER_SHOCK`: maturity=`standard`, evidence=`statistical` via `OvershootDetector`
- `POST_DELEVERAGING_REBOUND`: maturity=`standard`, evidence=`hybrid` via `PostDeleveragingReboundDetector`
- `PRICE_VOL_IMBALANCE_PROXY`: maturity=`standard`, evidence=`hybrid` via `PriceVolImbalanceProxyDetector`
- `PULLBACK_PIVOT`: maturity=`standard`, evidence=`hybrid` via `PullbackPivotDetector`
- `RANGE_BREAKOUT`: maturity=`standard`, evidence=`hybrid` via `RangeBreakoutDetector`
- `RANGE_COMPRESSION_END`: maturity=`standard`, evidence=`hybrid` via `RangeCompressionDetectorV2`
- `SCHEDULED_NEWS_WINDOW_EVENT`: maturity=`standard`, evidence=`contextual` via `ScheduledNewsDetector`
- `SEQ_FND_EXTREME_THEN_BREAKOUT`: maturity=`standard`, evidence=`sequence_confirmed` via `SeqFndExtremeThenBreakoutDetector`
- `SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY`: maturity=`standard`, evidence=`sequence_confirmed` via `SeqLiqVacuumThenDepthRecoveryDetector`
- `SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE`: maturity=`standard`, evidence=`sequence_confirmed` via `SeqOiSpikeposThenVolSpikeDetector`
- `SEQ_VOL_COMP_THEN_BREAKOUT`: maturity=`standard`, evidence=`sequence_confirmed` via `SeqVolCompThenBreakoutDetector`
- `SESSION_CLOSE_EVENT`: maturity=`standard`, evidence=`contextual` via `SessionCloseDetector`
- `SESSION_OPEN_EVENT`: maturity=`standard`, evidence=`contextual` via `SessionOpenDetector`
- `SLIPPAGE_SPIKE_EVENT`: maturity=`standard`, evidence=`direct` via `SlippageSpikeDetector`
- `SPOT_PERP_BASIS_SHOCK`: maturity=`production`, evidence=`inferred_cross_asset` via `SpotPerpBasisShockDetectorV2`
- `SPREAD_BLOWOUT`: maturity=`standard`, evidence=`direct` via `SpreadBlowoutDetector`
- `SPREAD_REGIME_WIDENING_EVENT`: maturity=`standard`, evidence=`direct` via `SpreadRegimeWideningDetector`
- `SUPPORT_RESISTANCE_BREAK`: maturity=`standard`, evidence=`hybrid` via `SREventDetector`
- `SWEEP_STOPRUN`: maturity=`standard`, evidence=`hybrid` via `SweepStopRunDetector`
- `TREND_ACCELERATION`: maturity=`standard`, evidence=`hybrid` via `TrendAccelerationDetector`
- `TREND_DECELERATION`: maturity=`standard`, evidence=`hybrid` via `TrendDecelerationDetector`
- `TREND_EXHAUSTION_TRIGGER`: maturity=`standard`, evidence=`hybrid` via `TrendExhaustionDetector`
- `TREND_TO_CHOP_SHIFT`: maturity=`standard`, evidence=`hybrid` via `TrendToChopDetector`
- `VOL_CLUSTER_SHIFT`: maturity=`standard`, evidence=`statistical` via `VolClusterShiftDetectorV2`
- `VOL_REGIME_SHIFT_EVENT`: maturity=`standard`, evidence=`statistical` via `VolRegimeShiftDetectorV2`
- `VOL_RELAXATION_START`: maturity=`standard`, evidence=`direct` via `VolRelaxationStartDetectorV2`
- `VOL_SHOCK`: maturity=`production`, evidence=`statistical` via `VolShockDetectorV2`
- `VOL_SPIKE`: maturity=`production`, evidence=`direct` via `VolSpikeDetectorV2`
- `WICK_REVERSAL_PROXY`: maturity=`standard`, evidence=`hybrid` via `WickReversalProxyDetector`
- `ZSCORE_STRETCH`: maturity=`standard`, evidence=`statistical` via `ZScoreStretchDetector`
