# Event Contract Reference

- Active events: `71`

## BASIS_FUNDING_DISLOCATION

### BASIS_DISLOC

- Detector: `BasisDislocationDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`basis_dislocation` | phase=`shock` | evidence=`statistical` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`basis_dislocation_event` | file=`basis_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 12, 'min_occurrences': 0, 'z_threshold': 5.0, 'lookback_window': 288, 'min_basis_bps': 10.0}`
- Tags: `['basis_dislocation']`
- Notes: Canonical basis dislocation surface.

### CROSS_VENUE_DESYNC

- Detector: `CrossVenueDesyncDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`INFORMATION_DESYNC`
- Shape: subtype=`cross_venue_desync` | phase=`persistence` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`cross_venue_desync_event` | file=`cross_venue_desync_events.parquet` | templates=`['desync_repair', 'convergence', 'basis_repair', 'lead_lag_follow', 'divergence_continuation']` | horizons=`['5m', '15m', '60m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'window_end': 32, 'anchor_quantile': 0.995, 'revert_z': 0.5, 'basis_lookback': 96, 'persistence_bars': 2, 'min_basis_bps': 35.0}`
- Tags: `['desync']`
- Notes: Cross-venue basis persistence folded into basis/funding dislocation.

### FND_DISLOC

- Detector: `FndDislocDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`funding_dislocation` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`funding_dislocation_event` | file=`funding_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 12, 'min_occurrences': 0, 'threshold_bps': 2.0, 'funding_quantile': 0.95, 'alignment_window': 3, 'min_basis_bps': 5.0}`
- Tags: `['basis_dislocation']`
- Notes: Funding dislocation folded into basis/funding regime.

### FUNDING_EXTREME_ONSET

- Detector: `FundingExtremeOnsetDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`funding_extreme` | phase=`onset` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`funding_extreme_onset_event` | file=`funding_episode_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'only_if_regime', 'only_if_highvol', 'tail_risk_avoid']` | horizons=`['5m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'funding_pct_window': 2880, 'accel_lookback': 32, 'persistence_bars': 3, 'normalization_lookback': 96, 'min_event_spacing': 96, 'vol_burst_quantile': 0.9, 'htf_window': 384, 'htf_lookback': 96, 'pre_window': 96, 'post_window': 96, 'phase_tolerance_bars': 16, 'crossover_confirm_bars': 4, 'accel_quantile': 0.995, 'accel_near_zero_cutoff': 5, 'interaction_min_rows_per_year': 150, 'min_hazard_events': 50}`
- Tags: `['funding_crowding']`
- Notes: Funding extreme onset under basis/funding dislocation.

### FUNDING_FLIP

- Detector: `FundingFlipDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`funding_flip` | phase=`flip` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`funding_flip_event` | file=`positioning_extremes_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['5m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'funding_extreme_quantile': 0.99, 'oi_change_z_threshold': 3.0, 'min_flip_abs': 0.00025, 'persistence_bars': 2, 'min_spacing': 24}`
- Tags: `['funding_crowding', 'high_urgency']`
- Notes: Funding sign inversion as subtype metadata.

### FUNDING_NORMALIZATION_TRIGGER

- Detector: `FundingNormalizationDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`funding_normalization` | phase=`normalization` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`funding_normalization_event` | file=`funding_episode_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'funding_pct_window': 2880, 'accel_lookback': 32, 'persistence_bars': 12, 'normalization_lookback': 96, 'min_prior_extreme_abs': 0.0004, 'min_event_spacing': 96, 'vol_burst_quantile': 0.9, 'htf_window': 384, 'htf_lookback': 96, 'pre_window': 96, 'post_window': 96, 'phase_tolerance_bars': 16, 'crossover_confirm_bars': 4, 'accel_quantile': 0.995, 'accel_near_zero_cutoff': 5, 'interaction_min_rows_per_year': 150, 'min_hazard_events': 50}`
- Tags: `['funding_crowding']`
- Notes: Normalization phase of funding dislocation.

### FUNDING_PERSISTENCE_TRIGGER

- Detector: `FundingPersistenceDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`funding_persistence` | phase=`persistence` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`funding_persistence_event` | file=`funding_episode_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid', 'only_if_regime']` | horizons=`['5m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'funding_pct_window': 2880, 'accel_lookback': 32, 'persistence_bars': 12, 'normalization_lookback': 96, 'min_event_spacing': 576, 'min_spacing': 576, 'vol_burst_quantile': 0.9, 'htf_window': 384, 'htf_lookback': 96, 'pre_window': 96, 'post_window': 96, 'phase_tolerance_bars': 16, 'crossover_confirm_bars': 4, 'accel_quantile': 0.995, 'accel_near_zero_cutoff': 5, 'interaction_min_rows_per_year': 150, 'min_hazard_events': 50}`
- Tags: `['funding_crowding']`
- Notes: Persistence phase of funding dislocation.

### SEQ_FND_EXTREME_THEN_BREAKOUT

- Detector: `EventSequenceDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`funding_extreme_then_breakout` | phase=`confirmation` | evidence=`sequence_confirmed` | layer=`composite` | disposition=`demote`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`True` | composite=`True` | context_tag=`False`
- Runtime config: signal=`seq_fnd_extreme_then_breakout_event` | file=`event_sequences.parquet` | templates=`[]` | horizons=`['5m', '60m']` | max_candidates=`250`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 24, 'min_occurrences': 0, 'max_gap_bars': 96}`
- Tags: `['funding_crowding', 'sequence']`
- Notes: Composite research hypothesis, not a canonical event.

### SPOT_PERP_BASIS_SHOCK

- Detector: `SpotPerpBasisShockDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`INFORMATION_DESYNC`
- Shape: subtype=`spot_perp_basis_shock` | phase=`shock` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`cross_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`spot_perp_basis_shock_event` | file=`information_desync_events.parquet` | templates=`['desync_repair', 'convergence', 'basis_repair', 'lead_lag_follow', 'divergence_continuation']` | horizons=`['5m', '15m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'z_threshold': 5.0, 'shock_change_quantile': 0.95, 'lead_lag_window': 60, 'min_basis_bps': 15.0}`
- Tags: `['desync', 'basis_dislocation', 'high_urgency']`
- Notes: Spot/perp basis shock is a basis/funding subtype.

## CROSS_ASSET_DESYNCHRONIZATION

### CROSS_ASSET_DESYNC_EVENT

- Detector: `CrossAssetDesyncDetector` | enabled=`True` | band=`context_only`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`INFORMATION_DESYNC`
- Shape: subtype=`cross_asset_desync` | phase=`divergence` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`keep`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`cross_asset_desync_event` | file=`cross_asset_desync_event_events.parquet` | templates=`['desync_repair', 'convergence', 'basis_repair', 'lead_lag_follow', 'divergence_continuation']` | horizons=`['5m', '15m', '60m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 24, 'min_occurrences': 0, 'lookback_window': 2880, 'threshold_z': 3.0, 'min_pair_observations': 96}`
- Tags: `['desync']`
- Notes: First-class cross-asset desynchronization regime.

### INDEX_COMPONENT_DIVERGENCE

- Detector: `IndexComponentDivergenceDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`INFORMATION_DESYNC`
- Shape: subtype=`index_component_divergence` | phase=`divergence` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`keep`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`index_component_divergence_event` | file=`information_desync_events.parquet` | templates=`['desync_repair', 'convergence', 'basis_repair', 'lead_lag_follow', 'divergence_continuation']` | horizons=`['5m', '15m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'desync_z_threshold': 3.0, 'lead_lag_window': 60}`
- Tags: `['desync']`
- Notes: Cross-asset divergence between index and components.

### LEAD_LAG_BREAK

- Detector: `LeadLagBreakDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`INFORMATION_DESYNC`
- Shape: subtype=`lead_lag_break` | phase=`divergence` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`keep`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`lead_lag_break_event` | file=`information_desync_events.parquet` | templates=`['desync_repair', 'convergence', 'basis_repair', 'lead_lag_follow', 'divergence_continuation']` | horizons=`['5m', '15m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'desync_z_threshold': 3.0, 'lead_lag_window': 60}`
- Tags: `['desync']`
- Notes: Lead/lag structure break.

## EXECUTION_FRICTION

### FEE_REGIME_CHANGE_EVENT

- Detector: `FeeRegimeChangeDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`EXECUTION_FRICTION`
- Shape: subtype=`fee_regime_change` | phase=`transition` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`fee_regime_change_event` | file=`execution_friction_events.parquet` | templates=`['slippage_aware_filter', 'tail_risk_avoid']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'spread_z_threshold': 3.0, 'slippage_z_threshold': 3.0}`
- Tags: `['execution_cost']`
- Notes: Execution-cost regime shift.

### SLIPPAGE_SPIKE_EVENT

- Detector: `SlippageSpikeDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`EXECUTION_FRICTION`
- Shape: subtype=`slippage_spike` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`slippage_spike_event` | file=`execution_friction_events.parquet` | templates=`['slippage_aware_filter', 'tail_risk_avoid']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'spread_z_threshold': 3.0, 'slippage_z_threshold': 3.0}`
- Tags: `['execution_cost', 'high_urgency']`
- Notes: Execution friction shock.

### SPREAD_REGIME_WIDENING_EVENT

- Detector: `SpreadRegimeWideningDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`EXECUTION_FRICTION`
- Shape: subtype=`spread_regime_widening` | phase=`persistence` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`spread_regime_widening_event` | file=`execution_friction_events.parquet` | templates=`['slippage_aware_filter', 'tail_risk_avoid']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'trend_window': 24, 'lookback_window': 2880, 'min_periods': 288, 'low_volume_quantile': 0.25, 'min_spacing': 48}`
- Tags: `['execution_cost', 'liquidity_stress']`
- Notes: Persistent execution-friction regime widening.

## LIQUIDATION_CASCADE

### LIQUIDATION_CASCADE

- Detector: `LiquidationCascadeDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`liquidation_cascade` | phase=`cascade` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidation_cascade_event` | file=`liquidation_cascade_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['5m', '15m', '60m']` | max_candidates=`1000`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'liq_multiplier': 3.0, 'median_window': 288, 'min_periods': 24, 'max_gap': 6}`
- Tags: `['forced_liquidation', 'high_urgency']`
- Notes: Structurally distinct canonical cascade regime.

### LIQUIDATION_CASCADE_PROXY

- Detector: `LiquidationCascadeProxyDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`liquidation_cascade_proxy` | phase=`onset` | evidence=`proxy` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidation_cascade_proxy_event` | file=`liquidation_cascade_proxy_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['5m', '15m', '60m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'oi_window': 288, 'vol_window': 288, 'min_periods': 24, 'oi_drop_quantile': 0.997, 'vol_surge_quantile': 0.9, 'ret_window': 3, 'min_episode_oi_reduction_pct': 0.0, 'max_gap': 1}`
- Tags: `['oi_dynamic', 'liquidation_proxy']`
- Notes: Use when liquidation_notional data is unavailable (e.g., Bybit native data without cross-exchange liquidation feed). Expected to have lower precision than LIQUIDATION_CASCADE but comparable recall for large cascade events.

## LIQUIDITY_STRESS

### ABSORPTION_PROXY

- Detector: `AbsorptionProxyDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`absorption` | phase=`persistence` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`absorption_proxy_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 96, 'min_occurrences': 0, 'window': 288, 'spread_quantile': 0.965, 'rv_quantile': 0.9, 'imbalance_abs_quantile': 0.25, 'min_history_bars': 288}`
- Notes: Hybrid liquidity-stress evidence: direct imbalance-stall confirmation combined with spread and RV stress.

### DEPTH_COLLAPSE

- Detector: `DepthCollapseDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`depth_collapse` | phase=`collapse` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`depth_collapse_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2}`
- Tags: `['liquidity_stress']`
- Notes: Direct depth failure evidence for liquidity stress.

### DEPTH_STRESS_PROXY

- Detector: `DepthStressProxyDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`depth_collapse` | phase=`collapse` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`depth_stress_proxy_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 96, 'min_occurrences': 0, 'window': 288, 'spread_quantile': 0.99, 'rv_quantile': 0.9, 'depth_quantile': 0.93, 'min_history_bars': 288}`
- Notes: Hybrid liquidity-stress evidence: direct micro-depth depletion combined with spread and RV stress.

### LIQUIDITY_GAP_PRINT

- Detector: `LiquidityGapDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`gap_print` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidity_gap_print_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['12b', '24b', '48b']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2}`
- Tags: `['liquidity_stress']`
- Notes: Gap print inside liquidity-stress regime.

### LIQUIDITY_SHOCK

- Detector: `LiquidityStressDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`liquidity_shock` | phase=`shock` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidity_shock_event` | file=`liquidity_shock_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'median_window': 288}`
- Tags: `['liquidity_stress', 'high_urgency']`
- Notes: High-level liquidity stress onset.

### LIQUIDITY_STRESS_DIRECT

- Detector: `DirectLiquidityStressDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`liquidity_stress` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidity_stress_direct_event` | file=`liquidity_shock_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'median_window': 288}`
- Notes: Direct evidence mode for liquidity stress.

### LIQUIDITY_STRESS_PROXY

- Detector: `ProxyLiquidityStressDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`liquidity_stress` | phase=`shock` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidity_stress_proxy_event` | file=`liquidity_shock_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'median_window': 288}`
- Notes: Hybrid liquidity-stress evidence: direct book stress when available plus bar-range expansion and optional trade-flow collapse confirmation.

### LIQUIDITY_VACUUM

- Detector: `LiquidityVacuumDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`liquidity_vacuum` | phase=`collapse` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidity_vacuum_event` | file=`liquidity_vacuum_events.parquet` | templates=`['mean_reversion', 'stop_run_repair', 'overshoot_repair', 'continuation', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`1000`
- Thresholds: `{'merge_gap_bars': 2, 'cooldown_bars': 12, 'min_occurrences': 3, 'min_events_calibration': 1, 'shock_quantiles': '0.5,0.6,0.7', 'shock_quantile': 0.99, 'shock_threshold_mode': 'rolling', 'volume_window': 24, 'range_window': 24, 'vol_ratio_floor': 0.95, 'range_multiplier': 1.05, 'min_vacuum_bars': 1, 'max_vacuum_bars': 96, 'post_horizon_bars': 96, 'auc_horizon_bars': 96, 'range_expansion_threshold': 0.02}`
- Tags: `['liquidity_stress', 'high_urgency']`
- Notes: Thin-book vacuum regime.

### ORDERFLOW_IMBALANCE_SHOCK

- Detector: `OrderflowImbalanceDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`orderflow_imbalance` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`orderflow_imbalance_shock_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2, 'ret_quantile': 0.998, 'rv_quantile': 0.92, 'vol_quantile': 0.92, 'ret_window': 288, 'rv_window': 288, 'vol_window': 288, 'min_history_bars': 288}`
- Tags: `['orderflow_imbalance']`
- Notes: Orderflow imbalance is treated as a liquidity-stress subtype.

### PRICE_VOL_IMBALANCE_PROXY

- Detector: `PriceVolImbalanceProxyDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`price_vol_imbalance` | phase=`shock` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`price_vol_imbalance_proxy_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'ret_quantile': 0.995, 'rv_quantile': 0.9, 'volume_quantile': 0.9, 'ret_window': 288, 'rv_window': 288, 'vol_window': 288, 'min_history_bars': 288, 'min_spacing': 24}`
- Notes: Hybrid liquidity-stress evidence via synchronized return, realized-volatility, volume, and flow-pressure imbalance.

### SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY

- Detector: `EventSequenceDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`liquidity_vacuum_then_depth_recovery` | phase=`confirmation` | evidence=`sequence_confirmed` | layer=`composite` | disposition=`demote`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`True` | composite=`True` | context_tag=`False`
- Runtime config: signal=`seq_liq_vacuum_then_depth_recovery_event` | file=`event_sequences.parquet` | templates=`[]` | horizons=`['15m']` | max_candidates=`400`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 24, 'min_occurrences': 0, 'max_gap_bars': 48}`
- Tags: `['liquidity_stress', 'absorption_recovery', 'sequence']`
- Notes: Composite liquidity repair motif.

### SPREAD_BLOWOUT

- Detector: `SpreadBlowoutDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`spread_widening` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`spread_blowout_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2}`
- Tags: `['liquidity_stress', 'execution_cost']`
- Notes: Spread blowout retained as liquidity-stress subtype.

### SWEEP_STOPRUN

- Detector: `StopRunDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`sweep_stoprun` | phase=`shock` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`sweep_stoprun_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'continuation', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`600`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2}`
- Tags: `['orderflow_imbalance', 'high_urgency']`
- Notes: Stop-run style liquidity stress.

### WICK_REVERSAL_PROXY

- Detector: `WickReversalProxyDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`LIQUIDITY_DISLOCATION`
- Shape: subtype=`wick_reversal` | phase=`recovery` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`wick_reversal_proxy_event` | file=`liquidity_dislocation_events.parquet` | templates=`['mean_reversion', 'stop_run_repair', 'overshoot_repair', 'only_if_liquidity', 'slippage_aware_filter']` | horizons=`['15m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'z_threshold': 3.0, 'min_volume_quantile': 0.2}`
- Notes: Hybrid liquidity-stress recovery evidence: wick and reclaim structure confirmed by realized-volume expansion.

## POSITIONING_EXPANSION

### OI_SPIKE_NEGATIVE

- Detector: `OISpikeNegativeDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`oi_spike_negative` | phase=`expansion` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`oi_spike_neg_event` | file=`oi_shock_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['5m', '60m']` | max_candidates=`250`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'oi_window': 96, 'spike_z_th': 2.5}`
- Tags: `['oi_dynamic']`
- Notes: Direction stays in subtype; canonical regime is positioning expansion.

### OI_SPIKE_POSITIVE

- Detector: `OISpikePositiveDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`oi_spike_positive` | phase=`expansion` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`oi_spike_pos_event` | file=`oi_shock_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`250`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'oi_window': 96, 'spike_z_th': 2.5}`
- Tags: `['oi_dynamic']`
- Notes: Direction stays in subtype; canonical regime is positioning expansion.

### SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE

- Detector: `EventSequenceDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`oi_spike_positive_then_vol_spike` | phase=`confirmation` | evidence=`sequence_confirmed` | layer=`composite` | disposition=`demote`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`True` | composite=`True` | context_tag=`False`
- Runtime config: signal=`seq_oi_spikepos_then_vol_spike_event` | file=`event_sequences.parquet` | templates=`[]` | horizons=`['5m', '60m']` | max_candidates=`250`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 24, 'min_occurrences': 0, 'max_gap_bars': 48}`
- Tags: `['oi_dynamic', 'sequence']`
- Notes: Composite positioning/volatility hypothesis.

## POSITIONING_UNWIND_DELEVERAGING

### DELEVERAGING_WAVE

- Detector: `DeleveragingWaveDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`deleveraging_wave` | phase=`unwind` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`deleveraging_wave_event` | file=`positioning_extremes_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'tail_risk_avoid']` | horizons=`['5m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'funding_extreme_quantile': 0.99, 'oi_change_z_threshold': 3.0}`
- Tags: `['forced_liquidation', 'oi_dynamic']`
- Notes: Positioning unwind regime with explicit deleveraging dynamics.

### OI_FLUSH

- Detector: `OIFlushDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`POSITIONING_EXTREMES`
- Shape: subtype=`oi_flush` | phase=`unwind` | evidence=`direct` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`oi_flush_event` | file=`oi_shock_events.parquet` | templates=`['reversal_or_squeeze', 'mean_reversion', 'continuation', 'exhaustion_reversal', 'convexity_capture', 'only_if_funding', 'only_if_oi', 'only_if_regime', 'only_if_highvol', 'tail_risk_avoid']` | horizons=`['5m', '60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'oi_window': 96, 'spike_z_th': 2.5}`
- Tags: `['oi_dynamic', 'high_urgency']`
- Notes: OI flush folded into unwind/deleveraging regime.

### POST_DELEVERAGING_REBOUND

- Detector: `PostDeleveragingReboundDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`post_deleveraging_rebound` | phase=`recovery` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`post_deleveraging_rebound_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`400`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'threshold_window': 2880, 'oi_drop_quantile': 0.8, 'liquidation_quantile': 0.85, 'spread_quantile': 0.7, 'return_quantile': 0.75, 'wick_quantile': 0.7, 'rebound_window': 6, 'rebound_quantile': 0.7, 'reversal_window': 3, 'reversal_quantile': 0.65, 'oi_drop_abs_min': 5.0, 'liquidation_abs_min': 25.0, 'liquidation_multiplier': 0.9, 'return_abs_min': 0.0025, 'spread_abs_min': 5.0, 'cluster_window': 12, 'rebound_window_bars': 12, 'post_cluster_lookback': 48, 'rv_peak_decay_ratio': 1.01, 'liq_cooldown_ratio': 0.55, 'liquidation_cooldown_abs_max': 100000.0, 'rebound_return_min': 0.001, 'wick_ratio_min': 0.55}`
- Notes: Recovery phase following deleveraging wave.

## REGIME_TRANSITION

### BETA_SPIKE_EVENT

- Detector: `BetaSpikeDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`REGIME_TRANSITION`
- Shape: subtype=`beta_spike` | phase=`transition` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`keep`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`beta_spike_event` | file=`regime_transition_events.parquet` | templates=`['only_if_regime', 'continuation', 'mean_reversion', 'drawdown_filter', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'regime_window': 288, 'transition_z_threshold': 2.5}`
- Tags: `['regime_shift']`
- Notes: Cross-asset transition evidence via beta instability.

### CHOP_TO_TREND_SHIFT

- Detector: `ChopToTrendDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`REGIME_TRANSITION`
- Shape: subtype=`chop_to_trend` | phase=`transition` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`chop_to_trend_shift_event` | file=`regime_transition_events.parquet` | templates=`['only_if_regime', 'continuation', 'mean_reversion', 'drawdown_filter', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'regime_window': 288, 'transition_z_threshold': 2.5}`
- Tags: `['regime_shift', 'trend_momentum']`
- Notes: Transition from choppy to directional conditions.

### CORRELATION_BREAKDOWN_EVENT

- Detector: `CorrelationBreakdownDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`REGIME_TRANSITION`
- Shape: subtype=`correlation_breakdown` | phase=`transition` | evidence=`inferred_cross_asset` | layer=`canonical` | disposition=`keep`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`correlation_breakdown_event` | file=`regime_transition_events.parquet` | templates=`['only_if_regime', 'continuation', 'mean_reversion', 'drawdown_filter', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'regime_window': 288, 'transition_z_threshold': 2.5}`
- Tags: `['regime_shift']`
- Notes: Cross-asset regime transition via correlation instability.

### TREND_TO_CHOP_SHIFT

- Detector: `TrendToChopDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`REGIME_TRANSITION`
- Shape: subtype=`trend_to_chop` | phase=`transition` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`trend_to_chop_shift_event` | file=`regime_transition_events.parquet` | templates=`['only_if_regime', 'continuation', 'mean_reversion', 'drawdown_filter', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'regime_window': 288, 'transition_z_threshold': 2.5}`
- Tags: `['regime_shift', 'trend_momentum']`
- Notes: Transition from directional to choppy conditions.

### VOL_REGIME_SHIFT_EVENT

- Detector: `VolRegimeShiftDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`REGIME_TRANSITION`
- Shape: subtype=`vol_regime_shift` | phase=`transition` | evidence=`statistical` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`vol_regime_shift_event` | file=`regime_transition_events.parquet` | templates=`['only_if_regime', 'continuation', 'mean_reversion', 'drawdown_filter', 'tail_risk_avoid']` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'regime_window': 288, 'transition_z_threshold': 2.5}`
- Tags: `['regime_shift', 'vol_regime']`
- Notes: Regime transition expressed through volatility-state change.

## SCHEDULED_TEMPORAL_WINDOW

### FUNDING_TIMESTAMP_EVENT

- Detector: `FundingTimestampDetector` | enabled=`True` | band=`context_only`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`TEMPORAL_STRUCTURE`
- Shape: subtype=`funding_timestamp` | phase=`window` | evidence=`contextual` | layer=`context_tag` | disposition=`demote`
- Scope: asset=`multi_asset` | venue=`market_wide` | research_only=`False` | composite=`False` | context_tag=`True`
- Runtime config: signal=`funding_timestamp_event` | file=`temporal_structure_events.parquet` | templates=`['mean_reversion', 'continuation']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'event_spacing_bars': 288, 'funding_abs_quantile': 0.0}`
- Tags: `['temporal_anchor', 'funding_crowding']`
- Notes: Mechanical funding window context; not a canonical regime.

### SCHEDULED_NEWS_WINDOW_EVENT

- Detector: `ScheduledNewsDetector` | enabled=`True` | band=`context_only`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`TEMPORAL_STRUCTURE`
- Shape: subtype=`scheduled_news_window` | phase=`window` | evidence=`contextual` | layer=`context_tag` | disposition=`demote`
- Scope: asset=`multi_asset` | venue=`market_wide` | research_only=`False` | composite=`False` | context_tag=`True`
- Runtime config: signal=`scheduled_news_window_event` | file=`temporal_structure_events.parquet` | templates=`['mean_reversion', 'continuation']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'event_spacing_bars': 288}`
- Tags: `['temporal_anchor']`
- Notes: Context gate for scheduled news windows.

### SESSION_CLOSE_EVENT

- Detector: `SessionCloseDetector` | enabled=`True` | band=`context_only`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`TEMPORAL_STRUCTURE`
- Shape: subtype=`session_close` | phase=`window` | evidence=`contextual` | layer=`context_tag` | disposition=`demote`
- Scope: asset=`multi_asset` | venue=`market_wide` | research_only=`False` | composite=`False` | context_tag=`True`
- Runtime config: signal=`session_close_event` | file=`temporal_structure_events.parquet` | templates=`['mean_reversion', 'continuation']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'event_spacing_bars': 288, 'session_range_quantile': 0.0, 'session_vol_z_min': None}`
- Tags: `['temporal_anchor']`
- Notes: Session close context tag.

### SESSION_OPEN_EVENT

- Detector: `SessionOpenDetector` | enabled=`True` | band=`context_only`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`TEMPORAL_STRUCTURE`
- Shape: subtype=`session_open` | phase=`window` | evidence=`contextual` | layer=`context_tag` | disposition=`demote`
- Scope: asset=`multi_asset` | venue=`market_wide` | research_only=`False` | composite=`False` | context_tag=`True`
- Runtime config: signal=`session_open_event` | file=`temporal_structure_events.parquet` | templates=`['mean_reversion', 'continuation']` | horizons=`['5m', '15m']` | max_candidates=`300`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'event_spacing_bars': 288, 'session_range_quantile': 0.0, 'session_vol_z_min': None}`
- Tags: `['temporal_anchor']`
- Notes: Session open context tag.

## STATISTICAL_STRETCH_OVERSHOOT

### BAND_BREAK

- Detector: `BandBreakDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`band_break` | phase=`breakout` | evidence=`statistical` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`band_break_event` | file=`statistical_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'band_z_threshold': 3.0}`
- Tags: `['statistical_stretch']`
- Notes: Statistical band escape; retained as subtype under stretch/overshoot.

### COPULA_PAIRS_TRADING

- Detector: `CopulaPairsTradingDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`copula_pairs_trading` | phase=`strategy` | evidence=`statistical` | layer=`strategy_construct` | disposition=`demote`
- Scope: asset=`cross_asset` | venue=`multi_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`copula_pairs_trading_event` | file=`copula_pairs_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288}`
- Tags: `['basis_dislocation']`
- Notes: Relative-value strategy/search construct, not a canonical market event.

### GAP_OVERSHOOT

- Detector: `GapOvershootDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`gap_overshoot` | phase=`overshoot` | evidence=`statistical` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`gap_overshoot_event` | file=`statistical_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'band_z_threshold': 3.0, 'return_quantile': 0.995}`
- Tags: `['statistical_stretch']`
- Notes: Gap-extension overshoot state.

### OVERSHOOT_AFTER_SHOCK

- Detector: `OvershootDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`overshoot_after_shock` | phase=`overshoot` | evidence=`statistical` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`overshoot_after_shock_event` | file=`statistical_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'band_z_threshold': 3.0, 'price_quantile': 0.95, 'rv_quantile': 0.95}`
- Tags: `['statistical_stretch']`
- Notes: Statistical overshoot conditional on preceding shock.

### ZSCORE_STRETCH

- Detector: `ZScoreStretchDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`STATISTICAL_DISLOCATION`
- Shape: subtype=`zscore_stretch` | phase=`stretch` | evidence=`statistical` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`zscore_stretch_event` | file=`statistical_dislocation_events.parquet` | templates=`['mean_reversion', 'overshoot_repair', 'tail_risk_avoid']` | horizons=`['15m', '60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'lookback_window': 288, 'band_z_threshold': 3.0, 'zscore_quantile': 0.96}`
- Tags: `['statistical_stretch']`
- Notes: Canonical stretch/overshoot event.

## TREND_CONTINUATION

### PULLBACK_PIVOT

- Detector: `PullbackPivotDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`pullback_pivot` | phase=`recovery` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`pullback_pivot_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'trend_window': 288, 'breakout_z_threshold': 2.5}`
- Tags: `['trend_momentum']`
- Notes: Pullback continuation setup.

### RANGE_BREAKOUT

- Detector: `RangeBreakoutDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`range_breakout` | phase=`breakout` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`range_breakout_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'trend_window': 288, 'breakout_z_threshold': 2.5}`
- Tags: `['trend_momentum']`
- Notes: Directional breakout retained under trend continuation.

### SUPPORT_RESISTANCE_BREAK

- Detector: `SREventDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`support_resistance_break` | phase=`breakout` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`support_resistance_break_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'trend_window': 288, 'breakout_z_threshold': 2.5}`
- Tags: `['trend_momentum']`
- Notes: Structural breakout subtype within trend continuation.

### TREND_ACCELERATION

- Detector: `TrendAccelerationDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`trend_acceleration` | phase=`persistence` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`trend_acceleration_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 0, 'min_occurrences': 0, 'trend_window': 96, 'breakout_z_threshold': 2.5, 'min_spacing': 192}`
- Tags: `['trend_momentum']`
- Notes: Continuation regime with accelerating trend.

## TREND_FAILURE_EXHAUSTION

### CLIMAX_VOLUME_BAR

- Detector: `ClimaxVolumeDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`climax_volume` | phase=`exhaustion` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`climax_volume_bar_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 48, 'min_occurrences': 0, 'liquidation_spike_z_threshold': 3.0, 'cascade_window_bars': 12, 'vol_quantile': 0.992, 'ret_quantile': 0.998, 'range_quantile': 0.995}`
- Tags: `['exhaustion', 'high_urgency']`
- Notes: Exhaustion marker rather than standalone family.

### FAILED_CONTINUATION

- Detector: `FailedContinuationDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`failed_continuation` | phase=`failure` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`failed_continuation_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'breakout_window': 48, 'reversal_window': 12, 'breakout_strength_min': 0.003}`
- Tags: `['exhaustion']`
- Notes: Trend-follow signal failure belongs in exhaustion/failure regime.

### FALSE_BREAKOUT

- Detector: `FalseBreakoutDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`false_breakout` | phase=`failure` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`false_breakout_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'trend_window': 288, 'max_range_ratio': 8.0, 'breakout_z_threshold': 2.5}`
- Tags: `['trend_momentum', 'exhaustion']`
- Notes: Breakout failure surface under trend failure/exhaustion.

### FLOW_EXHAUSTION_PROXY

- Detector: `FlowExhaustionDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`flow_exhaustion` | phase=`exhaustion` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`flow_exhaustion_proxy_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'threshold_window': 2880, 'oi_drop_quantile': 0.8, 'liquidation_quantile': 0.85, 'spread_quantile': 0.7, 'return_quantile': 0.75, 'rebound_window': 6, 'reversal_window': 3, 'reversal_quantile': 0.65, 'oi_drop_abs_min': 5.0, 'liquidation_abs_min': 25.0, 'liquidation_multiplier': 0.9, 'return_abs_min': 0.0025, 'spread_abs_min': 5.0, 'rv_decay_ratio': 0.99, 'lookback_window': 288, 'z_threshold': 3.0, 'min_spacing': 24}`
- Notes: Hybrid exhaustion evidence: liquidation and OI unwind confirmation combined with price and volatility exhaustion.

### FORCED_FLOW_EXHAUSTION

- Detector: `ForcedFlowExhaustionDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`forced_flow_exhaustion` | phase=`exhaustion` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`forced_flow_exhaustion_event` | file=`directional_exhaustion_after_forced_flow_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'threshold_window': 2880, 'oi_drop_quantile': 0.88, 'liquidation_quantile': 0.92, 'spread_quantile': 0.7, 'return_quantile': 0.8, 'rebound_window': 6, 'reversal_window': 3, 'reversal_quantile': 0.65, 'oi_drop_abs_min': 5.0, 'liquidation_abs_min': 25.0, 'liquidation_multiplier': 0.9, 'return_abs_min': 0.0025, 'spread_abs_min': 5.0, 'rv_decay_ratio': 0.99, 'min_spacing': 32, 'window_end': 32, 'anchor_quantile': 0.99}`
- Tags: `['exhaustion', 'forced_liquidation']`
- Notes: Exhaustion regime with forced-flow mechanics.

### LIQUIDATION_EXHAUSTION_REVERSAL

- Detector: `PostDeleveragingReboundDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`liquidation_exhaustion_reversal` | phase=`recovery` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`liquidation_exhaustion_reversal_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`400`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 24, 'min_occurrences': 0, 'threshold_window': 2880, 'oi_drop_quantile': 0.8, 'liquidation_quantile': 0.85, 'spread_quantile': 0.7, 'return_quantile': 0.75, 'wick_quantile': 0.7, 'rebound_window': 6, 'rebound_quantile': 0.7, 'reversal_window': 3, 'reversal_quantile': 0.65, 'oi_drop_abs_min': 5.0, 'liquidation_abs_min': 25.0, 'liquidation_multiplier': 0.9, 'return_abs_min': 0.0025, 'spread_abs_min': 5.0, 'cluster_window': 12, 'rebound_window_bars': 12, 'post_cluster_lookback': 48, 'rv_peak_decay_ratio': 1.01, 'liq_cooldown_ratio': 0.55, 'liquidation_cooldown_abs_max': 100000.0, 'rebound_return_min': 0.001, 'wick_ratio_min': 0.55}`
- Tags: `['forced_liquidation']`
- Notes: Reversal after forced liquidation exhaustion.

### MOMENTUM_DIVERGENCE_TRIGGER

- Detector: `MomentumDivergenceDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`momentum_divergence` | phase=`exhaustion` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`momentum_divergence_trigger_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 96, 'min_occurrences': 0, 'slow_trend_quantile': 0.7, 'max_trend_persistence_bars': 72, 'liquidation_spike_z_threshold': 3.0, 'cascade_window_bars': 12}`
- Tags: `['exhaustion']`
- Notes: Divergence-based exhaustion signal.

### TREND_DECELERATION

- Detector: `TrendDecelerationDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`TREND_STRUCTURE`
- Shape: subtype=`trend_deceleration` | phase=`exhaustion` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`trend_deceleration_event` | file=`trend_structure_events.parquet` | templates=`['breakout_followthrough', 'false_breakout_reversal', 'pullback_entry', 'trend_continuation', 'continuation', 'only_if_trend']` | horizons=`['15m', '60m']` | max_candidates=`450`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'trend_window': 288, 'breakout_z_threshold': 2.5}`
- Tags: `['trend_momentum']`
- Notes: Deceleration belongs with trend failure/exhaustion.

### TREND_EXHAUSTION_TRIGGER

- Detector: `TrendExhaustionDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`FORCED_FLOW_AND_EXHAUSTION`
- Shape: subtype=`trend_exhaustion` | phase=`onset` | evidence=`hybrid` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`trend_exhaustion_trigger_event` | file=`forced_flow_and_exhaustion_events.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 96, 'min_occurrences': 0, 'context_min_confidence': 0.55, 'context_max_entropy': 0.9, 'trend_window': 96, 'vol_window': 288, 'slope_fast_window': 12, 'slope_slow_window': 48, 'pullback_window': 96, 'threshold_window': 2880, 'trend_quantile': 0.95, 'cooldown_quantile': 0.35, 'pullback_quantile': 0.7, 'reversal_window': 3, 'reversal_quantile': 0.65, 'trend_peak_multiplier': 1.3, 'trend_strength_ratio': 3.0, 'min_trend_duration_bars': 72, 'cooldown_ratio': 0.9, 'reversal_alignment_window': 3, 'liquidation_spike_z_threshold': 3.0, 'cascade_window_bars': 12}`
- Tags: `['exhaustion', 'trend_momentum']`
- Notes: Canonical exhaustion onset.

## VOLATILITY_EXPANSION

### VOL_CLUSTER_SHIFT

- Detector: `VolClusterShiftDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`vol_cluster_shift` | phase=`persistence` | evidence=`statistical` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`vol_cluster_shift_event` | file=`volatility_transition_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation', 'volatility_expansion_follow', 'pullback_entry', 'only_if_regime']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'vol_lookback_window': 288, 'compression_quantile': 0.1, 'expansion_z_threshold': 2.5}`
- Tags: `['vol_regime']`
- Notes: Volatility cluster migration under expansion regime.

### VOL_SPIKE

- Detector: `VolSpikeDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`vol_spike` | phase=`shock` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`vol_spike_event` | file=`volatility_transition_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation', 'volatility_expansion_follow', 'pullback_entry', 'only_if_regime']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 6, 'min_occurrences': 5}`
- Tags: `['vol_regime', 'high_urgency']`
- Notes: Canonical realized-volatility expansion onset.

## VOLATILITY_RELAXATION_COMPRESSION_RELEASE

### BREAKOUT_TRIGGER

- Detector: `BreakoutTriggerDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`breakout_trigger` | phase=`breakout` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`breakout_trigger_event` | file=`volatility_transition_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation', 'volatility_expansion_follow', 'pullback_entry', 'only_if_regime']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'vol_lookback_window': 96, 'compression_ratio_max': 0.8, 'compression_window': 6, 'min_breakout_distance': 0.0015, 'expansion_quantile': 0.85}`
- Tags: `['vol_regime', 'trend_momentum']`
- Notes: Compression-release bridge event; not a standalone canonical regime.

### RANGE_COMPRESSION_END

- Detector: `RangeCompressionDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`range_compression_end` | phase=`breakout` | evidence=`hybrid` | layer=`canonical` | disposition=`merge`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`range_compression_end_event` | file=`volatility_transition_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation', 'volatility_expansion_follow', 'pullback_entry', 'only_if_regime']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'vol_lookback_window': 288, 'compression_quantile': 0.1, 'expansion_z_threshold': 2.5}`
- Tags: `['vol_regime']`
- Notes: Compression-release subtype rather than standalone family.

### SEQ_VOL_COMP_THEN_BREAKOUT

- Detector: `EventSequenceDetector` | enabled=`True` | band=`composite_or_fragile`
- Eligibility: planning=`False` | runtime=`False` | promotion=`False` | primary_anchor=`False` | legacy_default_executable=`False`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`vol_compression_then_breakout` | phase=`confirmation` | evidence=`sequence_confirmed` | layer=`composite` | disposition=`demote`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`True` | composite=`True` | context_tag=`False`
- Runtime config: signal=`seq_vol_comp_then_breakout_event` | file=`event_sequences.parquet` | templates=`[]` | horizons=`['60m']` | max_candidates=`350`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 24, 'min_occurrences': 0, 'max_gap_bars': 48}`
- Tags: `['vol_regime', 'sequence']`
- Notes: Composite compression-release hypothesis.

### VOL_RELAXATION_START

- Detector: `VolRelaxationDetector` | enabled=`True` | band=`research_trigger`
- Eligibility: planning=`True` | runtime=`False` | promotion=`True` | primary_anchor=`False` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`vol_relaxation_start` | phase=`normalization` | evidence=`direct` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`vol_relaxation_start_event` | file=`volatility_transition_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation', 'volatility_expansion_follow', 'pullback_entry', 'only_if_regime']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 0, 'cooldown_bars': 12, 'min_occurrences': 0, 'vol_lookback_window': 288, 'compression_quantile': 0.1, 'expansion_z_threshold': 2.5}`
- Tags: `['vol_regime']`
- Notes: Start of volatility normalization after expansion.

## VOLATILITY_TRANSITION

### VOL_SHOCK

- Detector: `VolShockRelaxationDetector` | enabled=`True` | band=`deployable_core`
- Eligibility: planning=`True` | runtime=`True` | promotion=`True` | primary_anchor=`True` | legacy_default_executable=`True`
- Family: canonical=`VOLATILITY_TRANSITION`
- Shape: subtype=`vol_shock` | phase=`shock` | evidence=`statistical` | layer=`canonical` | disposition=`keep`
- Scope: asset=`single_asset` | venue=`single_venue` | research_only=`False` | composite=`False` | context_tag=`False`
- Runtime config: signal=`vol_shock_relaxation_event` | file=`vol_shock_relaxation_events.parquet` | templates=`['mean_reversion', 'continuation', 'trend_continuation']` | horizons=`['60m']` | max_candidates=`500`
- Thresholds: `{'merge_gap_bars': 1, 'cooldown_bars': 0, 'min_occurrences': 0, 'phase_tolerance_bars': 16, 'shock_quantile': 0.9}`
- Tags: `['vol_regime', 'high_urgency']`
- Notes: Canonical volatility shock; current detector implementation also models relaxation boundary.
