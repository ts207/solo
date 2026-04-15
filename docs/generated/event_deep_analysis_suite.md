# Event Deep Analysis Suite

- Overall status: `attention`
- Task count: `10`
- Passed tasks: `9`
- Tasks needing attention: `1`

## 01. Audit event universe

- Task id: `01_event_universe`
- Status: `passed`
- Summary:
  - `active_event_count`: `71`
  - `default_executable_event_count`: `62`
  - `planning_event_count`: `59`
  - `registered_detector_entry_count`: `73`
  - `alias_count`: `8`

- Details:
  - `runtime_aliases`: `['ABSORPTION_EVENT', 'ABSORPTION_PROXY', 'BASIS_DISLOCATION', 'DEPTH_COLLAPSE', 'DEPTH_STRESS_PROXY', 'LIQUIDITY_STRESS_DIRECT', 'LIQUIDITY_STRESS_PROXY', 'VOL_REGIME_SHIFT']`
  - `default_executable_but_not_planning`: `['CROSS_ASSET_DESYNC_EVENT', 'DEPTH_COLLAPSE', 'LIQUIDATION_CASCADE_PROXY']`
  - `source_audits`: `['docs/generated/event_ontology_audit.json', 'docs/generated/detector_coverage.json']`

- Verification commands:
  - `python -m project.scripts.event_ontology_audit --check`
  - `python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md`

## 02. Review event contracts

- Task id: `02_event_contracts`
- Status: `passed`
- Summary:
  - `active_event_count`: `71`
  - `complete_event_count`: `71`
  - `missing_event_count`: `0`

- Details:
  - `missing_fields`: `{}`
  - `source_artifacts`: `['docs/generated/event_contract_completeness.json', 'docs/generated/event_contract_completeness.md']`

- Verification commands:
  - `python -m project.scripts.build_event_contract_artifacts --check`

## 03. Inspect detector fidelity

- Task id: `03_detector_fidelity`
- Status: `passed`
- Summary:
  - `active_event_count`: `71`
  - `issue_count`: `0`
  - `warning_count`: `0`
  - `error_count`: `0`

- Details:
  - `maturity_counts`: `{'standard': 61, 'production': 6, 'specialized': 4}`
  - `evidence_tier_counts`: `{'hybrid': 26, 'statistical': 9, 'inferred_cross_asset': 7, 'direct': 20, 'contextual': 4, 'proxy': 1, 'sequence_confirmed': 4}`
  - `issues`: `[]`
  - `source_artifacts`: `['docs/generated/detector_coverage.json', 'docs/generated/detector_coverage.md']`

- Verification commands:
  - `python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md`

## 04. Check maturity tiers

- Task id: `04_maturity_tiers`
- Status: `passed`
- Summary:
  - `tier_counts`: `{'A': 8, 'B': 52, 'C': 6, 'D': 5}`
  - `role_counts`: `{'context': 4, 'filter': 1, 'research_only': 1, 'sequence_component': 4, 'trigger': 61}`
  - `deployment_disposition_counts`: `{'context_only': 4, 'primary_trigger_candidate': 8, 'repair_before_promotion': 1, 'research_only': 5, 'secondary_or_confirm': 53}`

- Details:
  - `planning_default_tiers`: `['A', 'B']`
  - `planning_event_count`: `59`
  - `non_planning_active_events`: `['COPULA_PAIRS_TRADING', 'CROSS_ASSET_DESYNC_EVENT', 'DEPTH_COLLAPSE', 'FUNDING_TIMESTAMP_EVENT', 'LIQUIDATION_CASCADE_PROXY', 'SCHEDULED_NEWS_WINDOW_EVENT', 'SEQ_FND_EXTREME_THEN_BREAKOUT', 'SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY', 'SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE', 'SEQ_VOL_COMP_THEN_BREAKOUT', 'SESSION_CLOSE_EVENT', 'SESSION_OPEN_EVENT']`
  - `source_artifacts`: `['docs/generated/event_maturity_matrix.csv', 'docs/generated/event_tiers.md']`

- Verification commands:
  - `python -m project.scripts.build_event_contract_artifacts --check`
  - `python -m pytest project/tests/events/test_event_governance_integration.py -q`

## 05. Audit thresholds and calibration

- Task id: `05_threshold_calibration`
- Status: `passed`
- Summary:
  - `threshold_method_counts`: `{'basis_shock': 1, 'calibrated_search_range': 4, 'forced_flow_and_exhaustion_failure_hybrid_gate': 1, 'liquidity_dislocation_confirmation_sequence_confirmed_gate': 1, 'liquidity_dislocation_shock_direct_gate': 1, 'liquidity_dislocation_shock_hybrid_gate': 2, 'positioning_extremes_cascade_direct_gate': 1, 'positioning_extremes_confirmation_sequence_confirmed_gate': 2, 'positioning_extremes_expansion_direct_gate': 2, 'positioning_extremes_unwind_direct_gate': 1, 'quantile_plus_abs_floor': 2, 'quantile_plus_zscore_gate': 19, 'rolling': 1, 'rolling_percentile_gate': 2, 'rolling_quantile_gate': 6, 'rolling_zscore_gate': 16, 'scheduled_window_gate': 4, 'statistical_dislocation_strategy_statistical_gate': 1, 'taker_imbalance': 1, 'volatility_transition_confirmation_sequence_confirmed_gate': 1, 'volatility_transition_shock_direct_gate': 1, 'zscore_plus_bps_floor': 1}`
  - `calibration_method_counts`: `{'Calibrate ABSORPTION_PROXY by tuning imbalance_abs_quantile, spread_quantile, rv_quantile to maximize persistent-state separation versus transient noise; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate BAND_BREAK by tuning band_z_threshold to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate BASIS_DISLOC by tuning z_threshold, min_basis_bps to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate BETA_SPIKE_EVENT by tuning transition_z_threshold to maximize persistent regime-state separation after transition onset; require stable change-point timing across rolling windows while preserving minimum event counts.': 1, 'Calibrate BREAKOUT_TRIGGER by tuning expansion_quantile to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate CHOP_TO_TREND_SHIFT by tuning transition_z_threshold to maximize persistent regime-state separation after transition onset; require stable change-point timing across rolling windows while preserving minimum event counts.': 1, 'Calibrate CLIMAX_VOLUME_BAR by tuning liquidation_spike_z_threshold, range_quantile, ret_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate COPULA_PAIRS_TRADING by tuning detector thresholds to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate CORRELATION_BREAKDOWN_EVENT by tuning transition_z_threshold to maximize persistent regime-state separation after transition onset; require stable change-point timing across rolling windows while preserving minimum event counts.': 1, 'Calibrate DELEVERAGING_WAVE by tuning oi_change_z_threshold, funding_extreme_quantile to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate DEPTH_COLLAPSE by tuning z_threshold, min_volume_quantile to maximize forward response separation; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate DEPTH_STRESS_PROXY by tuning spread_quantile, rv_quantile, depth_quantile to maximize forward response separation; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate FAILED_CONTINUATION by tuning detector thresholds to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate FALSE_BREAKOUT by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate FEE_REGIME_CHANGE_EVENT by tuning spread_z_threshold, slippage_z_threshold to maximize execution-friction separation across spread or slippage stress; require stability across venues and liquidity buckets while preserving minimum event counts.': 1, 'Calibrate FLOW_EXHAUSTION_PROXY by tuning z_threshold, spread_quantile, liquidation_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate FND_DISLOC by tuning min_basis_bps, threshold_bps, funding_quantile to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate FORCED_FLOW_EXHAUSTION by tuning anchor_mode, spread_quantile, liquidation_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate FUNDING_FLIP by tuning oi_change_z_threshold, funding_extreme_quantile, min_flip_abs to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate FUNDING_NORMALIZATION_TRIGGER by tuning funding_pct_window, extreme_pct, accel_pct to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate FUNDING_PERSISTENCE_TRIGGER by tuning funding_pct_window, extreme_pct, accel_pct to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate FUNDING_TIMESTAMP_EVENT by tuning funding_abs_quantile to maximize session-specific response separation around scheduled windows; require stability across calendar partitions and rolling windows while preserving minimum event counts.': 1, 'Calibrate GAP_OVERSHOOT by tuning band_z_threshold, return_quantile to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate INDEX_COMPONENT_DIVERGENCE by tuning desync_z_threshold to maximize convergence or repricing separation after desynchronization; require stability across assets, venues, and sessions while preserving minimum event counts.': 1, 'Calibrate LEAD_LAG_BREAK by tuning desync_z_threshold to maximize convergence or repricing separation after desynchronization; require stability across assets, venues, and sessions while preserving minimum event counts.': 1, 'Calibrate LIQUIDATION_CASCADE by tuning detector thresholds to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate LIQUIDATION_EXHAUSTION_REVERSAL by tuning spread_quantile, liquidation_quantile, oi_drop_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate LIQUIDITY_GAP_PRINT by tuning z_threshold, min_volume_quantile to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate LIQUIDITY_SHOCK by tuning detector thresholds to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate LIQUIDITY_STRESS_DIRECT by tuning detector thresholds to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate LIQUIDITY_STRESS_PROXY by tuning detector thresholds to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate MOMENTUM_DIVERGENCE_TRIGGER by tuning liquidation_spike_z_threshold, slow_trend_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate OI_FLUSH by tuning flush_pct_th to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate OI_SPIKE_NEGATIVE by tuning flush_pct_th to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate OI_SPIKE_POSITIVE by tuning flush_pct_th to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate ORDERFLOW_IMBALANCE_SHOCK by tuning z_threshold, rv_quantile, min_volume_quantile to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate OVERSHOOT_AFTER_SHOCK by tuning band_z_threshold, rv_quantile, price_quantile to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Calibrate POST_DELEVERAGING_REBOUND by tuning spread_quantile, liquidation_quantile, oi_drop_quantile to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate PRICE_VOL_IMBALANCE_PROXY by tuning rv_quantile, ret_quantile, volume_quantile to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate PULLBACK_PIVOT by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate RANGE_BREAKOUT by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate RANGE_COMPRESSION_END by tuning compression_quantile, expansion_z_threshold to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate SCHEDULED_NEWS_WINDOW_EVENT by tuning detector thresholds to maximize session-specific response separation around scheduled windows; require stability across calendar partitions and rolling windows while preserving minimum event counts.': 1, 'Calibrate SEQ_FND_EXTREME_THEN_BREAKOUT by tuning detector thresholds to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY by tuning detector thresholds to maximize forward response separation; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE by tuning detector thresholds to maximize post-extreme unwind or normalization separation; require stability across assets, funding regimes, and rolling windows while preserving minimum event counts.': 1, 'Calibrate SEQ_VOL_COMP_THEN_BREAKOUT by tuning detector thresholds to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate SESSION_CLOSE_EVENT by tuning session_range_quantile, hours_utc, minute_close_start to maximize session-specific response separation around scheduled windows; require stability across calendar partitions and rolling windows while preserving minimum event counts.': 1, 'Calibrate SESSION_OPEN_EVENT by tuning session_range_quantile, hours_utc, minute_open to maximize session-specific response separation around scheduled windows; require stability across calendar partitions and rolling windows while preserving minimum event counts.': 1, 'Calibrate SLIPPAGE_SPIKE_EVENT by tuning spread_z_threshold, slippage_z_threshold to maximize execution-friction separation across spread or slippage stress; require stability across venues and liquidity buckets while preserving minimum event counts.': 1, 'Calibrate SPOT_PERP_BASIS_SHOCK by tuning z_threshold, min_basis_bps, shock_change_quantile to maximize convergence or repricing separation after desynchronization; require stability across assets, venues, and sessions while preserving minimum event counts.': 1, 'Calibrate SPREAD_BLOWOUT by tuning z_threshold, min_volume_quantile to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate SPREAD_REGIME_WIDENING_EVENT by tuning low_volume_quantile to maximize execution-friction separation across spread or slippage stress; require stability across venues and liquidity buckets while preserving minimum event counts.': 1, 'Calibrate SUPPORT_RESISTANCE_BREAK by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate SWEEP_STOPRUN by tuning z_threshold, min_volume_quantile to maximize post-shock response separation versus normal conditions; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate TREND_ACCELERATION by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate TREND_DECELERATION by tuning breakout_z_threshold to maximize continuation-versus-failure separation after trend signals; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate TREND_EXHAUSTION_TRIGGER by tuning reversal_quantile, cooldown_quantile, liquidation_spike_z_threshold to maximize exhaustion-to-reversal separation after forced-flow shocks; require stability across volatility regimes and rolling splits while preserving minimum event counts.': 1, 'Calibrate TREND_TO_CHOP_SHIFT by tuning transition_z_threshold to maximize persistent regime-state separation after transition onset; require stable change-point timing across rolling windows while preserving minimum event counts.': 1, 'Calibrate VOL_CLUSTER_SHIFT by tuning compression_quantile, expansion_z_threshold to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate VOL_REGIME_SHIFT_EVENT by tuning transition_z_threshold to maximize persistent regime-state separation after transition onset; require stable change-point timing across rolling windows while preserving minimum event counts.': 1, 'Calibrate VOL_RELAXATION_START by tuning compression_quantile, expansion_z_threshold to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate VOL_SPIKE by tuning detector thresholds to maximize post-transition volatility and return separation; require stability across volatility regimes and rolling windows while preserving minimum event counts.': 1, 'Calibrate WICK_REVERSAL_PROXY by tuning z_threshold, min_volume_quantile to maximize forward response separation; require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts.': 1, 'Calibrate ZSCORE_STRETCH by tuning band_z_threshold, zscore_quantile to maximize mean-reversion or continuation separation after statistical stretch; require stability across assets and rolling windows while preserving minimum event counts.': 1, 'Maximize AUC of forward return (price recovery) vs oi_reduction_pct. Require at least 30 events over calibration window.': 1, 'Maximize AUC of forward return vs funding normalization probability. Require at least min_hazard_events = 50 events over calibration window.': 1, 'Maximize information ratio of mean-reversion return within window_end bars. Require at least 40 events per asset over calibration window.': 1, 'Maximize stability of point-in-time shock-onset detection and usefulness of post-event decay telemetry (half-life, excess vol area, secondary-shock incidence) while keeping sufficient event counts per window.': 1, 'Minimum event count >= min_events_calibration; prefer higher quantile.': 1, 'Select the smallest threshold that preserves pair-specific stability while maintaining acceptable event counts across major pairs.': 1}`
  - `generic_calibration_event_count`: `0`
  - `declared_threshold_event_count`: `0`

- Details:
  - `generic_calibration_events`: `[]`
  - `declared_threshold_events`: `[]`

- Verification commands:
  - `python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md`
  - `python -m pytest project/tests/events/test_detector_hardening.py -q`

## 06. Map overlap and collisions

- Task id: `06_overlap_collisions`
- Status: `passed`
- Summary:
  - `contracts_with_overlap_notes`: `71`
  - `avg_expected_overlap_entries`: `1.155`
  - `direct_proxy_group_count`: `2`
  - `ontology_collision_group_count`: `0`

- Details:
  - `direct_proxy_groups`: `{'LIQUIDATION_CASCADE': ['LIQUIDATION_CASCADE', 'LIQUIDATION_CASCADE_PROXY'], 'LIQUIDITY_STRESS': ['LIQUIDITY_STRESS_DIRECT', 'LIQUIDITY_STRESS_PROXY']}`
  - `ontology_collisions`: `{}`

- Verification commands:
  - `python -m project.scripts.event_ontology_audit --check`
  - `python -m pytest project/tests/events/test_ontology_deconfliction.py -q`

## 07. Review regime restrictions

- Task id: `07_regime_restrictions`
- Status: `passed`
- Summary:
  - `routed_regime_count`: `14`
  - `missing_regime_count`: `0`
  - `contracts_with_disabled_regimes`: `71`
  - `avg_disabled_regime_entries`: `1.085`

- Details:
  - `missing_regimes`: `[]`
  - `unexpected_regimes`: `[]`
  - `non_routable_entries`: `[]`
  - `invalid_templates`: `{}`

- Verification commands:
  - `python -m project.scripts.regime_routing_audit --check`

## 08. Validate data dependencies

- Task id: `08_data_dependencies`
- Status: `passed`
- Summary:
  - `events_with_required_features`: `71`
  - `unique_required_feature_count`: `11`
  - `unique_required_column_count`: `22`
  - `events_missing_required_columns`: `0`

- Details:
  - `top_required_features`: `{'timestamp': 71, 'close': 4, 'high': 2, 'low': 2, 'volume': 2, 'pair_close': 1, 'close_spot': 1, 'close_perp': 1, 'funding_rate': 1, 'oi_notional': 1, 'oi_delta_1h': 1}`
  - `top_required_columns`: `{'timestamp': 71, 'close': 48, 'rv_96': 21, 'high': 18, 'low': 18, 'oi_delta_1h': 7, 'volume': 6, 'range_96': 5, 'range_med_2880': 5, 'liquidation_notional': 5, 'oi_notional': 5, 'close_perp': 4, 'close_spot': 4, 'funding_abs_pct': 3, 'funding_abs': 3}`
  - `events_missing_required_columns`: `[]`

- Verification commands:
  - `python -m pytest project/tests/events/test_detector_contract.py -q`
  - `python -m pytest project/tests/events/test_registry_loader.py -q`

## 09. Test CI event guards

- Task id: `09_ci_event_guards`
- Status: `attention`
- Summary:
  - `configured_guard_path_count`: `9`
  - `expected_guard_path_count`: `11`
  - `workflow_file_count`: `3`

- Details:
  - `guard_paths`: `['project/scripts/build_event_contract_artifacts.py', 'project/scripts/build_event_ontology_artifacts.py', 'project/scripts/build_event_deep_analysis_suite.py', 'project/scripts/detector_coverage_audit.py', 'project/scripts/event_ontology_audit.py', 'project/scripts/regime_routing_audit.py', 'project/tests/events/test_event_governance_integration.py', 'project/tests/scripts/test_detector_coverage_audit.py', 'project/tests/scripts/test_event_deep_analysis_suite.py']`
  - `workflow_paths`: `['.github/workflows/tier1.yml', '.github/workflows/tier2.yml', '.github/workflows/tier3.yml']`

- Verification commands:
  - `bash project/scripts/pre_commit.sh`
  - `python -m project.scripts.run_researcher_verification --mode contracts`

## 10. Synthesize event findings

- Task id: `10_synthesis`
- Status: `passed`
- Summary:
  - `overall_status`: `passed`
  - `critical_issue_count`: `1`
  - `residual_priority_count`: `1`

- Details:
  - `proxy_evidence_event_count`: `1`
  - `proxy_evidence_planning_event_count`: `0`
  - `proxy_evidence_events`: `['LIQUIDATION_CASCADE_PROXY']`
  - `proxy_evidence_planning_events`: `[]`
  - `descriptive_non_planning_event_count`: `10`
  - `descriptive_non_planning_events`: `['COPULA_PAIRS_TRADING', 'CROSS_ASSET_DESYNC_EVENT', 'FUNDING_TIMESTAMP_EVENT', 'SCHEDULED_NEWS_WINDOW_EVENT', 'SEQ_FND_EXTREME_THEN_BREAKOUT', 'SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY', 'SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE', 'SEQ_VOL_COMP_THEN_BREAKOUT', 'SESSION_CLOSE_EVENT', 'SESSION_OPEN_EVENT']`
  - `residual_priorities`: `[{'label': 'keep_context_and_research_events_out_of_default_planning', 'reason': 'Active events that are context-only, research-only, or sequence-only must stay outside the default planning set to preserve promotion safety.', 'event_count': 10, 'priority': 1}]`
  - `recommended_next_actions`: `['Proxy-evidence events are quarantined from default planning; upgrade them to stronger evidence before reintroducing them to default trigger selection.', 'Keep context-only, research-only, and sequence-only events out of the default planning set to preserve promotion safety.', 'Keep the new deep-analysis suite in artifact regeneration and contract verification so drift is caught automatically.']`

- Verification commands:
  - `python -m project.scripts.build_event_deep_analysis_suite --check`
  - `bash project/scripts/regenerate_artifacts.sh`
