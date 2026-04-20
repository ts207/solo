# Detector Migration Ledger

| Event | Version | Role | Band | Migration Bucket | Target State | Owner | Rationale |
|---|---|---|---|---|---|---|---|
| ABSORPTION_PROXY | v1 | trigger | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| BAND_BREAK | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| BASIS_DISLOC | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| BETA_SPIKE_EVENT | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| BREAKOUT_TRIGGER | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | v2 research trigger retained on the governed migration path |
| CHOP_TO_TREND_SHIFT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| CLIMAX_VOLUME_BAR | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| COPULA_PAIRS_TRADING | v1 | research_only | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | composite or research-only construct; do not expand migration scope blindly |
| CORRELATION_BREAKDOWN_EVENT | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| CROSS_ASSET_DESYNC_EVENT | v2 | context | context_only | research_perimeter | demote | workstream_b | context marker; keep behind adapter boundaries and out of runtime/promotion |
| CROSS_VENUE_DESYNC | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| DELEVERAGING_WAVE | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| DEPTH_COLLAPSE | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| DEPTH_STRESS_PROXY | v1 | trigger | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| FAILED_CONTINUATION | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| FALSE_BREAKOUT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| FEE_REGIME_CHANGE_EVENT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| FLOW_EXHAUSTION_PROXY | v1 | trigger | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| FND_DISLOC | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| FORCED_FLOW_EXHAUSTION | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| FUNDING_EXTREME_ONSET | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| FUNDING_FLIP | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| FUNDING_NORMALIZATION_TRIGGER | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| FUNDING_PERSISTENCE_TRIGGER | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| FUNDING_TIMESTAMP_EVENT | v1 | context | context_only | research_perimeter | wrap_v1 | workstream_b | context marker; keep behind adapter boundaries and out of runtime/promotion |
| GAP_OVERSHOOT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| INDEX_COMPONENT_DIVERGENCE | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| LEAD_LAG_BREAK | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| LIQUIDATION_CASCADE | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| LIQUIDATION_CASCADE_PROXY | v2 | trigger | composite_or_fragile | research_perimeter | demote | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| LIQUIDATION_EXHAUSTION_REVERSAL | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| LIQUIDITY_GAP_PRINT | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | v2 research trigger retained on the governed migration path |
| LIQUIDITY_SHOCK | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| LIQUIDITY_STRESS_DIRECT | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| LIQUIDITY_STRESS_PROXY | v2 | trigger | composite_or_fragile | research_perimeter | demote | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| LIQUIDITY_VACUUM | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| MOMENTUM_DIVERGENCE_TRIGGER | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| OI_FLUSH | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| OI_SPIKE_NEGATIVE | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| OI_SPIKE_POSITIVE | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| ORDERFLOW_IMBALANCE_SHOCK | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| OVERSHOOT_AFTER_SHOCK | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| POST_DELEVERAGING_REBOUND | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| PRICE_VOL_IMBALANCE_PROXY | v1 | trigger | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| PULLBACK_PIVOT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| RANGE_BREAKOUT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| RANGE_COMPRESSION_END | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | v2 research trigger retained on the governed migration path |
| SCHEDULED_NEWS_WINDOW_EVENT | v1 | context | context_only | research_perimeter | wrap_v1 | workstream_b | context marker; keep behind adapter boundaries and out of runtime/promotion |
| SEQ_FND_EXTREME_THEN_BREAKOUT | v1 | composite | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | composite or research-only construct; do not expand migration scope blindly |
| SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY | v1 | composite | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | composite or research-only construct; do not expand migration scope blindly |
| SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE | v1 | composite | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | composite or research-only construct; do not expand migration scope blindly |
| SEQ_VOL_COMP_THEN_BREAKOUT | v1 | composite | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | composite or research-only construct; do not expand migration scope blindly |
| SESSION_CLOSE_EVENT | v1 | context | context_only | research_perimeter | wrap_v1 | workstream_b | context marker; keep behind adapter boundaries and out of runtime/promotion |
| SESSION_OPEN_EVENT | v1 | context | context_only | research_perimeter | wrap_v1 | workstream_b | context marker; keep behind adapter boundaries and out of runtime/promotion |
| SLIPPAGE_SPIKE_EVENT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| SPOT_PERP_BASIS_SHOCK | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| SPREAD_BLOWOUT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| SPREAD_REGIME_WIDENING_EVENT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| SUPPORT_RESISTANCE_BREAK | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| SWEEP_STOPRUN | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| TREND_ACCELERATION | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| TREND_DECELERATION | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| TREND_EXHAUSTION_TRIGGER | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| TREND_TO_CHOP_SHIFT | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
| VOL_CLUSTER_SHIFT | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | v2 research trigger retained on the governed migration path |
| VOL_REGIME_SHIFT_EVENT | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | v2 research trigger retained on the governed migration path |
| VOL_RELAXATION_START | v2 | trigger | research_trigger | promotion_eligible_middle_layer | migrate_to_v2 | workstream_c | promotion-eligible detector; keep on the governed v2 migration path |
| VOL_SHOCK | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| VOL_SPIKE | v2 | trigger | deployable_core | runtime_core_first | migrate_to_v2 | workstream_c | deployable core runtime detector; keep fully v2 and contract-complete |
| WICK_REVERSAL_PROXY | v1 | trigger | composite_or_fragile | research_perimeter | wrap_v1 | workstream_b | fragile or proxy-heavy detector; preserve via adapters or demotion only |
| ZSCORE_STRETCH | v1 | trigger | research_trigger | research_perimeter | keep_v1 | workstream_b | legacy research trigger still available for planning but not promoted to v2 yet |
