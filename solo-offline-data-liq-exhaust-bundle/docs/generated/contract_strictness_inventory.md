# Contract Strictness Inventory

Generated from `project.contracts` registries. Do not edit by hand.

## Summary

- total_contracts: `58`
- strict: `58`

## Stage Artifact Contracts

| id | strictness | stage_patterns | inputs | optional_inputs | outputs |
| --- | --- | --- | --- | --- | --- |
| stage_artifact::001 | strict | ingest_binance_um_funding |  |  | raw.perp.funding_5m |
| stage_artifact::002 | strict | ingest_bybit_derivatives_funding |  |  | raw.perp.funding_5m |
| stage_artifact::003 | strict | ingest_binance_um_liquidation_snapshot |  |  | raw.perp.liquidations |
| stage_artifact::004 | strict | ingest_binance_um_open_interest_hist |  |  | raw.perp.open_interest |
| stage_artifact::005 | strict | ingest_bybit_derivatives_oi |  |  | raw.perp.open_interest |
| stage_artifact::006 | strict | ingest_binance_um_ohlcv_{tf} |  |  | raw.perp.ohlcv_{tf} |
| stage_artifact::007 | strict | ingest_bybit_derivatives_ohlcv_{tf} |  |  | raw.perp.ohlcv_{tf} |
| stage_artifact::008 | strict | ingest_binance_spot_ohlcv_{tf} |  |  | raw.spot.ohlcv_{tf} |
| stage_artifact::009 | strict | build_cleaned_{tf} | raw.perp.ohlcv_{tf} |  | clean.perp.* |
| stage_artifact::010 | strict | build_cleaned_{tf}_spot | raw.spot.ohlcv_{tf} |  | clean.spot.* |
| stage_artifact::011 | strict | build_features_{tf} | clean.perp.* | raw.perp.funding_{tf}, raw.perp.liquidations, raw.perp.open_interest | features.perp.v2 |
| stage_artifact::012 | strict | build_features_{tf}_spot | clean.spot.* |  | features.spot.v2 |
| stage_artifact::013 | strict | build_universe_snapshots | clean.perp.* |  | metadata.universe_snapshots |
| stage_artifact::014 | strict | build_market_context* | features.perp.v2 |  | context.market_state |
| stage_artifact::015 | strict | build_microstructure_rollup* | features.perp.v2 |  | context.microstructure |
| stage_artifact::016 | strict | validate_feature_integrity* | features.perp.v2 |  |  |
| stage_artifact::017 | strict | validate_data_coverage* | clean.perp.* |  |  |
| stage_artifact::018 | strict | validate_context_entropy | context.features |  |  |
| stage_artifact::019 | strict | build_normalized_replay_stream | metadata.universe_snapshots |  | runtime.normalized_stream |
| stage_artifact::020 | strict | run_causal_lane_ticks | runtime.normalized_stream |  | runtime.causal_ticks |
| stage_artifact::021 | strict | run_determinism_replay_checks | runtime.causal_ticks |  | runtime.determinism_checks |
| stage_artifact::022 | strict | run_oms_replay_validation | runtime.causal_ticks |  | runtime.oms_replay |
| stage_artifact::023 | strict | analyze_* |  | features.perp.v2, context.market_state, context.microstructure | phase1.events.{event_type} |
| stage_artifact::024 | strict | phase1_correlation_clustering |  | phase1.events.* | phase1.correlation_clustering |
| stage_artifact::025 | strict | build_event_registry* |  | phase1.events.* | phase2.event_registry.{event_type} |
| stage_artifact::026 | strict | canonicalize_event_episodes* | phase2.event_registry.{event_type} |  | phase2.event_episodes.{event_type} |
| stage_artifact::027 | strict | phase2_search_engine | features.perp.v2 |  | phase2.candidates.search |
| stage_artifact::028 | strict | analyze_interaction_lift | phase2.candidates.* |  | research.interaction_lift |
| stage_artifact::029 | strict | finalize_experiment |  | phase2.candidates.* | experiment.tested_ledger |
| stage_artifact::030 | strict | summarize_discovery_quality | phase2.candidates.* | phase2.bridge_summary.* | phase2.discovery_quality_summary |
| stage_artifact::031 | strict | evaluate_naive_entry | phase2.candidates.* |  | phase2.naive_entry_eval |
| stage_artifact::032 | strict | export_edge_candidates | phase2.candidates.* | phase2.bridge_metrics.* | edge_candidates.normalized |
| stage_artifact::033 | strict | generate_negative_control_summary | edge_candidates.normalized |  | research.negative_control_summary |
| stage_artifact::034 | strict | promote_candidates | edge_candidates.normalized, research.negative_control_summary | phase2.bridge_metrics.*, phase2.naive_entry_eval | promotion.audit, promotion.promoted_candidates |
| stage_artifact::035 | strict | update_edge_registry | promotion.audit, promotion.promoted_candidates |  | history.candidate.edge_observations, history.candidate.edge_registry, edge_registry.snapshot |
| stage_artifact::036 | strict | update_campaign_memory |  | edge_candidates.normalized, promotion.audit, history.candidate.edge_registry, phase2.discovery_quality_summary | experiment.memory.tested_regions, experiment.memory.reflections, experiment.memory.failures |
| stage_artifact::037 | strict | analyze_conditional_expectancy | history.candidate.edge_registry |  | research.expectancy_analysis |
| stage_artifact::038 | strict | validate_expectancy_traps | research.expectancy_analysis |  | research.expectancy_traps |
| stage_artifact::039 | strict | generate_recommendations_checklist | research.expectancy_traps |  | research.recommendations_checklist |
| stage_artifact::040 | strict | compile_strategy_blueprints | research.recommendations_checklist |  | strategy.blueprints |
| stage_artifact::041 | strict | build_strategy_candidates |  | research.recommendations_checklist, strategy.blueprints | strategy.candidates |
| stage_artifact::042 | strict | select_profitable_strategies |  | strategy.candidates | strategy.profitable |

## Lifecycle Artifact Contracts

| id | strictness | producer_stage_family | consumer_stage_families | schema_id | path_pattern |
| --- | --- | --- | --- | --- | --- |
| discovery_phase2_candidates | strict | phase2_discovery | validation, operator | phase2_candidates | reports/phase2/{run_id}/phase2_candidates.parquet |
| validation_bundle | strict | validation | promotion, operator | validation_bundle | reports/validation/{run_id}/validation_bundle.json |
| promotion_ready_candidates | strict | validation | promotion, operator | promotion_ready_candidates | reports/validation/{run_id}/promotion_ready_candidates.parquet |
| promoted_theses | strict | promotion | deploy, operator | promoted_theses_payload | live/theses/{run_id}/promoted_theses.json |
| live_thesis_index | strict | promotion | deploy, operator | live_thesis_index | live/theses/index.json |
| run_manifest | strict | run_orchestration | validation, promotion, deploy, operator | run_manifest | runs/{run_id}/run_manifest.json |

## DataFrame Schema Contracts

| id | strictness | schema_version | required_columns |
| --- | --- | --- | --- |
| evidence_bundle_summary | strict | phase5_schema_v1 | candidate_id, event_type, promotion_decision, promotion_track, policy_version, bundle_version, is_reduced_evidence |
| phase2_candidates | strict | phase5_schema_v1 | candidate_id, hypothesis_id, event_type, symbol, run_id |
| promoted_candidates | strict | phase5_schema_v1 | candidate_id, event_type, status |
| promotion_audit | strict | phase5_schema_v1 | candidate_id, event_type, promotion_decision, promotion_track |
| promotion_decisions | strict | phase5_schema_v1 | candidate_id, event_type, promotion_decision, promotion_track, policy_version, bundle_version, is_reduced_evidence |
| promotion_ready_candidates | strict | phase5_schema_v1 | candidate_id, validation_status, validation_run_id, validation_program_id, metric_sample_count, metric_q_value, metric_stability_score, metric_net_expectancy |

## Payload Schema Contracts

| id | strictness | schema_version | required_fields | version_field |
| --- | --- | --- | --- | --- |
| live_thesis_index | strict | promoted_thesis_index_v1 | schema_version, latest_run_id, default_resolution_disabled, runs | schema_version |
| promoted_theses_payload | strict | promoted_theses_v1 | schema_version, run_id, generated_at_utc, thesis_count, active_thesis_count, pending_thesis_count, theses | schema_version |
| run_manifest | strict | run_manifest_v1 | run_id |  |
| validation_bundle | strict | validation_bundle_v1 | run_id, created_at, validated_candidates, rejected_candidates, inconclusive_candidates, summary_stats, effect_stability_report |  |
