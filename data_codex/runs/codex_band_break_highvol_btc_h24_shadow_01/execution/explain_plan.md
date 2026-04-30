# Explain Plan

- Run ID: `codex_band_break_highvol_btc_h24_shadow_01`
- Planned at: `2026-04-30T19:44:50.967071+00:00`
- Mode: `research`
- Symbols: `BTCUSDT`
- Timeframe: `5m`

## Selected Stages

- `build_features_5m` [selected] family=`core` owner=`project.pipelines.stages.core`
  outputs: `features.perp.v2`
  inputs: `clean.perp.*`
- `build_universe_snapshots` [selected] family=`core` owner=`project.pipelines.stages.core`
  outputs: `metadata.universe_snapshots`
  inputs: `clean.perp.*`
- `build_market_context_5m` [selected] family=`core` owner=`project.pipelines.stages.core`
  outputs: `context.market_state`
  inputs: `features.perp.v2`
- `build_microstructure_rollup_5m` [selected] family=`core` owner=`project.pipelines.stages.core`
  outputs: `context.microstructure`
  inputs: `features.perp.v2`
- `validate_feature_integrity_5m` [selected] family=`core` owner=`project.pipelines.stages.core`
  inputs: `features.perp.v2`
- `validate_data_coverage_5m` [selected] family=`core` owner=`project.pipelines.stages.core`
  inputs: `clean.perp.*`
- `build_normalized_replay_stream` [selected] family=`runtime_invariants` owner=`project.pipelines.stages.core`
  outputs: `runtime.normalized_stream`
  inputs: `metadata.universe_snapshots`
- `run_causal_lane_ticks` [selected] family=`runtime_invariants` owner=`project.pipelines.stages.core`
  outputs: `runtime.causal_ticks`
  inputs: `runtime.normalized_stream`
- `analyze_events__BAND_BREAK_5m` [selected] family=`phase1_analysis` owner=`project.research.analyze_events`
  outputs: `phase1.events.BAND_BREAK`
- `build_event_registry__BAND_BREAK_5m` [selected] family=`phase2_event_registry` owner=`project.research.build_event_registry`
  outputs: `phase2.event_registry.BAND_BREAK`
- `canonicalize_event_episodes__BAND_BREAK_5m` [selected] family=`phase2_event_registry` owner=`project.research.build_event_registry`
  outputs: `phase2.event_episodes.BAND_BREAK`
  inputs: `phase2.event_registry.BAND_BREAK`
- `phase1_correlation_clustering` [selected] family=`phase1_analysis` owner=`project.research.analyze_events`
  outputs: `phase1.correlation_clustering`
- `phase2_search_engine` [selected] family=`phase2_discovery` owner=`project.research.phase2_search_engine`
  outputs: `phase2.candidates.search`
  inputs: `features.perp.v2`
- `summarize_discovery_quality` [selected] family=`phase2_discovery` owner=`project.research.phase2_search_engine`
  outputs: `phase2.discovery_quality_summary`
  inputs: `phase2.candidates.*`
- `export_edge_candidates` [selected] family=`promotion` owner=`project.research.services.promotion_service`
  outputs: `edge_candidates.normalized`
  inputs: `phase2.candidates.*`
- `update_edge_registry` [selected] family=`promotion` owner=`project.research.services.promotion_service`
  outputs: `history.candidate.edge_observations`, `history.candidate.edge_registry`, `edge_registry.snapshot`
  inputs: `promotion.audit`, `promotion.promoted_candidates`
- `update_campaign_memory` [selected] family=`promotion` owner=`project.research.services.promotion_service`
  outputs: `experiment.memory.tested_regions`, `experiment.memory.reflections`, `experiment.memory.failures`
- `analyze_conditional_expectancy` [selected] family=`phase1_analysis` owner=`project.research.analyze_events`
  outputs: `research.expectancy_analysis`
  inputs: `history.candidate.edge_registry`
- `validate_expectancy_traps` [selected] family=`research_quality` owner=`project.research.services.reporting_service`
  outputs: `research.expectancy_traps`
  inputs: `research.expectancy_analysis`
- `generate_recommendations_checklist` [selected] family=`research_quality` owner=`project.research.services.reporting_service`
  outputs: `research.recommendations_checklist`
  inputs: `research.expectancy_traps`
- `finalize_experiment` [selected] family=`phase2_discovery` owner=`project.research.phase2_search_engine`
  outputs: `experiment.tested_ledger`
- `build_cleaned_5m` [skipped] family=`core` owner=`project.pipelines.stages.core`
- `ingest_bybit_derivatives_ohlcv_5m` [skipped] family=`ingest` owner=`project.pipelines.stages.ingest`
- `ingest_bybit_derivatives_funding` [skipped] family=`ingest` owner=`project.pipelines.stages.ingest`

## Artifact Obligations

- `discovery_phase2_candidates` -> `reports/phase2/codex_band_break_highvol_btc_h24_shadow_01/phase2_candidates.parquet` (producer=`phase2_discovery`, schema=`phase2_candidates`)
- `run_manifest` -> `runs/codex_band_break_highvol_btc_h24_shadow_01/run_manifest.json` (producer=`run_orchestration`, schema=`run_manifest`)
