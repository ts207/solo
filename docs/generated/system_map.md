# System Map

Generated from stage and artifact contract registries.

## Canonical Entrypoints

| Name | Kind | Module | Status | Description |
| --- | --- | --- | --- | --- |
| run_all_cli | orchestration_entrypoint | `project.pipelines.run_all` | canonical | Primary orchestration CLI entrypoint. |
| phase2_search_engine | pipeline_stage | `project.research.phase2_search_engine` | canonical | Canonical phase-2 discovery stage (invoked by run_all.py). |
| candidate_discovery_service | service | `project.research.services.candidate_discovery_service` | legacy_compat | Legacy discovery service — retained for smoke tests and CLI tool only. Active pipeline uses phase2_search_engine. |
| promotion_service | service | `project.research.services.promotion_service` | canonical | Canonical promotion service. |
| reporting_service | service | `project.research.services.reporting_service` | canonical | Schema-aware reporting service for discovery and promotion outputs. |

## Compatibility Surfaces

Legacy wrapper surfaces have been removed.


## Stage Families

### `ingest`

- Owner service: `project.pipelines.stages.ingest`
- Stage patterns: `ingest_binance_um_ohlcv_*`, `ingest_binance_um_funding`, `ingest_binance_spot_ohlcv_*`, `ingest_binance_um_liquidation_snapshot`, `ingest_binance_um_open_interest_hist`, `ingest_bybit_derivatives_ohlcv_*`, `ingest_bybit_derivatives_funding`, `ingest_bybit_derivatives_oi`
- Script patterns: `pipelines/ingest/ingest_binance_um_ohlcv*.py`, `pipelines/ingest/ingest_binance_um_funding.py`, `pipelines/ingest/ingest_binance_spot_ohlcv*.py`, `pipelines/ingest/ingest_binance_um_liquidation_snapshot.py`, `pipelines/ingest/ingest_binance_um_open_interest_hist.py`, `pipelines/ingest/ingest_bybit_derivatives_ohlcv.py`, `pipelines/ingest/ingest_bybit_derivatives_funding.py`, `pipelines/ingest/ingest_bybit_derivatives_open_interest.py`

### `core`

- Owner service: `project.pipelines.stages.core`
- Stage patterns: `build_cleaned_*`, `build_features*`, `build_universe_snapshots`, `build_market_context*`, `build_microstructure_rollup*`, `validate_feature_integrity*`, `validate_data_coverage*`, `validate_context_entropy`
- Script patterns: `pipelines/clean/build_cleaned_bars.py`, `pipelines/features/build_features.py`, `pipelines/ingest/build_universe_snapshots.py`, `pipelines/features/build_market_context.py`, `pipelines/features/build_microstructure_rollup.py`, `pipelines/clean/validate_feature_integrity.py`, `pipelines/clean/validate_data_coverage.py`, `pipelines/clean/validate_context_entropy.py`

### `runtime_invariants`

- Owner service: `project.pipelines.stages.core`
- Stage patterns: `build_normalized_replay_stream`, `run_causal_lane_ticks`, `run_determinism_replay_checks`, `run_oms_replay_validation`
- Script patterns: `pipelines/runtime/build_normalized_replay_stream.py`, `pipelines/runtime/run_causal_lane_ticks.py`, `pipelines/runtime/run_determinism_replay_checks.py`, `pipelines/runtime/run_oms_replay_validation.py`

### `phase1_analysis`

- Owner service: `project.research.analyze_events`
- Stage patterns: `analyze_*`, `phase1_correlation_clustering`
- Script patterns: `research/analyze_*.py`, `research/phase1_correlation_clustering.py`

### `phase2_event_registry`

- Owner service: `project.research.build_event_registry`
- Stage patterns: `build_event_registry*`, `canonicalize_event_episodes*`
- Script patterns: `research/build_event_registry.py`, `research/canonicalize_event_episodes.py`

### `phase2_discovery`

- Owner service: `project.research.phase2_search_engine`
- Stage patterns: `phase2_search_engine`, `summarize_discovery_quality`, `analyze_interaction_lift`, `finalize_experiment`
- Script patterns: `research/phase2_search_engine.py`, `research/summarize_discovery_quality.py`, `research/analyze_interaction_lift.py`, `research/finalize_experiment.py`

### `promotion`

- Owner service: `project.research.services.promotion_service`
- Stage patterns: `evaluate_naive_entry`, `generate_negative_control_summary`, `promote_candidates`, `update_edge_registry`, `update_campaign_memory`, `export_edge_candidates`
- Script patterns: `research/evaluate_naive_entry.py`, `research/generate_negative_control_summary.py`, `research/cli/promotion_cli.py`, `research/update_edge_registry.py`, `research/update_campaign_memory.py`, `research/export_edge_candidates.py`

### `research_quality`

- Owner service: `project.research.services.reporting_service`
- Stage patterns: `analyze_conditional_expectancy`, `validate_expectancy_traps`, `generate_recommendations_checklist`
- Script patterns: `research/analyze_conditional_expectancy.py`, `research/validate_expectancy_traps.py`, `research/generate_recommendations_checklist.py`

### `strategy_packaging`

- Owner service: `project.research.services.promotion_service`
- Stage patterns: `compile_strategy_blueprints`, `build_strategy_candidates`, `select_profitable_strategies`
- Script patterns: `research/compile_strategy_blueprints.py`, `research/build_strategy_candidates.py`, `research/select_profitable_strategies.py`

## Artifact Contracts

### `ingest_binance_um_funding`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.funding_5m`
- External inputs: _none_

### `ingest_bybit_derivatives_funding`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.funding_5m`
- External inputs: _none_

### `ingest_binance_um_liquidation_snapshot`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.liquidations`
- External inputs: _none_

### `ingest_binance_um_open_interest_hist`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.open_interest`
- External inputs: _none_

### `ingest_bybit_derivatives_oi`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.open_interest`
- External inputs: _none_

### `ingest_binance_um_ohlcv_{tf}`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.ohlcv_{tf}`
- External inputs: _none_

### `ingest_bybit_derivatives_ohlcv_{tf}`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.perp.ohlcv_{tf}`
- External inputs: _none_

### `ingest_binance_spot_ohlcv_{tf}`

- Inputs: _none_
- Optional inputs: _none_
- Outputs: `raw.spot.ohlcv_{tf}`
- External inputs: _none_

### `build_cleaned_{tf}`

- Inputs: `raw.perp.ohlcv_{tf}`
- Optional inputs: _none_
- Outputs: `clean.perp.*`
- External inputs: `raw.perp.ohlcv_{tf}`

### `build_cleaned_{tf}_spot`

- Inputs: `raw.spot.ohlcv_{tf}`
- Optional inputs: _none_
- Outputs: `clean.spot.*`
- External inputs: `raw.spot.ohlcv_{tf}`

### `build_features_{tf}`

- Inputs: `clean.perp.*`
- Optional inputs: `raw.perp.funding_{tf}`, `raw.perp.liquidations`, `raw.perp.open_interest`
- Outputs: `features.perp.v2`
- External inputs: `clean.perp.*`, `raw.perp.funding_{tf}`, `raw.perp.liquidations`, `raw.perp.open_interest`

### `build_features_{tf}_spot`

- Inputs: `clean.spot.*`
- Optional inputs: _none_
- Outputs: `features.spot.v2`
- External inputs: `clean.spot.*`

### `build_universe_snapshots`

- Inputs: `clean.perp.*`
- Optional inputs: _none_
- Outputs: `metadata.universe_snapshots`
- External inputs: `clean.perp.*`

### `build_market_context*`

- Inputs: `features.perp.v2`
- Optional inputs: _none_
- Outputs: `context.market_state`
- External inputs: `features.perp.v2`

### `build_microstructure_rollup*`

- Inputs: `features.perp.v2`
- Optional inputs: _none_
- Outputs: `context.microstructure`
- External inputs: `features.perp.v2`

### `validate_feature_integrity*`

- Inputs: `features.perp.v2`
- Optional inputs: _none_
- Outputs: _none_
- External inputs: _none_

### `validate_data_coverage*`

- Inputs: `clean.perp.*`
- Optional inputs: _none_
- Outputs: _none_
- External inputs: _none_

### `validate_context_entropy`

- Inputs: `context.features`
- Optional inputs: _none_
- Outputs: _none_
- External inputs: _none_

### `build_normalized_replay_stream`

- Inputs: `metadata.universe_snapshots`
- Optional inputs: _none_
- Outputs: `runtime.normalized_stream`
- External inputs: _none_

### `run_causal_lane_ticks`

- Inputs: `runtime.normalized_stream`
- Optional inputs: _none_
- Outputs: `runtime.causal_ticks`
- External inputs: _none_

### `run_determinism_replay_checks`

- Inputs: `runtime.causal_ticks`
- Optional inputs: _none_
- Outputs: `runtime.determinism_checks`
- External inputs: _none_

### `run_oms_replay_validation`

- Inputs: `runtime.causal_ticks`
- Optional inputs: _none_
- Outputs: `runtime.oms_replay`
- External inputs: _none_

### `analyze_*`

- Inputs: _none_
- Optional inputs: `features.perp.v2`, `context.market_state`, `context.microstructure`
- Outputs: `phase1.events.{event_type}`
- External inputs: `features.perp.v2`, `context.market_state`, `context.microstructure`

### `phase1_correlation_clustering`

- Inputs: _none_
- Optional inputs: `phase1.events.*`
- Outputs: `phase1.correlation_clustering`
- External inputs: _none_

### `build_event_registry*`

- Inputs: _none_
- Optional inputs: `phase1.events.*`
- Outputs: `phase2.event_registry.{event_type}`
- External inputs: `phase1.events.*`

### `canonicalize_event_episodes*`

- Inputs: `phase2.event_registry.{event_type}`
- Optional inputs: _none_
- Outputs: `phase2.event_episodes.{event_type}`
- External inputs: `phase2.event_registry.{event_type}`

### `phase2_search_engine`

- Inputs: `features.perp.v2`
- Optional inputs: _none_
- Outputs: `phase2.candidates.search`
- External inputs: `features.perp.v2`

### `analyze_interaction_lift`

- Inputs: `phase2.candidates.*`
- Optional inputs: _none_
- Outputs: `research.interaction_lift`
- External inputs: `phase2.candidates.*`

### `finalize_experiment`

- Inputs: _none_
- Optional inputs: `phase2.candidates.*`
- Outputs: `experiment.tested_ledger`
- External inputs: _none_

### `summarize_discovery_quality`

- Inputs: `phase2.candidates.*`
- Optional inputs: `phase2.bridge_summary.*`
- Outputs: `phase2.discovery_quality_summary`
- External inputs: _none_

### `evaluate_naive_entry`

- Inputs: `phase2.candidates.*`
- Optional inputs: _none_
- Outputs: `phase2.naive_entry_eval`
- External inputs: _none_

### `export_edge_candidates`

- Inputs: `phase2.candidates.*`
- Optional inputs: `phase2.bridge_metrics.*`
- Outputs: `edge_candidates.normalized`
- External inputs: `phase2.candidates.*`, `phase2.bridge_metrics.*`

### `generate_negative_control_summary`

- Inputs: `edge_candidates.normalized`
- Optional inputs: _none_
- Outputs: `research.negative_control_summary`
- External inputs: `edge_candidates.normalized`

### `promote_candidates`

- Inputs: `edge_candidates.normalized`, `research.negative_control_summary`
- Optional inputs: `phase2.bridge_metrics.*`, `phase2.naive_entry_eval`
- Outputs: `promotion.audit`, `promotion.promoted_candidates`
- External inputs: `edge_candidates.normalized`, `research.negative_control_summary`, `phase2.bridge_metrics.*`, `phase2.naive_entry_eval`

### `update_edge_registry`

- Inputs: `promotion.audit`, `promotion.promoted_candidates`
- Optional inputs: _none_
- Outputs: `history.candidate.edge_observations`, `history.candidate.edge_registry`, `edge_registry.snapshot`
- External inputs: `promotion.audit`, `promotion.promoted_candidates`

### `update_campaign_memory`

- Inputs: _none_
- Optional inputs: `edge_candidates.normalized`, `promotion.audit`, `history.candidate.edge_registry`, `phase2.discovery_quality_summary`
- Outputs: `experiment.memory.tested_regions`, `experiment.memory.reflections`, `experiment.memory.failures`
- External inputs: `edge_candidates.normalized`, `promotion.audit`, `history.candidate.edge_registry`, `phase2.discovery_quality_summary`

### `analyze_conditional_expectancy`

- Inputs: `history.candidate.edge_registry`
- Optional inputs: _none_
- Outputs: `research.expectancy_analysis`
- External inputs: `history.candidate.edge_registry`

### `validate_expectancy_traps`

- Inputs: `research.expectancy_analysis`
- Optional inputs: _none_
- Outputs: `research.expectancy_traps`
- External inputs: _none_

### `generate_recommendations_checklist`

- Inputs: `research.expectancy_traps`
- Optional inputs: _none_
- Outputs: `research.recommendations_checklist`
- External inputs: _none_

### `compile_strategy_blueprints`

- Inputs: `research.recommendations_checklist`
- Optional inputs: _none_
- Outputs: `strategy.blueprints`
- External inputs: _none_

### `build_strategy_candidates`

- Inputs: _none_
- Optional inputs: `research.recommendations_checklist`, `strategy.blueprints`
- Outputs: `strategy.candidates`
- External inputs: _none_

### `select_profitable_strategies`

- Inputs: _none_
- Optional inputs: `strategy.candidates`
- Outputs: `strategy.profitable`
- External inputs: `strategy.candidates`

## Typed Artifact Contracts

### `discovery_phase2_candidates`

- Producer family: `phase2_discovery`
- Consumer families: `validation`, `operator`
- Schema: `phase2_candidates` @ `phase5_schema_v1`
- Path pattern: `reports/phase2/{run_id}/phase2_candidates.parquet`
- Strictness: `strict`
- Legacy aliases: _none_

### `validation_bundle`

- Producer family: `validation`
- Consumer families: `promotion`, `operator`
- Schema: `validation_bundle` @ `validation_bundle_v1`
- Path pattern: `reports/validation/{run_id}/validation_bundle.json`
- Strictness: `strict`
- Legacy aliases: _none_

### `promotion_ready_candidates`

- Producer family: `validation`
- Consumer families: `promotion`, `operator`
- Schema: `promotion_ready_candidates` @ `phase5_schema_v1`
- Path pattern: `reports/validation/{run_id}/promotion_ready_candidates.parquet`
- Strictness: `strict`
- Legacy aliases: `reports/validation/{run_id}/promotion_ready_candidates.csv`

### `promoted_theses`

- Producer family: `promotion`
- Consumer families: `deploy`, `operator`
- Schema: `promoted_theses_payload` @ `promoted_theses_v1`
- Path pattern: `live/theses/{run_id}/promoted_theses.json`
- Strictness: `strict`
- Legacy aliases: _none_

### `live_thesis_index`

- Producer family: `promotion`
- Consumer families: `deploy`, `operator`
- Schema: `live_thesis_index` @ `promoted_thesis_index_v1`
- Path pattern: `live/theses/index.json`
- Strictness: `strict`
- Legacy aliases: _none_

### `run_manifest`

- Producer family: `run_orchestration`
- Consumer families: `validation`, `promotion`, `deploy`, `operator`
- Schema: `run_manifest` @ `run_manifest_v1`
- Path pattern: `runs/{run_id}/run_manifest.json`
- Strictness: `strict`
- Legacy aliases: `runs/{run_id}/manifest.json`
