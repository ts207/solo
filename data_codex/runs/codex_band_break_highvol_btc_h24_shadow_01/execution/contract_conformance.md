# Contract Conformance

- Run ID: `codex_band_break_highvol_btc_h24_shadow_01`
- Verified at: `2026-04-30T19:46:34Z`
- Final status: `success`
- Stage mismatches: `0`
- Artifact mismatches: `0`

## Stage Verification

- `build_features_5m` planned=`selected` actual=`success`
- `build_universe_snapshots` planned=`selected` actual=`success`
- `build_market_context_5m` planned=`selected` actual=`success`
- `build_microstructure_rollup_5m` planned=`selected` actual=`success`
- `validate_feature_integrity_5m` planned=`selected` actual=`success`
- `validate_data_coverage_5m` planned=`selected` actual=`success`
- `build_normalized_replay_stream` planned=`selected` actual=`success`
- `run_causal_lane_ticks` planned=`selected` actual=`success`
- `analyze_events__BAND_BREAK_5m` planned=`selected` actual=`success`
- `build_event_registry__BAND_BREAK_5m` planned=`selected` actual=`success`
- `canonicalize_event_episodes__BAND_BREAK_5m` planned=`selected` actual=`success`
- `phase1_correlation_clustering` planned=`selected` actual=`success`
- `phase2_search_engine` planned=`selected` actual=`success`
- `summarize_discovery_quality` planned=`selected` actual=`success`
- `export_edge_candidates` planned=`selected` actual=`success`
- `update_edge_registry` planned=`selected` actual=`success`
- `update_campaign_memory` planned=`selected` actual=`success`
- `analyze_conditional_expectancy` planned=`selected` actual=`success`
- `validate_expectancy_traps` planned=`selected` actual=`success`
- `generate_recommendations_checklist` planned=`selected` actual=`success`
- `finalize_experiment` planned=`selected` actual=`success`
- `build_cleaned_5m` planned=`skipped` actual=`skipped`
- `ingest_bybit_derivatives_ohlcv_5m` planned=`skipped` actual=`skipped`
- `ingest_bybit_derivatives_funding` planned=`skipped` actual=`skipped`

## Artifact Verification

- `discovery_phase2_candidates` status=`conformant` expected=`reports/phase2/codex_band_break_highvol_btc_h24_shadow_01/phase2_candidates.parquet` actual=`data_codex/reports/phase2/codex_band_break_highvol_btc_h24_shadow_01/phase2_candidates.parquet`
- `run_manifest` status=`conformant` expected=`runs/codex_band_break_highvol_btc_h24_shadow_01/run_manifest.json` actual=`data_codex/runs/codex_band_break_highvol_btc_h24_shadow_01/run_manifest.json`
