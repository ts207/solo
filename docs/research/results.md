# All Results - Edge Discovery Project

*Auto-generated. Do not edit manually - rerun `project/scripts/update_results_index.py`.*
*692 indexed rows across 42 events.*
*Decision fields come from discover-doctor plus `docs/research/decisions.yaml`.*
*`year_split_event_support_pass` means event support is not dominated by one year; it is not PnL stability unless per-event returns are available.*

## Summary - Decision Row Per Event

| Event | Dir | Horizon | Template | t | rob | q | net bps | Evidence | Decision | Reason |
|---|---|---:|---|---:|---:|---:|---:|---|---|---|
| ABSORPTION_PROXY | long | 48 | mean_reversion | 0.59 | 0.538 | 0.2782 | 6.8 | review_only | review | below_bridge_gate |
| BAND_BREAK | long | 24 | mean_reversion | - | - | - | - | killed_candidate | kill | governed_reproduction_failed_no_bridge_candidates |
| BASIS_DISLOC | long | 12 | mean_reversion | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| BREAKOUT_TRIGGER | short | 24 | continuation | 1.45 | 0.582 | 0.0733 | 15.7 | killed_candidate | kill | oos_validation_failed |
| CHOP_TO_TREND_SHIFT | long | 12 | continuation | 1.00 | 0.503 | 0.1581 | 13.8 | killed_candidate | kill | oos_validation_failed |
| CLIMAX_VOLUME_BAR | long | 24 | exhaustion_reversal | 3.83 | 0.717 | 0.0001 | 23.2 | parked_candidate | park | forward_confirmation_failed |
| CROSS_VENUE_DESYNC | long | 12 | convergence | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| DEPTH_COLLAPSE | long | 12 | mean_reversion | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| FAILED_CONTINUATION | long | 48 | exhaustion_reversal | 2.97 | 0.476 | 0.0015 | 43.0 | candidate_signal | review | local_discovery_signal |
| FALSE_BREAKOUT | long | 48 | exhaustion_reversal | - | - | - | - | killed_candidate | kill | specificity_and_governed_reproduction_failed |
| FND_DISLOC | long | 12 | mean_reversion | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| FUNDING_EXTREME_ONSET | long | 24 | continuation | 2.86 | 0.865 | 0.0021 | 24.6 | validate_ready | validate | bridge_candidates_present |
| FUNDING_FLIP | short | 48 | continuation | 0.98 | 0.743 | 0.1647 | 37.5 | killed_candidate | kill | oos_validation_failed |
| GAP_OVERSHOOT | long | 48 | mean_reversion | 0.53 | 0.397 | 0.2978 | 7.5 | review_only | review | below_bridge_gate |
| LIQUIDATION_CASCADE_PROXY | long | 48 | exhaustion_reversal | 1.42 | 0.697 | 0.0783 | 14.2 | review_only | review | below_bridge_gate |
| LIQUIDITY_GAP_PRINT | long | 12 | mean_reversion | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| LIQUIDITY_SHOCK | long | 12 | continuation | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | below_bridge_gate |
| LIQUIDITY_STRESS_DIRECT | long | 12 | continuation | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | below_bridge_gate |
| LIQUIDITY_STRESS_PROXY | long | 12 | continuation | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | below_bridge_gate |
| MOMENTUM_DIVERGENCE_TRIGGER | short | 24 | exhaustion_reversal | 2.15 | 0.351 | 0.0157 | 47.7 | candidate_signal | review | local_discovery_signal |
| OI_FLUSH | short | 48 | exhaustion_reversal | 1.28 | 0.374 | 0.1011 | 9.6 | review_only | review | below_bridge_gate |
| OI_SPIKE_NEGATIVE | long | 12 | convexity_capture | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | below_bridge_gate |
| OI_SPIKE_POSITIVE | long | 12 | convexity_capture | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | below_bridge_gate |
| OVERSHOOT_AFTER_SHOCK | long | 48 | mean_reversion | 2.91 | 0.453 | 0.0018 | 37.0 | parked_candidate | monitor | robustness_failed_monitor_only |
| PRICE_DOWN_OI_DOWN | long | 24 | mean_reversion | 2.35 | 0.839 | 0.0095 | 42.0 | parked_candidate | park | year_conditional_pnl_concentration_2022_specificity_controls_missing |
| PRICE_VOL_IMBALANCE_PROXY | short | 48 | continuation | 1.40 | 0.179 | 0.0805 | 12.7 | review_only | review | below_bridge_gate |
| RANGE_BREAKOUT | long | 12 | breakout_followthrough | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | invalid_or_insufficient_metrics |
| RANGE_COMPRESSION_END | long | 24 | continuation | 0.83 | 0.623 | 0.2024 | 8.1 | killed_candidate | kill | oos_validation_failed |
| SLIPPAGE_SPIKE_EVENT | long | 48 | mean_reversion | 2.99 | 0.465 | 0.0014 | 23.8 | candidate_signal | review | local_discovery_signal |
| SPOT_PERP_BASIS_SHOCK | long | 12 | convergence | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| SPREAD_BLOWOUT | long | 48 | mean_reversion | 0.93 | 0.342 | 0.1773 | 3.0 | killed_candidate | kill | oos_validation_failed |
| SUPPORT_RESISTANCE_BREAK | long | 48 | breakout_followthrough | 1.24 | 0.519 | 0.1080 | 12.1 | killed_candidate | kill | oos_validation_failed |
| SWEEP_STOPRUN | long | 48 | mean_reversion | 1.12 | 0.620 | 0.1306 | 12.8 | killed_candidate | kill | oos_validation_failed |
| TREND_ACCELERATION | long | 12 | breakout_followthrough | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | invalid_or_insufficient_metrics |
| TREND_DECELERATION | short | 48 | continuation | 1.06 | 0.262 | 0.1451 | 19.2 | killed_candidate | kill | oos_validation_failed |
| TREND_EXHAUSTION_TRIGGER | long | 12 | exhaustion_reversal | 0.00 | 0.000 | 1.0000 | 0.0 | review_only | review | invalid_or_insufficient_metrics |
| TREND_TO_CHOP_SHIFT | long | 12 | continuation | 0.00 | 0.000 | 1.0000 | 0.0 | killed_candidate | kill | oos_validation_failed |
| VOL_CLUSTER_SHIFT | long | 48 | continuation | 0.82 | 0.622 | 0.2074 | 3.1 | killed_candidate | kill | oos_validation_failed |
| VOL_REGIME_SHIFT_EVENT | short | 24 | continuation | 1.04 | 0.451 | 0.1497 | 4.6 | killed_candidate | kill | oos_validation_failed |
| VOL_SHOCK | short | 48 | continuation | 2.18 | 0.369 | 0.0146 | 50.7 | candidate_signal | review | local_discovery_signal |
| WICK_REVERSAL_PROXY | long | 48 | mean_reversion | 2.12 | 0.519 | 0.0170 | 15.9 | candidate_signal | review | local_discovery_signal |
| ZSCORE_STRETCH | long | 48 | mean_reversion | 1.73 | 0.414 | 0.0417 | 11.8 | killed_candidate | kill | oos_validation_failed |

## Active Mechanism-Backed Candidates

| Mechanism | Event | Symbol | Context | Dir | Horizon | Template | Evidence | Decision | Run |
|---|---|---|---|---|---:|---|---|---|---|

## Full Index

| Epoch | Mechanism | Active | Event | Symbol | Context | Dir | Horizon | Template | n | events | t | net bps | nearby | Evidence | Decision | Run |
|---|---|---|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---|---|---|
| pre_mechanism |  | False | ABSORPTION_PROXY | BTCUSDT |  | long | 12 | mean_reversion | 123 | 123 | 0.40 | 2.5 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | ABSORPTION_PROXY | BTCUSDT |  | long | 24 | mean_reversion | 107 | 107 | 0.13 | 1.2 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | ABSORPTION_PROXY | BTCUSDT |  | long | 48 | mean_reversion | 123 | 123 | 0.59 | 6.8 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 12 | mean_reversion | 1938 | 1938 | 3.96 | 9.1 | 8 | candidate_signal | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 8 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 12 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 12 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 24 | mean_reversion | 1938 | 1938 | 3.28 | 11.0 | 10 | validate_ready | validate | `stat_stretch_eth_01` |
| pre_mechanism |  | False | BAND_BREAK |  |  | long | 24 | mean_reversion | 374 | 374 | 0.94 | 6.0 | 1 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T100513Z_...` |
| pre_mechanism |  | False | BAND_BREAK |  |  | long | 24 | mean_reversion | 374 | 374 | 0.94 | 6.0 | 1 | killed_candidate | kill | `single_event_band_break__20260429T051949Z_...` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 24 | mean_reversion | 374 | 374 | 0.94 | 6.0 | 10 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T100513Z_...` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 24 | mean_reversion | 374 | 374 | 0.94 | 6.0 | 10 | killed_candidate | kill | `single_event_band_break__20260429T051949Z_...` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 24 | mean_reversion | 3590 | 3590 | 0.01 | 0.0 | 9 | killed_candidate | kill | `raw_band_break_highvol_cell_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 9 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 10 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 24 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 9 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 24 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 9 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT | vol_regime=low | long | 24 | mean_reversion | - | - | - | - | 10 | killed_candidate | kill | `single_event_band_break__20260429T051949Z_...` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 48 | mean_reversion | 1938 | 1938 | 1.59 | 10.1 | 8 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | ETHUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 8 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 48 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | BAND_BREAK | BTCUSDT |  | long | 48 | mean_reversion | 12169 | 12169 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | long | 12 | mean_reversion | 614 | 614 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | long | 24 | mean_reversion | 614 | 614 | 0.00 | 0.0 | 2 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | long | 48 | mean_reversion | 614 | 614 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | short | 12 | mean_reversion | 4 | 4 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | short | 24 | mean_reversion | 4 | 4 | 0.00 | 0.0 | 2 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BASIS_DISLOC | BTCUSDT |  | short | 48 | mean_reversion | 4 | 4 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 12 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 12 | continuation | 1050 | 1050 | -1.59 | -8.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 24 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 24 | continuation | 1050 | 1050 | -0.67 | -4.3 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 48 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | long | 48 | continuation | 1050 | 1050 | -1.50 | -12.4 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 12 | continuation | 1050 | 1050 | 0.80 | 4.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 12 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER |  |  | short | 24 | continuation | 254 | 254 | 1.45 | 15.7 | 0 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | BTCUSDT |  | short | 24 | continuation | 254 | 254 | 1.45 | 15.7 | 1 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 24 | continuation | 1050 | 1050 | 0.04 | 0.3 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 24 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 48 | continuation | 1050 | 1050 | 1.02 | 8.4 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | BREAKOUT_TRIGGER | ETHUSDT |  | short | 48 | volatility_expansion_follow | 1050 | 1050 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 12 | continuation | 63 | 63 | 1.00 | 13.8 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 12 | mean_reversion | 27 | 27 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 24 | continuation | 63 | 63 | 0.59 | 10.5 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 24 | mean_reversion | 27 | 27 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 48 | continuation | 63 | 63 | 0.80 | 17.3 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | long | 48 | mean_reversion | 27 | 27 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | short | 12 | continuation | 27 | 27 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | short | 24 | continuation | 27 | 27 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CHOP_TO_TREND_SHIFT | BTCUSDT |  | short | 48 | continuation | 27 | 27 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 12 | exhaustion_reversal | 472 | 472 | 2.14 | 9.7 | 8 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 12 | exhaustion_reversal | 472 | 472 | 1.88 | 15.1 | 8 | candidate_signal | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | long | 12 | exhaustion_reversal | 829 | 829 | 1.40 | 8.0 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 12 | exhaustion_reversal | 843 | 843 | 1.27 | 3.9 | 8 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 472 | 472 | 3.83 | 23.2 | 11 | parked_candidate | park | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 472 | 472 | 2.34 | 23.9 | 11 | parked_candidate | park | `raw_climax_cell_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 472 | 472 | 2.34 | 23.9 | 11 | parked_candidate | park | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR |  |  | long | 24 | exhaustion_reversal | 309 | 309 | 2.25 | 26.0 | 1 | parked_candidate | park | `single_event_climax_volu_20260428T212745Z_...` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR |  |  | long | 24 | exhaustion_reversal | 309 | 309 | 2.25 | 26.0 | 1 | parked_candidate | park | `single_event_climax_volu_20260428T214026Z_...` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 309 | 309 | 2.25 | 26.0 | 11 | parked_candidate | park | `single_event_climax_volu_20260428T212745Z_...` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 309 | 309 | 2.25 | 26.0 | 11 | parked_candidate | park | `single_event_climax_volu_20260428T214026Z_...` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 24 | exhaustion_reversal | 843 | 843 | 1.77 | 7.2 | 11 | parked_candidate | park | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | long | 24 | exhaustion_reversal | 2686 | 2686 | 0.80 | 3.9 | 2 | parked_candidate | park | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 48 | exhaustion_reversal | 472 | 472 | 3.74 | 29.8 | 8 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 48 | exhaustion_reversal | 843 | 843 | 2.58 | 13.9 | 8 | candidate_signal | review | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | long | 48 | exhaustion_reversal | 843 | 843 | 1.93 | 16.4 | 8 | candidate_signal | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | long | 48 | exhaustion_reversal | 2685 | 2685 | 1.56 | 10.1 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | short | 12 | exhaustion_reversal | 785 | 785 | 1.56 | 11.7 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 12 | exhaustion_reversal | 1499 | 1499 | 0.31 | 1.2 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 12 | exhaustion_reversal | 1499 | 1499 | 0.31 | 1.2 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 12 | exhaustion_reversal | 778 | 778 | -0.08 | -0.5 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | short | 24 | exhaustion_reversal | 785 | 785 | 1.29 | 10.4 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 24 | exhaustion_reversal | 778 | 778 | 0.73 | 6.6 | 8 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 24 | exhaustion_reversal | 2798 | 2798 | -0.36 | -1.4 | 8 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 24 | exhaustion_reversal | 1554 | 1554 | -0.36 | -1.9 | 8 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | ETHUSDT |  | short | 48 | exhaustion_reversal | 784 | 784 | 1.29 | 12.3 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 48 | exhaustion_reversal | 1553 | 1553 | 0.85 | 5.8 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 48 | exhaustion_reversal | 1553 | 1553 | 0.85 | 5.8 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | CLIMAX_VOLUME_BAR | BTCUSDT |  | short | 48 | exhaustion_reversal | 777 | 777 | 0.68 | 6.7 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | lead_lag_follow | 79 | 79 | 1.48 | 25.2 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | lead_lag_follow | 225 | 225 | 1.24 | 20.9 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | convergence | 784 | 784 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | convergence | 784 | 784 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | convergence | 487 | 487 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 12 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 12 | convergence | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 12 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 12 | lead_lag_follow | 181 | 181 | -0.71 | -15.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | lead_lag_follow | 107 | 107 | 1.64 | 48.8 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | lead_lag_follow | 225 | 225 | 0.72 | 16.2 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | convergence | 784 | 784 | 0.00 | 0.0 | 8 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | convergence | 784 | 784 | 0.00 | 0.0 | 8 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | convergence | 487 | 487 | 0.00 | 0.0 | 8 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 24 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 24 | convergence | 614 | 614 | 0.00 | 0.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 24 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 24 | lead_lag_follow | 181 | 181 | -0.12 | -3.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | lead_lag_follow | 107 | 107 | 2.44 | 76.1 | 3 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 48 | lead_lag_follow | 181 | 181 | 1.85 | 57.3 | 1 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | lead_lag_follow | 225 | 225 | 0.45 | 11.2 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | convergence | 784 | 784 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | convergence | 784 | 784 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | convergence | 487 | 487 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | long | 48 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 48 | convergence | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | long | 48 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 12 | lead_lag_follow | 229 | 229 | 1.98 | 21.7 | 1 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 12 | lead_lag_follow | 296 | 296 | 0.87 | 6.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 12 | lead_lag_follow | 213 | 213 | 0.63 | 6.9 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 12 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 12 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 12 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 24 | lead_lag_follow | 229 | 229 | 1.80 | 22.1 | 2 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 24 | lead_lag_follow | 150 | 150 | 1.29 | 19.1 | 7 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 24 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 24 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 24 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 24 | lead_lag_follow | 213 | 213 | -0.04 | -0.5 | 7 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 48 | lead_lag_follow | 150 | 150 | 1.89 | 35.9 | 5 | candidate_signal | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 48 | lead_lag_follow | 167 | 167 | 0.93 | 14.1 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 48 | lead_lag_follow | 229 | 229 | 0.58 | 9.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC |  |  | short | 48 | lead_lag_follow | 6 | 6 | 0.00 | 0.0 | 0 | review_only | review | `spot_desync_confirm_01` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 48 | divergence_continuation | 784 | 784 | 0.00 | 0.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 48 | divergence_continuation | 487 | 487 | 0.00 | 0.0 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | ETHUSDT |  | short | 48 | divergence_continuation | 614 | 614 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | CROSS_VENUE_DESYNC | BTCUSDT |  | short | 48 | lead_lag_follow | 6 | 6 | 0.00 | 0.0 | 5 | review_only | review | `spot_desync_confirm_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 12 | continuation | 184 | 184 | 0.43 | 1.6 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 12 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 12 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 24 | continuation | 64 | 64 | 1.14 | 9.6 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 24 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 5 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 24 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 5 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 48 | continuation | 64 | 64 | 1.68 | 18.5 | 1 | candidate_signal | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 48 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | long | 48 | mean_reversion | 184 | 184 | 0.00 | 0.0 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | short | 12 | continuation | 14 | 14 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | short | 24 | continuation | 14 | 14 | 0.00 | 0.0 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | DEPTH_COLLAPSE | BTCUSDT |  | short | 48 | continuation | 14 | 14 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 12 | exhaustion_reversal | 169 | 169 | 2.30 | 18.3 | 5 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 12 | exhaustion_reversal | 169 | 169 | 1.84 | 24.3 | 5 | candidate_signal | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | long | 12 | exhaustion_reversal | 236 | 236 | 1.35 | 19.1 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 12 | exhaustion_reversal | 260 | 260 | 0.84 | 5.1 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 24 | exhaustion_reversal | 169 | 169 | 2.06 | 23.1 | 8 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | long | 24 | exhaustion_reversal | 236 | 236 | 1.19 | 20.7 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 24 | exhaustion_reversal | 169 | 169 | 0.82 | 15.4 | 8 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 24 | exhaustion_reversal | 260 | 260 | 0.31 | 2.4 | 8 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 48 | exhaustion_reversal | 169 | 169 | 2.97 | 43.0 | 5 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | long | 48 | exhaustion_reversal | 236 | 236 | 1.74 | 39.5 | 1 | candidate_signal | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 48 | exhaustion_reversal | 260 | 260 | 0.76 | 7.1 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | long | 48 | exhaustion_reversal | 260 | 260 | 0.67 | 9.5 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 12 | exhaustion_reversal | 257 | 257 | 2.53 | 11.6 | 5 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 12 | exhaustion_reversal | 257 | 257 | 2.53 | 11.6 | 5 | candidate_signal | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 12 | exhaustion_reversal | 257 | 257 | 1.16 | 8.7 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | short | 12 | exhaustion_reversal | 577 | 577 | -0.03 | -0.2 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 24 | exhaustion_reversal | 257 | 257 | 2.19 | 13.4 | 8 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 24 | exhaustion_reversal | 257 | 257 | 2.19 | 13.4 | 8 | candidate_signal | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 24 | exhaustion_reversal | 257 | 257 | 0.81 | 7.3 | 8 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | short | 24 | exhaustion_reversal | 328 | 328 | 0.05 | 0.7 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 48 | exhaustion_reversal | 257 | 257 | 1.37 | 11.3 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 48 | exhaustion_reversal | 257 | 257 | 1.37 | 11.3 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FAILED_CONTINUATION | BTCUSDT |  | short | 48 | exhaustion_reversal | 257 | 257 | 1.13 | 13.5 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FAILED_CONTINUATION | ETHUSDT |  | short | 48 | exhaustion_reversal | 576 | 576 | -0.78 | -8.6 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 12 | exhaustion_reversal | 767 | 767 | 2.07 | 11.9 | 3 | candidate_signal | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | long | 12 | exhaustion_reversal | 418 | 418 | 1.24 | 14.7 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 12 | exhaustion_reversal | 298 | 298 | 0.59 | 4.2 | 3 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 12 | continuation | 298 | 298 | 0.59 | 4.2 | 1 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 24 | exhaustion_reversal | 176 | 176 | 1.92 | 22.3 | 8 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 24 | exhaustion_reversal | 293 | 293 | 1.58 | 21.5 | 8 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 24 | continuation | 298 | 298 | 0.71 | 5.7 | 2 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | long | 24 | exhaustion_reversal | 418 | 418 | 0.58 | 8.2 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 48 | exhaustion_reversal | 176 | 176 | 2.92 | 39.8 | 6 | candidate_signal | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 48 | exhaustion_reversal | 293 | 293 | 2.09 | 30.4 | 6 | validate_ready | validate | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 48 | continuation | 293 | 293 | 1.16 | 9.7 | 1 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT |  |  | long | 48 | exhaustion_reversal | 274 | 274 | 0.86 | 11.6 | 0 | killed_candidate | kill | `single_event_false_break_20260429T052713Z_...` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | long | 48 | exhaustion_reversal | 274 | 274 | 0.86 | 11.6 | 6 | killed_candidate | kill | `single_event_false_break_20260429T052713Z_...` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | long | 48 | exhaustion_reversal | 418 | 418 | 0.61 | 9.9 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT | ms_trend_state=bullish | long | 48 | exhaustion_reversal | - | - | - | - | 6 | killed_candidate | kill | `single_event_false_break_20260429T052713Z_...` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 12 | exhaustion_reversal | 520 | 520 | 0.40 | 3.0 | 3 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 12 | continuation | 520 | 520 | 0.40 | 3.0 | 1 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | short | 12 | exhaustion_reversal | 447 | 447 | -0.10 | -0.8 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 12 | exhaustion_reversal | 348 | 348 | -0.51 | -3.8 | 3 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 24 | exhaustion_reversal | 348 | 348 | 1.39 | 13.7 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 24 | exhaustion_reversal | 520 | 520 | 0.86 | 7.1 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 24 | continuation | 520 | 520 | 0.86 | 7.1 | 2 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | short | 24 | exhaustion_reversal | 343 | 343 | 0.78 | 9.4 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 48 | exhaustion_reversal | 348 | 348 | 0.49 | 6.0 | 3 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | FALSE_BREAKOUT | ETHUSDT |  | short | 48 | exhaustion_reversal | 343 | 343 | 0.44 | 6.2 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 48 | exhaustion_reversal | 939 | 939 | 0.38 | 3.4 | 3 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | FALSE_BREAKOUT | BTCUSDT |  | short | 48 | continuation | 520 | 520 | 0.29 | 3.4 | 1 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | long | 12 | mean_reversion | 25 | 25 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | long | 24 | mean_reversion | 25 | 25 | 0.00 | 0.0 | 2 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | long | 48 | mean_reversion | 25 | 25 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | short | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | short | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 2 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FND_DISLOC | BTCUSDT |  | short | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 1 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 12 | continuation | 234 | 234 | 2.53 | 20.4 | 1 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 12 | continuation | 231 | 231 | 2.21 | 16.8 | 2 | validate_ready | validate | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 12 | mean_reversion | 1067 | 1067 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 12 | mean_reversion | 1081 | 1081 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 24 | continuation | 237 | 237 | 2.86 | 24.6 | 3 | validate_ready | validate | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 24 | continuation | 288 | 288 | 2.82 | 28.7 | 2 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET |  |  | long | 24 | continuation | 105 | 105 | 2.08 | 45.3 | 0 | candidate_signal | review | `governed_sweep_20260429_v1_funding_btcusdt` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 24 | continuation | 105 | 105 | 2.08 | 45.3 | 3 | candidate_signal | review | `governed_sweep_20260429_v1_funding_btcusdt` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 24 | mean_reversion | 1067 | 1067 | 0.00 | 0.0 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 24 | mean_reversion | 1081 | 1081 | 0.00 | 0.0 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 48 | continuation | 231 | 231 | 2.76 | 45.8 | 2 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 48 | continuation | 288 | 288 | 2.55 | 34.8 | 1 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | long | 48 | mean_reversion | 1066 | 1066 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | long | 48 | mean_reversion | 1079 | 1079 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | short | 12 | continuation | 289 | 289 | 1.78 | 12.3 | 1 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | short | 12 | continuation | 385 | 385 | -0.51 | -1.6 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | short | 24 | continuation | 289 | 289 | 1.69 | 19.1 | 2 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | short | 24 | continuation | 385 | 385 | -0.08 | -0.5 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | ETHUSDT |  | short | 48 | continuation | 289 | 289 | 0.87 | 13.2 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_EXTREME_ONSET | BTCUSDT |  | short | 48 | continuation | 385 | 385 | -0.47 | -4.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | continuation | 19 | 19 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | reversal_or_squeeze | 19 | 19 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | continuation | 14 | 14 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 12 | reversal_or_squeeze | 14 | 14 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | continuation | 34 | 34 | 0.50 | 10.7 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | reversal_or_squeeze | 34 | 34 | 0.50 | 10.7 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | continuation | 19 | 19 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | reversal_or_squeeze | 19 | 19 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 24 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | continuation | 19 | 19 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | reversal_or_squeeze | 19 | 19 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | continuation | 14 | 14 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | mean_reversion | 67 | 67 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | long | 48 | reversal_or_squeeze | 14 | 14 | 0.00 | 0.0 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 12 | continuation | 67 | 67 | 0.53 | 4.9 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 12 | reversal_or_squeeze | 67 | 67 | 0.53 | 4.9 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 12 | continuation | 67 | 67 | 0.53 | 4.9 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 12 | reversal_or_squeeze | 67 | 67 | 0.53 | 4.9 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 24 | continuation | 19 | 19 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 24 | reversal_or_squeeze | 19 | 19 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 24 | continuation | 14 | 14 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 24 | reversal_or_squeeze | 14 | 14 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 48 | continuation | 30 | 30 | 0.98 | 37.5 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 48 | reversal_or_squeeze | 30 | 30 | 0.98 | 37.5 | 3 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 48 | continuation | 67 | 67 | 0.46 | 11.7 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | FUNDING_FLIP | BTCUSDT |  | short | 48 | reversal_or_squeeze | 67 | 67 | 0.46 | 11.7 | 3 | killed_candidate | kill | `basis_funding_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 12 | mean_reversion | 701 | 701 | 0.16 | 2.0 | 3 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 12 | mean_reversion | 1831 | 1831 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 12 | mean_reversion | 1831 | 1831 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 24 | mean_reversion | 607 | 607 | 0.50 | 5.5 | 5 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 8 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 24 | mean_reversion | 1831 | 1831 | 0.00 | 0.0 | 8 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 24 | mean_reversion | 1831 | 1831 | 0.00 | 0.0 | 8 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 48 | mean_reversion | 701 | 701 | 0.53 | 7.5 | 3 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | ETHUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 48 | mean_reversion | 1829 | 1829 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | GAP_OVERSHOOT | BTCUSDT |  | long | 48 | mean_reversion | 1829 | 1829 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 12 | exhaustion_reversal | 742 | 742 | 0.27 | 1.4 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 12 | reversal_or_squeeze | 742 | 742 | 0.27 | 1.4 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 12 | exhaustion_reversal | 524 | 524 | -0.26 | -1.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 12 | reversal_or_squeeze | 524 | 524 | -0.26 | -1.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 24 | exhaustion_reversal | 249 | 249 | 0.66 | 5.2 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 24 | reversal_or_squeeze | 249 | 249 | 0.66 | 5.2 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 24 | exhaustion_reversal | 742 | 742 | 0.65 | 4.4 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 24 | reversal_or_squeeze | 742 | 742 | 0.65 | 4.4 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 48 | exhaustion_reversal | 249 | 249 | 1.42 | 14.2 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 48 | reversal_or_squeeze | 249 | 249 | 1.42 | 14.2 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 48 | exhaustion_reversal | 567 | 567 | 0.43 | 4.1 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | long | 48 | reversal_or_squeeze | 567 | 567 | 0.43 | 4.1 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 12 | exhaustion_reversal | 568 | 568 | 0.74 | 4.9 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 12 | reversal_or_squeeze | 568 | 568 | 0.74 | 4.9 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 12 | exhaustion_reversal | 697 | 697 | 0.30 | 1.7 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 12 | reversal_or_squeeze | 697 | 697 | 0.30 | 1.7 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 24 | exhaustion_reversal | 697 | 697 | -0.37 | -2.4 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 24 | reversal_or_squeeze | 697 | 697 | -0.37 | -2.4 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 24 | exhaustion_reversal | 785 | 785 | -0.56 | -5.1 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 24 | reversal_or_squeeze | 785 | 785 | -0.56 | -5.1 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 48 | exhaustion_reversal | 788 | 788 | 1.04 | 8.3 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 48 | reversal_or_squeeze | 788 | 788 | 1.04 | 8.3 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 48 | exhaustion_reversal | 785 | 785 | 0.24 | 3.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDATION_CASCADE_PROXY | BTCUSDT |  | short | 48 | reversal_or_squeeze | 785 | 785 | 0.24 | 3.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 12 | continuation | 1518 | 1518 | 0.48 | 1.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 12 | mean_reversion | 9492 | 9492 | 0.00 | 0.0 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 12 | mean_reversion | 9492 | 9492 | 0.00 | 0.0 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 24 | continuation | 1518 | 1518 | 0.99 | 2.8 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 24 | mean_reversion | 9491 | 9491 | 0.00 | 0.0 | 5 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 24 | mean_reversion | 9491 | 9491 | 0.00 | 0.0 | 5 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 48 | continuation | 1518 | 1518 | 2.37 | 8.8 | 1 | candidate_signal | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 48 | mean_reversion | 9489 | 9489 | 0.00 | 0.0 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | long | 48 | mean_reversion | 9489 | 9489 | 0.00 | 0.0 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | short | 12 | continuation | 1656 | 1656 | -1.16 | -2.3 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | short | 24 | continuation | 1655 | 1655 | -1.07 | -2.8 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_GAP_PRINT | BTCUSDT |  | short | 48 | continuation | 4279 | 4279 | -0.53 | -1.9 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 12 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 24 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | short | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | short | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_SHOCK | BTCUSDT |  | short | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 12 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 24 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | long | 48 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | short | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | short | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_DIRECT | BTCUSDT |  | short | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 12 | continuation | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 12 | mean_reversion | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 24 | continuation | 1 | 1 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 24 | mean_reversion | 1 | 1 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 48 | continuation | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | long | 48 | mean_reversion | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | short | 12 | continuation | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | short | 24 | continuation | 1 | 1 | 0.00 | 0.0 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | LIQUIDITY_STRESS_PROXY | BTCUSDT |  | short | 48 | continuation | 1 | 1 | 0.00 | 0.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 12 | exhaustion_reversal | 118 | 118 | 1.41 | 10.5 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 12 | exhaustion_reversal | 135 | 135 | 1.28 | 7.8 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 12 | exhaustion_reversal | 135 | 135 | 1.28 | 7.8 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | long | 12 | exhaustion_reversal | 43 | 43 | 0.64 | 30.7 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 24 | exhaustion_reversal | 135 | 135 | 1.26 | 10.4 | 8 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 24 | exhaustion_reversal | 135 | 135 | 1.26 | 10.4 | 8 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | long | 24 | exhaustion_reversal | 72 | 72 | 0.58 | 16.7 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 24 | exhaustion_reversal | 135 | 135 | 0.55 | 6.6 | 8 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 48 | exhaustion_reversal | 135 | 135 | 1.40 | 16.3 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 48 | exhaustion_reversal | 135 | 135 | 1.40 | 16.3 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | long | 48 | exhaustion_reversal | 118 | 118 | 0.93 | 16.3 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | long | 48 | exhaustion_reversal | 144 | 144 | 0.75 | 21.1 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 12 | exhaustion_reversal | 137 | 137 | 1.63 | 26.2 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | short | 12 | exhaustion_reversal | 144 | 144 | 1.49 | 26.3 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 12 | exhaustion_reversal | 210 | 210 | 1.14 | 7.6 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 12 | exhaustion_reversal | 210 | 210 | 1.14 | 7.6 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 24 | exhaustion_reversal | 137 | 137 | 2.15 | 47.7 | 8 | candidate_signal | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | short | 24 | exhaustion_reversal | 144 | 144 | 1.63 | 36.3 | 2 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 24 | exhaustion_reversal | 210 | 210 | 1.16 | 10.5 | 8 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 24 | exhaustion_reversal | 210 | 210 | 1.16 | 10.5 | 8 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 48 | exhaustion_reversal | 60 | 60 | 1.15 | 40.3 | 5 | review_only | review | `trend_fail_v1_2021` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 48 | exhaustion_reversal | 210 | 210 | 1.01 | 10.1 | 5 | review_only | review | `trend_fail_v1_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | BTCUSDT |  | short | 48 | exhaustion_reversal | 210 | 210 | 1.01 | 10.1 | 5 | review_only | review | `trend_failure_01` |
| pre_mechanism |  | False | MOMENTUM_DIVERGENCE_TRIGGER | ETHUSDT |  | short | 48 | exhaustion_reversal | 153 | 153 | 0.07 | 1.7 | 1 | review_only | review | `trend_fail_v1_eth` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 12 | exhaustion_reversal | 3429 | 3429 | 1.08 | 3.5 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 12 | exhaustion_reversal | 3429 | 3429 | 1.08 | 3.5 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 24 | exhaustion_reversal | 3383 | 3383 | 1.04 | 4.4 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 24 | exhaustion_reversal | 3429 | 3429 | 1.03 | 4.3 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 48 | exhaustion_reversal | 1437 | 1437 | 0.91 | 6.6 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | long | 48 | exhaustion_reversal | 3427 | 3427 | 0.29 | 1.6 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 12 | exhaustion_reversal | 604 | 604 | 0.62 | 2.6 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 12 | exhaustion_reversal | 992 | 992 | -0.56 | -2.6 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 24 | exhaustion_reversal | 604 | 604 | 0.16 | 0.8 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 24 | exhaustion_reversal | 992 | 992 | -0.21 | -1.3 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 48 | exhaustion_reversal | 924 | 924 | 1.28 | 9.6 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_FLUSH | BTCUSDT |  | short | 48 | exhaustion_reversal | 888 | 888 | 0.04 | 0.5 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 12 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 12 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 24 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 24 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 48 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | long | 48 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 12 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 12 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 24 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 5 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 24 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 5 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 48 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_NEGATIVE | BTCUSDT |  | short | 48 | convexity_capture | 657 | 657 | 0.00 | 0.0 | 3 | review_only | review | `liq_positioning_postfix_20260429` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | long | 12 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 1 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | long | 24 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 2 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | long | 48 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 1 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | short | 12 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 1 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | short | 24 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 2 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OI_SPIKE_POSITIVE | BTCUSDT |  | short | 48 | convexity_capture | 1044 | 1044 | 0.00 | 0.0 | 1 | review_only | review | `liq_pos_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 12 | mean_reversion | 1040 | 1040 | 1.88 | 11.7 | 3 | candidate_signal | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 12 | mean_reversion | 3584 | 3584 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 12 | mean_reversion | 3584 | 3584 | 0.00 | 0.0 | 6 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK |  |  | long | 24 | mean_reversion | 235 | 235 | 0.66 | 7.1 | 1 | review_only | review | `stat_stretch_04` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 24 | mean_reversion | 1015 | 1015 | 0.47 | 3.8 | 7 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 13 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 7 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 24 | mean_reversion | 3578 | 3578 | 0.00 | 0.0 | 13 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 24 | mean_reversion | 3578 | 3578 | 0.00 | 0.0 | 13 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 48 | mean_reversion | 1137 | 1137 | 2.91 | 37.0 | 5 | parked_candidate | monitor | `stat_stretch_eth_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 234 | 234 | 2.09 | 33.1 | 11 | parked_candidate | monitor | `stat_stretch_04` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 48 |  | 234 | 234 | 2.09 | 33.1 | 2 | parked_candidate | monitor | `stat_stretch_04` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK |  |  | long | 48 | mean_reversion | 234 | 234 | 2.09 | 33.1 | 2 | parked_candidate | monitor | `stat_stretch_04` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 48 | mean_reversion | 246 | 246 | 1.16 | 20.8 | 5 | parked_candidate | monitor | `stat_stretch_04` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 11 | parked_candidate | monitor | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | ETHUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 5 | parked_candidate | monitor | `stat_stretch_01` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 3576 | 3576 | 0.00 | 0.0 | 11 | parked_candidate | monitor | `stat_stretch_02` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 3576 | 3576 | 0.00 | 0.0 | 11 | parked_candidate | monitor | `stat_stretch_03` |
| pre_mechanism |  | False | OVERSHOOT_AFTER_SHOCK |  |  | long | 72 | mean_reversion | 233 | 233 | 1.33 | 20.6 | 1 | review_only | review | `stat_stretch_04` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 78 | 78 | 1.55 | 23.1 | 29 | review_only | review | `supported_path_20260427T000843Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 78 | 78 | 1.55 | 23.1 | 29 | review_only | review | `supported_path_20260427T001141Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 78 | 78 | 1.55 | 23.1 | 29 | review_only | review | `supported_path_20260427T001217Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 78 | 78 | 1.55 | 23.1 | 29 | review_only | review | `supported_path_20260427T001344Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 38 | 38 | 1.45 | 23.6 | 29 | killed_candidate | kill | `supported_path_20260426T200321Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 149 | 149 | 0.71 | 6.0 | 29 | killed_candidate | kill | `supported_path_20260426T195830Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 12 | mean_reversion | 149 | 149 | 0.00 | 0.0 | 29 | review_only | review | `supported_path_20260426T194523Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T002214Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T015253Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T015937Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T075010Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T080041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T080250Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T080951Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T081501Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T082809Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T084014Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T084420Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T090041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T092736Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | parked_candidate | park | `supported_path_20260427T180610Z_price_down...` |
| mechanism_backed | forced_flow_reversal | False | PRICE_DOWN_OI_DOWN |  |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 14 | validate_ready | park | `mechanism_forced_flow_price_down_oi_down_h...` |
| mechanism_backed | forced_flow_reversal | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | validate_ready | park | `mechanism_forced_flow_price_down_oi_down_h...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T001920Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T002214Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T015253Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T015937Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T075010Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080250Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080951Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T081501Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T082809Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T084014Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T084420Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T090041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T092736Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T180610Z_price_down...` |
| mechanism_backed | forced_flow_reversal | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | exhaustion_reversal | 79 | 79 | 2.35 | 42.0 | 0 | validate_ready | park | `mechanism_forced_flow_price_down_oi_down_h...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T001920Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T002214Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T015253Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T015937Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T075010Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080250Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T080951Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T081501Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T082809Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T084014Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T084420Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T090041Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T092736Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 |  | 79 | 79 | 2.35 | 42.0 | 29 | parked_candidate | park | `supported_path_20260427T180610Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 78 | 78 | 2.02 | 40.4 | 29 | parked_candidate | park | `supported_path_20260427T000843Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 78 | 78 | 2.02 | 40.4 | 29 | parked_candidate | park | `supported_path_20260427T001141Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 78 | 78 | 2.02 | 40.4 | 29 | parked_candidate | park | `supported_path_20260427T001217Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 78 | 78 | 2.02 | 40.4 | 29 | parked_candidate | park | `supported_path_20260427T001344Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 38 | 38 | 1.87 | 38.6 | 29 | parked_candidate | park | `supported_path_20260426T200321Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 149 | 149 | 0.67 | 6.4 | 29 | parked_candidate | park | `supported_path_20260426T195830Z_price_down...` |
| pre_mechanism |  | False | PRICE_DOWN_OI_DOWN | BTCUSDT |  | long | 24 | mean_reversion | 149 | 149 | 0.00 | 0.0 | 29 | parked_candidate | park | `supported_path_20260426T194523Z_price_down...` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 12 | mean_reversion | 396 | 396 | 1.22 | 6.6 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 12 | continuation | 594 | 594 | 0.51 | 1.9 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 24 | mean_reversion | 469 | 469 | 0.97 | 6.6 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 24 | continuation | 202 | 202 | 0.18 | 1.7 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 48 | mean_reversion | 469 | 469 | 1.35 | 12.2 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | long | 48 | continuation | 197 | 197 | 0.69 | 6.9 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | short | 12 | continuation | 469 | 469 | 1.25 | 6.0 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | short | 24 | continuation | 469 | 469 | 1.00 | 6.9 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | PRICE_VOL_IMBALANCE_PROXY | BTCUSDT |  | short | 48 | continuation | 469 | 469 | 1.40 | 12.7 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | BTCUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_BREAKOUT | ETHUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 12 | continuation | 296 | 296 | 0.34 | 2.6 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 12 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 24 | continuation | 296 | 296 | 0.83 | 8.1 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 24 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 48 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | long | 48 | continuation | 296 | 296 | -0.38 | -4.6 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 12 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 12 | continuation | 296 | 296 | -0.85 | -6.6 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 24 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 24 | continuation | 296 | 296 | -1.24 | -12.1 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 48 | continuation | 296 | 296 | 0.05 | 0.6 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | RANGE_COMPRESSION_END | ETHUSDT |  | short | 48 | volatility_expansion_follow | 296 | 296 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 12 | mean_reversion | 289 | 289 | 1.39 | 7.0 | 3 | review_only | review | `guard_filter_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 12 | mean_reversion | 289 | 289 | 1.39 | 7.0 | 3 | review_only | review | `temporal_guard_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 24 | mean_reversion | 289 | 289 | 1.29 | 7.8 | 5 | review_only | review | `guard_filter_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 24 | mean_reversion | 289 | 289 | 1.29 | 7.8 | 5 | review_only | review | `temporal_guard_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 48 | mean_reversion | 289 | 289 | 2.99 | 23.8 | 3 | candidate_signal | review | `guard_filter_01` |
| pre_mechanism |  | False | SLIPPAGE_SPIKE_EVENT | BTCUSDT |  | long | 48 | mean_reversion | 289 | 289 | 2.99 | 23.8 | 3 | candidate_signal | review | `temporal_guard_01` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 12 | lead_lag_follow | 127 | 127 | 1.53 | 35.4 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 12 | lead_lag_follow | 63 | 63 | 1.31 | 42.5 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 12 | convergence | 417 | 417 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 12 | convergence | 417 | 417 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 12 | convergence | 269 | 269 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 12 | convergence | 376 | 376 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 12 | lead_lag_follow | 121 | 121 | -0.49 | -14.1 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 24 | lead_lag_follow | 63 | 63 | 1.49 | 57.1 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 24 | lead_lag_follow | 127 | 127 | 1.00 | 33.4 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 24 | lead_lag_follow | 121 | 121 | 0.45 | 14.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 24 | convergence | 417 | 417 | 0.00 | 0.0 | 8 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 24 | convergence | 417 | 417 | 0.00 | 0.0 | 8 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 24 | convergence | 269 | 269 | 0.00 | 0.0 | 8 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 24 | convergence | 376 | 376 | 0.00 | 0.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 48 | lead_lag_follow | 63 | 63 | 2.33 | 82.8 | 3 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 48 | lead_lag_follow | 121 | 121 | 2.19 | 88.1 | 1 | candidate_signal | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 48 | lead_lag_follow | 127 | 127 | 1.07 | 38.5 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 48 | convergence | 417 | 417 | 0.00 | 0.0 | 5 | killed_candidate | kill | `basis_funding_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 48 | convergence | 417 | 417 | 0.00 | 0.0 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | long | 48 | convergence | 269 | 269 | 0.00 | 0.0 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | long | 48 | convergence | 376 | 376 | 0.00 | 0.0 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | short | 12 | lead_lag_follow | 131 | 131 | 1.28 | 23.1 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 12 | lead_lag_follow | 171 | 171 | 0.69 | 8.0 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 12 | lead_lag_follow | 128 | 128 | -0.04 | -0.7 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | short | 24 | lead_lag_follow | 131 | 131 | 1.37 | 28.0 | 2 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 24 | lead_lag_follow | 78 | 78 | 0.82 | 22.7 | 5 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 24 | lead_lag_follow | 128 | 128 | -0.46 | -7.3 | 5 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 48 | lead_lag_follow | 78 | 78 | 1.25 | 28.4 | 3 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | BTCUSDT |  | short | 48 | lead_lag_follow | 78 | 78 | 1.13 | 36.2 | 3 | review_only | review | `desync_postfix_20260429` |
| pre_mechanism |  | False | SPOT_PERP_BASIS_SHOCK | ETHUSDT |  | short | 48 | lead_lag_follow | 124 | 124 | 0.39 | 11.7 | 1 | review_only | review | `spot_data_20260429` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 12 | mean_reversion | 1747 | 1747 | 0.29 | 0.7 | 5 | review_only | review | `guard_filter_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 12 | mean_reversion | 1747 | 1747 | 0.29 | 0.7 | 5 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 12 | mean_reversion | 1747 | 1747 | 0.29 | 0.7 | 5 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 12 | continuation | 2740 | 2740 | -1.00 | -1.6 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 24 | mean_reversion | 1747 | 1747 | 0.23 | 0.7 | 8 | review_only | review | `guard_filter_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 24 | mean_reversion | 1747 | 1747 | 0.23 | 0.7 | 8 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 24 | mean_reversion | 1747 | 1747 | 0.23 | 0.7 | 8 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 24 | continuation | 2740 | 2740 | -0.52 | -1.1 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 48 | mean_reversion | 2321 | 2321 | 0.93 | 3.0 | 5 | review_only | review | `guard_filter_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 48 | mean_reversion | 2321 | 2321 | 0.93 | 3.0 | 5 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 48 | mean_reversion | 2321 | 2321 | 0.93 | 3.0 | 5 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | long | 48 | continuation | 2170 | 2170 | 0.30 | 1.0 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | short | 12 | continuation | 1747 | 1747 | 0.37 | 0.9 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | short | 24 | continuation | 1747 | 1747 | 0.34 | 1.1 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SPREAD_BLOWOUT | BTCUSDT |  | short | 48 | continuation | 2321 | 2321 | 1.20 | 3.9 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 4 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK |  |  | long | 48 | breakout_followthrough | 561 | 561 | 1.24 | 12.1 | 0 | killed_candidate | kill | `trend_cont_02` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | long | 48 | breakout_followthrough | 561 | 561 | 1.24 | 12.1 | 3 | killed_candidate | kill | `trend_cont_02` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | BTCUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SUPPORT_RESISTANCE_BREAK | ETHUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 12 | continuation | 120 | 120 | 1.46 | 11.7 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 12 | mean_reversion | 157 | 157 | 0.02 | 0.1 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 12 | mean_reversion | 157 | 157 | 0.02 | 0.1 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 24 | continuation | 180 | 180 | 1.36 | 16.9 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 24 | mean_reversion | 157 | 157 | 0.92 | 8.8 | 5 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 24 | mean_reversion | 157 | 157 | 0.92 | 8.8 | 5 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 48 | mean_reversion | 157 | 157 | 1.12 | 12.8 | 3 | killed_candidate | kill | `liq_repair_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 48 | mean_reversion | 157 | 157 | 1.12 | 12.8 | 3 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | long | 48 | continuation | 120 | 120 | 0.23 | 3.8 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | short | 12 | continuation | 157 | 157 | 0.06 | 0.4 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | short | 24 | continuation | 157 | 157 | 0.97 | 9.3 | 2 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | SWEEP_STOPRUN | BTCUSDT |  | short | 48 | continuation | 157 | 157 | 1.21 | 13.8 | 1 | review_only | review | `liq_stress_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | long | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | long | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | long | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | short | 12 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | short | 24 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 2 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | BTCUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_ACCELERATION | ETHUSDT |  | short | 48 | breakout_followthrough | 0 | 0 | 0.00 | 0.0 | 1 | review_only | review | `trend_cont_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 12 | continuation | 88 | 88 | -0.02 | -0.2 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 12 | false_breakout_reversal | 88 | 88 | -0.02 | -0.2 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 24 | continuation | 1158 | 1158 | 0.32 | 0.9 | 2 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 24 | false_breakout_reversal | 1158 | 1158 | 0.32 | 0.9 | 2 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 48 | continuation | 1158 | 1158 | 0.40 | 1.5 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | long | 48 | false_breakout_reversal | 1158 | 1158 | 0.40 | 1.5 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 12 | continuation | 740 | 740 | 0.21 | 0.5 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 12 | false_breakout_reversal | 740 | 740 | 0.21 | 0.5 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 24 | continuation | 88 | 88 | 0.50 | 6.6 | 2 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 24 | false_breakout_reversal | 88 | 88 | 0.50 | 6.6 | 2 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 48 | continuation | 88 | 88 | 1.06 | 19.2 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_DECELERATION | BTCUSDT |  | short | 48 | false_breakout_reversal | 88 | 88 | 1.06 | 19.2 | 1 | killed_candidate | kill | `trend_fail_residual_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 12 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 12 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 3 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | long | 12 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 1 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 24 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 24 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 5 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | long | 24 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 2 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 48 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | long | 48 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 3 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | long | 48 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 1 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 12 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 12 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 3 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | short | 12 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 1 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 24 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 24 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 5 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | short | 24 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 2 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 48 | exhaustion_reversal | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `liq_cells_smoke_01` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | BTCUSDT |  | short | 48 | exhaustion_reversal | 24 | 24 | 0.00 | 0.0 | 3 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_EXHAUSTION_TRIGGER | ETHUSDT |  | short | 48 | exhaustion_reversal | 21 | 21 | 0.00 | 0.0 | 1 | review_only | review | `liq_exhaustion_postfix_20260429` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 12 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 24 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | long | 48 | mean_reversion | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | short | 12 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | short | 24 | continuation | 2 | 2 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | TREND_TO_CHOP_SHIFT | BTCUSDT |  | short | 48 | continuation | 2 | 2 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 12 | mean_reversion | 4350 | 4350 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 12 | volatility_expansion_follow | 4350 | 4350 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 12 | continuation | 4350 | 4350 | -0.55 | -1.3 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 24 | continuation | 4350 | 4350 | 0.66 | 1.9 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 24 | mean_reversion | 4350 | 4350 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 24 | volatility_expansion_follow | 4350 | 4350 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 48 | continuation | 4349 | 4349 | 0.82 | 3.1 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 48 | mean_reversion | 4349 | 4349 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | long | 48 | volatility_expansion_follow | 4349 | 4349 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 12 | volatility_expansion_follow | 4350 | 4350 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 12 | continuation | 4350 | 4350 | -1.18 | -2.7 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 24 | volatility_expansion_follow | 4350 | 4350 | 0.00 | 0.0 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 24 | continuation | 4350 | 4350 | -2.03 | -5.9 | 5 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 48 | volatility_expansion_follow | 4349 | 4349 | 0.00 | 0.0 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_CLUSTER_SHIFT | ETHUSDT |  | short | 48 | continuation | 4349 | 4349 | -1.85 | -7.1 | 3 | killed_candidate | kill | `vol_transition_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 12 | mean_reversion | 2949 | 2949 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 12 | continuation | 424 | 424 | -0.52 | -1.9 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 24 | mean_reversion | 2949 | 2949 | 0.00 | 0.0 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 24 | continuation | 424 | 424 | -0.89 | -3.9 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 48 | continuation | 423 | 423 | 0.44 | 2.8 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | long | 48 | mean_reversion | 2948 | 2948 | 0.00 | 0.0 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | short | 12 | continuation | 975 | 975 | 0.77 | 2.6 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | short | 24 | continuation | 975 | 975 | 1.04 | 4.6 | 2 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_REGIME_SHIFT_EVENT | BTCUSDT |  | short | 48 | continuation | 1502 | 1502 | 0.58 | 2.8 | 1 | killed_candidate | kill | `regime_trans_01` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 12 | continuation | 379 | 379 | 1.35 | 11.4 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 12 | continuation | 292 | 292 | 0.42 | 7.1 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 12 | mean_reversion | 958 | 958 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 12 | mean_reversion | 917 | 917 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 24 | continuation | 379 | 379 | 1.65 | 18.5 | 2 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 24 | continuation | 373 | 373 | 1.22 | 15.7 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 24 | mean_reversion | 958 | 958 | 0.00 | 0.0 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 24 | mean_reversion | 917 | 917 | 0.00 | 0.0 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 48 | continuation | 379 | 379 | 0.82 | 12.2 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 48 | continuation | 259 | 259 | 0.54 | 8.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | long | 48 | mean_reversion | 958 | 958 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | long | 48 | mean_reversion | 917 | 917 | 0.00 | 0.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | short | 12 | continuation | 295 | 295 | 1.51 | 14.0 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | short | 12 | continuation | 259 | 259 | 0.84 | 10.8 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | short | 24 | continuation | 295 | 295 | 1.62 | 23.1 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | short | 24 | continuation | 292 | 292 | 0.76 | 13.2 | 2 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | BTCUSDT |  | short | 48 | continuation | 182 | 182 | 2.18 | 50.7 | 1 | candidate_signal | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | VOL_SHOCK | ETHUSDT |  | short | 48 | continuation | 292 | 292 | 0.77 | 21.8 | 1 | review_only | review | `governed_sweep_20260429_v1` |
| pre_mechanism |  | False | WICK_REVERSAL_PROXY | BTCUSDT |  | long | 12 | mean_reversion | 333 | 333 | 0.46 | 2.2 | 1 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | WICK_REVERSAL_PROXY | BTCUSDT |  | long | 24 | mean_reversion | 333 | 333 | 1.12 | 6.6 | 2 | review_only | review | `liq_proxy_01` |
| pre_mechanism |  | False | WICK_REVERSAL_PROXY | BTCUSDT |  | long | 48 | mean_reversion | 333 | 333 | 2.12 | 15.9 | 1 | candidate_signal | review | `liq_proxy_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 12 | mean_reversion | 3588 | 3588 | 4.93 | 20.3 | 7 | candidate_signal | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH |  |  | long | 12 | mean_reversion | 738 | 738 | 1.49 | 9.2 | 1 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T101211Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 12 | mean_reversion | 738 | 738 | 1.49 | 9.2 | 7 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T101211Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH |  |  | long | 12 | mean_reversion | 1108 | 1108 | 1.00 | 4.7 | 1 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T100734Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 12 | mean_reversion | 1108 | 1108 | 1.00 | 4.7 | 7 | killed_candidate | kill | `edge_cell_stat_stretch_e_20260428T100734Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 12 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 7 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 12 | mean_reversion | 12796 | 12796 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 12 | mean_reversion | 12796 | 12796 | 0.00 | 0.0 | 5 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 24 | mean_reversion | 3586 | 3586 | 1.79 | 8.2 | 9 | candidate_signal | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 10 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 24 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 9 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 24 | mean_reversion | 12792 | 12792 | 0.00 | 0.0 | 10 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 24 | mean_reversion | 12792 | 12792 | 0.00 | 0.0 | 10 | review_only | review | `stat_stretch_03` |
| pre_mechanism |  | False | ZSCORE_STRETCH |  |  | long | 48 | mean_reversion | 1038 | 1038 | 1.73 | 11.8 | 0 | killed_candidate | kill | `edge_cell_stat_stretch_0_20260428T091734Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 48 | mean_reversion | 1038 | 1038 | 1.73 | 11.8 | 7 | killed_candidate | kill | `edge_cell_stat_stretch_0_20260428T091734Z_...` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 48 | mean_reversion | 2670 | 2670 | 1.58 | 10.5 | 3 | review_only | review | `stat_stretch_eth_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 7 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | ETHUSDT |  | long | 48 | mean_reversion | 0 | 0 | 0.00 | 0.0 | 3 | review_only | review | `stat_stretch_01` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 48 | mean_reversion | 12789 | 12789 | 0.00 | 0.0 | 7 | review_only | review | `stat_stretch_02` |
| pre_mechanism |  | False | ZSCORE_STRETCH | BTCUSDT |  | long | 48 | mean_reversion | 12789 | 12789 | 0.00 | 0.0 | 7 | review_only | review | `stat_stretch_03` |
