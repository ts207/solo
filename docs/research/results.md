# All Results — Edge Discovery Project

*Auto-generated. Do not edit manually — rerun `project/scripts/update_results_index.py`.*
*139 unique results across 20 events.*
*Gates: bridge = t ≥ 2.0 AND rob ≥ 0.70; phase2 = rob ≥ 0.60. exp = after-cost per trade (bps).*

---

## Summary — Best Result Per Event

| Event | Dir | Horizon | Template | t | rob | q | exp (bps) | Status |
|-------|-----|---------|----------|---|-----|---|-----------|--------|
| BETA_SPIKE_EVENT |  | 24b | mean_reversion | — | — | — | 0.0 | no events |
| BREAKOUT_TRIGGER | long | 24b | mean_reversion | 0.79 | 0.460 | 1.0000 | 22.1 | below gate |
| CLIMAX_VOLUME_BAR | long | 12b | exhaustion_reversal | 2.09 | 0.521 | 0.0183 | 38.7 | t passes |
| DELEVERAGING_WAVE | long | 24b | exhaustion_reversal | 1.02 | 0.667 | 1.0000 | 12.7 | below gate |
| FAILED_CONTINUATION |  | 48b | unknown | — | — | — | -138006.0 | no events |
| FORCED_FLOW_EXHAUSTION | long | 48b | exhaustion_reversal | 1.40 | 0.599 | 0.0807 | 30.4 | below gate |
| LIQUIDATION_CASCADE | long | 8b | mean_reversion | 3.07 | 0.611 | 0.0011 | 27.1 | phase2 gate |
| LIQUIDATION_CASCADE_PROXY | long | 16b | mean_reversion | 2.64 | 0.608 | 0.0042 | 57.3 | phase2 gate |
| LIQUIDATION_EXHAUSTION_REVERSAL | long | 48b | exhaustion_reversal | 1.95 | 0.677 | 0.0254 | 14.6 | discovery |
| OI_FLUSH | long | 24b | exhaustion_reversal | 1.27 | 0.793 | 0.1026 | 12.3 | below gate |
| OI_SPIKE_NEGATIVE |  | 24b | unknown | — | — | — | 51.5 | **PROMOTED** |
| OI_SPIKE_POSITIVE | long | 48b | exhaustion_reversal | 1.65 | 0.646 | 0.0499 | 44.1 | discovery |
| POST_DELEVERAGING_REBOUND | long | 48b | exhaustion_reversal | 1.95 | 0.677 | 0.0254 | 14.6 | discovery |
| RANGE_COMPRESSION_END |  | 24b | mean_reversion | — | — | — | -7407.0 | no events |
| TREND_EXHAUSTION_TRIGGER |  | 48b | unknown | — | — | — | 0.0 | no events |
| VOL_CLUSTER_SHIFT |  | 24b | mean_reversion | — | — | — | -106826.0 | no events |
| VOL_REGIME_SHIFT_EVENT |  | 24b | mean_reversion | — | — | — | 0.0 | no events |
| VOL_RELAXATION_START |  | 24b | mean_reversion | — | — | — | 0.0 | no events |
| VOL_SHOCK | long | 24b | continuation | 0.62 | 0.414 | 1.0000 | 4.7 | below gate |
| VOL_SPIKE | long | 24b | mean_reversion | 3.59 | 0.616 | 0.0002 | 21.8 | **PROMOTED** |

---

## BETA_SPIKE_EVENT

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 24b | mean_reversion | — | — | 0 | — | 0.0 | no events | `beta_spike_event_mr_24b` |

## BREAKOUT_TRIGGER

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | mean_reversion | 0.79 | 0.460 | 20 | 1.0000 | 22.1 | below gate | `breakout_trigger_mr_24b` |
|  | 24b | mean_reversion | — | — | 0 | — | 22.1 | no events | `breakout_trigger_mr_24b` |

## CLIMAX_VOLUME_BAR

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 12b | exhaustion_reversal | 2.09 | 0.521 | 123 | 0.0183 | 38.7 | t passes | `climax_volume_bar_12b` |
| long | 24b | exhaustion_reversal | 1.95 | 0.793 | 123 | 0.0256 | 36.8 | discovery | `climax_volume_bar_24b` |
| long | 24b | exhaustion_reversal | 1.92 | 0.818 | 107 | 0.0277 | 40.6 | discovery | `climax_volume_bar_24b_rv75` |
| long | 24b | exhaustion_reversal | 1.73 | 0.553 | 97 | 0.0417 | 39.0 | discovery | `climax_volume_bar_24b_rv80` |
| long | 48b | exhaustion_reversal | 1.64 | 0.844 | 122 | 0.0505 | 35.5 | below gate | `climax_volume_bar` |
| long | 24b | exhaustion_reversal | 1.17 | 0.633 | 213 | 0.1202 | 22.5 | below gate | `climax_volume_bar_24b_2021` |
| long | 96b | exhaustion_reversal | 0.34 | 0.508 | 122 | 1.0000 | 8.6 | below gate | `climax_volume_bar_96b` |
|  | 12b | exhaustion_reversal | — | — | 0 | — | 38.7 | no events | `climax_volume_bar_12b` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 39.0 | no events | `climax_volume_bar_24b_rv80` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 35.5 | no events | `climax_volume_bar` |
|  | 96b | exhaustion_reversal | — | — | 0 | — | 8.6 | no events | `climax_volume_bar_96b` |

## DELEVERAGING_WAVE

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | exhaustion_reversal | 1.02 | 0.667 | 130 | 1.0000 | 12.7 | below gate | `deleveraging-wave` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 12.7 | no events | `deleveraging-wave` |

## FAILED_CONTINUATION

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 48b | unknown | — | — | 0 | — | -138006.0 | no events | `failed_continuation` |

## FORCED_FLOW_EXHAUSTION

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 48b | exhaustion_reversal | 1.40 | 0.599 | 72 | 0.0807 | 30.4 | below gate | `forced_flow_exhaustion` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 30.4 | no events | `forced_flow_exhaustion` |

## LIQUIDATION_CASCADE

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 8b | mean_reversion | 3.07 | 0.611 | 101 | 0.0011 | 27.1 | phase2 gate | `h8` |
| long | 8b | reversal_or_squeeze | 3.00 | 0.624 | 371 | 0.0027 | 12.9 | phase2 gate | `direct_edge` |
| short | 8b | mean_reversion | 3.00 | 0.118 | 371 | 0.0013 | 12.9 | t passes | `direct_edge` |
| long | 8b | mean_reversion | 3.00 | 0.624 | 371 | 0.0013 | 12.9 | phase2 gate | `direct_edge` |
| long | 8b | continuation | 3.00 | 0.624 | 371 | 0.0027 | 12.9 | phase2 gate | `direct_edge` |
| long | 8b | exhaustion_reversal | 3.00 | 0.624 | 371 | 0.0027 | 12.9 | phase2 gate | `direct_edge` |
| short | 8b | mean_reversion | 2.76 | 0.092 | 114 | 0.0029 | 25.9 | t passes | `direct_edge` |
| long | 8b | mean_reversion | 2.76 | 0.682 | 114 | 0.0029 | 25.9 | phase2 gate | `direct_edge` |
| long | 8b | reversal_or_squeeze | 2.76 | 0.682 | 114 | 0.0057 | 25.9 | phase2 gate | `direct_edge` |
| long | 8b | continuation | 2.76 | 0.682 | 114 | 0.0057 | 25.9 | phase2 gate | `direct_edge` |
| long | 8b | exhaustion_reversal | 2.76 | 0.682 | 114 | 0.0057 | 25.9 | phase2 gate | `direct_edge` |
| long | 10b | mean_reversion | 2.61 | 0.560 | 178 | 0.0046 | 18.0 | t passes | `h10` |
| long | 8b | continuation | 2.40 | 0.836 | 65 | 0.0163 | 27.1 | bridge gate | `direct_edge` |
| long | 8b | mean_reversion | 2.40 | 0.836 | 65 | 0.0082 | 27.1 | bridge gate | `direct_edge` |
| short | 8b | mean_reversion | 2.40 | 0.013 | 65 | 0.0082 | 27.1 | t passes | `direct_edge` |
| long | 8b | exhaustion_reversal | 2.40 | 0.836 | 65 | 0.0163 | 27.1 | bridge gate | `direct_edge` |
| long | 8b | reversal_or_squeeze | 2.40 | 0.836 | 65 | 0.0163 | 27.1 | bridge gate | `direct_edge` |
| long | 8b | reversal_or_squeeze | 2.39 | 0.630 | 89 | 0.0168 | 27.8 | phase2 gate | `direct_edge` |
| long | 8b | mean_reversion | 2.39 | 0.630 | 89 | 0.0084 | 27.8 | phase2 gate | `direct_edge` |
| long | 8b | continuation | 2.39 | 0.630 | 89 | 0.0168 | 27.8 | phase2 gate | `direct_edge` |
| long | 8b | exhaustion_reversal | 2.39 | 0.630 | 89 | 0.0168 | 27.8 | phase2 gate | `direct_edge` |
| short | 8b | mean_reversion | 2.39 | 0.173 | 89 | 0.0084 | 27.8 | t passes | `direct_edge` |
| long | 16b | mean_reversion | 2.35 | 0.830 | 110 | 0.0094 | 25.6 | bridge gate | `h16_confirmatory` |
| long | 16b | exhaustion_reversal | 2.35 | 0.830 | 110 | 0.0094 | 25.6 | bridge gate | `liqh16_20210413_a` |
| long | 3b | exhaustion_reversal | 2.30 | 0.715 | 101 | 0.0108 | 12.2 | bridge gate | `liqconfirm_20210413_a` |
| long | 3b | mean_reversion | 2.30 | 0.715 | 101 | 0.0108 | 12.2 | bridge gate | `confirmatory` |
| long | 24b | exhaustion_reversal | 1.78 | 0.818 | 94 | 0.0372 | 23.8 | discovery | `std_gate_3yr` |
| long | 12b | mean_reversion | 1.63 | 0.562 | 53 | 0.0519 | 13.8 | below gate | `productive_golden_path` |
| long | 12b | exhaustion_reversal | 1.54 | 0.529 | 94 | 0.0612 | 21.9 | below gate | `std_gate_12b` |
| long | 24b | exhaustion_reversal | 1.50 | 0.826 | 57 | 0.0667 | 25.6 | below gate | `std_gate_regime` |
| long | 24b | exhaustion_reversal | 1.26 | 0.512 | 91 | 0.1035 | 12.8 | below gate | `std_gate_regime` |
| long | 24b | mean_reversion | 1.26 | 0.512 | 91 | 0.1035 | 12.8 | below gate | `standard_gate_v2_h24` |
| long | 24b | exhaustion_reversal | 1.09 | 0.494 | 35 | 0.1375 | 24.5 | below gate | `std_gate_regime_confirm` |
| long | 12b | exhaustion_reversal | 1.01 | 0.525 | 91 | 1.0000 | 7.8 | below gate | `standard_gat070638Z_2438ebe1b3` |
| long | 12b | mean_reversion | 1.01 | 0.525 | 91 | 0.1552 | 7.8 | below gate | `productive_golden_path` |
|  | 10b | exhaustion_reversal | — | — | 0 | — | 18.0 | no events | `h10` |
|  | 12b | exhaustion_reversal | — | — | 0 | — | 121020.8 | no events | `direct_funding_pos` |
|  | 12b | mean_reversion | — | — | 0 | — | 8.4 | no events | `productive_golden_path` |
|  | 16b | exhaustion_reversal | — | — | 0 | — | 25.6 | no events | `h16` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 5878.0 | no events | `std_gate_full2024` |
|  | 3b | exhaustion_reversal | — | — | 0 | — | 12.2 | no events | `confirmatory` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | -3950.0 | no events | `std_gate_48b` |
|  | 8b | exhaustion_reversal | — | — | 0 | — | 23.4 | no events | `direct_edge` |

## LIQUIDATION_CASCADE_PROXY

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 16b | mean_reversion | 2.64 | 0.608 | 77 | 0.0042 | 57.3 | phase2 gate | `proxy_edge` |
| long | 16b | reversal_or_squeeze | 2.64 | 0.608 | 77 | 0.0083 | 57.3 | phase2 gate | `proxy_edge` |
| long | 16b | continuation | 2.64 | 0.608 | 77 | 0.0083 | 57.3 | phase2 gate | `proxy_edge` |
| long | 16b | exhaustion_reversal | 2.64 | 0.608 | 77 | 0.0083 | 57.3 | phase2 gate | `proxy_edge` |
| short | 16b | mean_reversion | 2.64 | 0.186 | 77 | 0.0042 | 57.3 | t passes | `proxy_edge` |
| short | 1b | exhaustion_reversal | 2.41 | 0.040 | 78 | 0.0159 | 17.0 | t passes | `proxy_edge` |
| short | 1b | reversal_or_squeeze | 2.41 | 0.040 | 78 | 0.0159 | 17.0 | t passes | `proxy_edge` |
| short | 1b | continuation | 2.41 | 0.040 | 78 | 0.0159 | 17.0 | t passes | `proxy_edge` |
| long | 14b | mean_reversion | 2.36 | 0.806 | 77 | 0.0092 | 49.0 | bridge gate | `proxy_edge` |
| long | 14b | exhaustion_reversal | 2.36 | 0.806 | 77 | 0.0183 | 49.0 | bridge gate | `proxy_edge` |
| long | 14b | continuation | 2.36 | 0.806 | 77 | 0.0183 | 49.0 | bridge gate | `proxy_edge` |
| long | 14b | reversal_or_squeeze | 2.36 | 0.806 | 77 | 0.0183 | 49.0 | bridge gate | `proxy_edge` |
| short | 14b | mean_reversion | 2.36 | 0.003 | 77 | 0.0092 | 49.0 | t passes | `proxy_edge` |
| long | 49b | reversal_or_squeeze | 2.07 | 1.000 | 38 | 0.0383 | 166.0 | bridge gate | `proxy_edge` |
| short | 49b | mean_reversion | 2.07 | 0.012 | 38 | 0.0192 | 166.0 | t passes | `proxy_edge` |
| long | 49b | mean_reversion | 2.07 | 1.000 | 38 | 0.0192 | 166.0 | bridge gate | `proxy_edge` |
| long | 49b | continuation | 2.07 | 1.000 | 38 | 0.0383 | 166.0 | bridge gate | `proxy_edge` |
| long | 49b | exhaustion_reversal | 2.07 | 1.000 | 38 | 0.0383 | 166.0 | bridge gate | `proxy_edge` |
| short | 18b | mean_reversion | 2.02 | 0.015 | 36 | 0.0219 | 73.8 | t passes | `proxy_edge` |
| long | 18b | reversal_or_squeeze | 2.02 | 0.934 | 36 | 0.0438 | 73.8 | bridge gate | `proxy_edge` |
| long | 18b | continuation | 2.02 | 0.934 | 36 | 0.0438 | 73.8 | bridge gate | `proxy_edge` |
| long | 18b | exhaustion_reversal | 2.02 | 0.934 | 36 | 0.0438 | 73.8 | bridge gate | `proxy_edge` |
| long | 18b | mean_reversion | 2.02 | 0.934 | 36 | 0.0219 | 73.8 | bridge gate | `proxy_edge` |
|  | 12b | exhaustion_reversal | — | — | 0 | — | 40865.5 | no events | `proxy_edge` |

## LIQUIDATION_EXHAUSTION_REVERSAL

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 48b | exhaustion_reversal | 1.95 | 0.677 | 301 | 0.0254 | 14.6 | discovery | `exhaustion_r` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 14.6 | no events | `exhaustion_r` |

## OI_FLUSH

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | exhaustion_reversal | 1.27 | 0.793 | 90 | 0.1026 | 12.3 | below gate | `oi-flush` |
| long | 24b | exhaustion_reversal | 1.14 | 0.793 | 85 | 0.1278 | 11.1 | below gate | `oi_flush_rv75` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 11.1 | no events | `oi_flush_rv75` |
|  | 24b | unknown | — | — | 0 | — | 12.3 | no events | `oi-flush` |

## OI_SPIKE_NEGATIVE

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | exhaustion_reversal | 2.74 | 0.933 | 16 | 0.0031 | 62.3 | bridge gate | `oi_spike_neg_confirm` |
| long | 48b | exhaustion_reversal | 2.37 | 0.871 | 53 | 0.0088 | 68.7 | **PROMOTED** | `oi_spike_neg_48b` |
| long | 24b | exhaustion_reversal | 2.28 | 0.848 | 53 | 0.0112 | 51.5 | **PROMOTED** | `oi-spike-negative` |
| long | 96b | exhaustion_reversal | 2.09 | 0.520 | 53 | 0.0185 | 105.2 | t passes | `oi_spike_neg_96b` |
| long | 24b | exhaustion_reversal | 0.94 | 0.267 | 92 | 1.0000 | 13.7 | below gate | `oi_spike_neg_eth` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 6.9 | no events | `oi_spike_neg_eth` |
|  | 24b | unknown | — | — | 0 | — | 51.5 | **PROMOTED** | `oi-spike-negative` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 68.7 | **PROMOTED** | `oi_spike_neg_48b` |
|  | 96b | exhaustion_reversal | — | — | 0 | — | 105.2 | no events | `oi_spike_neg_96b` |

## OI_SPIKE_POSITIVE

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 48b | exhaustion_reversal | 1.65 | 0.646 | 63 | 0.0499 | 44.1 | discovery | `oi_spike_positive` |
| long | 48b | exhaustion_reversal | 1.62 | 0.646 | 62 | 0.0529 | 43.8 | below gate | `oi_spike_positive_h2confirm` |
| long | 24b | exhaustion_reversal | 0.53 | 0.419 | 63 | 1.0000 | 10.2 | below gate | `oi_spike_positive_24b` |
|  | 12b | exhaustion_reversal | — | — | 0 | — | -62226.0 | no events | `oi_spike_positive_12b` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 10.2 | no events | `oi_spike_positive_24b` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 44.1 | no events | `oi_spike_positive` |

## POST_DELEVERAGING_REBOUND

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 48b | exhaustion_reversal | 1.95 | 0.677 | 301 | 0.0254 | 14.6 | discovery | `post_deleveraging_reboun` |
| long | 12b | exhaustion_reversal | 1.14 | 0.512 | 302 | 0.1272 | 2.8 | below gate | `post_deleveraging_rebound_12b` |
| long | 24b | exhaustion_reversal | 0.62 | 0.577 | 301 | 1.0000 | 1.4 | below gate | `post_deleveraging_rebound_24b` |
|  | 12b | exhaustion_reversal | — | — | 0 | — | 2.8 | no events | `post_deleveraging_rebound_12b` |
|  | 24b | exhaustion_reversal | — | — | 0 | — | 1.4 | no events | `post_deleveraging_rebound_24b` |
|  | 48b | exhaustion_reversal | — | — | 0 | — | 14.6 | no events | `post_deleveraging_reboun` |

## RANGE_COMPRESSION_END

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 24b | mean_reversion | — | — | 0 | — | -7407.0 | no events | `range_compression_end_mr_24b` |

## TREND_EXHAUSTION_TRIGGER

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 48b | unknown | — | — | 0 | — | 0.0 | no events | `trend_exhaustion_trigger` |

## VOL_CLUSTER_SHIFT

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 24b | mean_reversion | — | — | 0 | — | -106826.0 | no events | `vol_cluster_shift_mr_24b` |

## VOL_REGIME_SHIFT_EVENT

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 24b | mean_reversion | — | — | 0 | — | 0.0 | no events | `vol_regime_shift_event_mr_24b` |

## VOL_RELAXATION_START

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
|  | 24b | mean_reversion | — | — | 0 | — | 0.0 | no events | `vol_relaxation_start_mr_24b` |

## VOL_SHOCK

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | continuation | 0.62 | 0.414 | 318 | 1.0000 | 4.7 | below gate | `vol_shock_long_cont_24b` |
|  | 12b | mean_reversion | — | — | 0 | — | -67863.0 | no events | `productive_golden_path` |
|  | 12b | unknown | — | — | 0 | — | -77593.0 | no events | `standard_gate_vol_shock` |
|  | 24b | continuation | — | — | 0 | — | 4.7 | no events | `vol_shock_long_cont_24b` |
|  | 24b | mean_reversion | — | — | 0 | — | -87209.0 | no events | `vol_shock_long_mr_24b` |

## VOL_SPIKE

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | mean_reversion | 3.59 | 0.616 | 681 | 0.0002 | 21.8 | **PROMOTED** | `vol_spike_long_mr_24b` |
| long | 24b | unknown | 3.59 | 0.616 | 681 | 0.0002 | 21.8 | phase2 gate | `vol_spike210045Z_68e0020707` |
| long | 24b | mean_reversion | 3.24 | 0.466 | 672 | 0.0006 | 20.6 | t passes | `vol_spike_long_mr_24b_h2confirm` |
| long | 12b | mean_reversion | 2.95 | 0.586 | 682 | 0.0016 | 11.6 | t passes | `vol_spike_long_mr_12b` |
| long | 24b | mean_reversion | 2.53 | 0.529 | 625 | 0.0057 | 16.1 | t passes | `vol_spike_long_mr_24b_rv75` |
| long | 12b | mean_reversion | 2.31 | 0.547 | 626 | 0.0103 | 9.3 | t passes | `vol_spike_long_mr_12b_rv75` |
| short | 48b | continuation | 1.08 | 0.598 | 678 | 0.1407 | 8.4 | below gate | `vol_spike_short` |
| long | 48b | mean_reversion | 0.87 | 0.428 | 678 | 1.0000 | 5.3 | below gate | `vol_spike_long_mr_48b` |
| short | 24b | continuation | 0.35 | 0.605 | 681 | 1.0000 | 0.5 | below gate | `vol_spike_short_24b` |
|  | 12b | continuation | — | — | 0 | — | -14669.0 | no events | `vol_spike_short_12b` |
|  | 12b | mean_reversion | — | — | 0 | — | 9.3 | no events | `vol_spike_long_mr_12b_rv75` |
|  | 24b | continuation | — | — | 0 | — | 0.5 | no events | `vol_spike_short_24b` |
|  | 24b | mean_reversion | — | — | 0 | — | 16.1 | no events | `vol_spike_long_mr_24b_rv75` |
|  | 48b | continuation | — | — | 0 | — | 8.4 | no events | `vol_spike_short` |
|  | 48b | mean_reversion | — | — | 0 | — | 5.3 | no events | `vol_spike_long_mr_48b` |
|  | 48b | unknown | — | — | 0 | — | -123691.0 | no events | `vol_spike` |
