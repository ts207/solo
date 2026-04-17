# Campaign Results — 2026-04-17

Systematic discovery across the full event universe, BTC 2023-2024, rv_pct_17280 > 70 unless noted.
Gates: bridge requires t ≥ 2.0 AND rob ≥ 0.70. Phase2 requires rob ≥ 0.60.

---

## Promoted Theses

| Event | Direction | Horizon | Template | t | rob | q | exp (bps) | run_id |
|-------|-----------|---------|----------|---|-----|---|-----------|--------|
| VOL_SPIKE | long | 24b | mean_reversion | **3.59** | 0.62 | 0.0002 | 21.8 | `broad_vol_spike_20260416T210045Z_68e0020707` |
| OI_SPIKE_NEGATIVE | long | 48b | exhaustion_reversal | **2.37** | 0.87 | 0.009 | 68.7 | `campaign_pe_oi_spike_neg_20260416T200016Z_3a1e1e23a1` |
| OI_SPIKE_NEGATIVE | long | 24b | exhaustion_reversal | 2.28 | 0.85 | 0.011 | 51.5 | `campaign_pe_oi_spike_neg_20260416T092104Z_f6e6885923` |
| LIQUIDATION_CASCADE | long | 24b | exhaustion_reversal | 1.78 | 0.82 | 0.037 | 23.8 | `liquidation_std_gate_2yr_20260416T090207Z_84e1c40190` |

*OI_SPIKE_NEGATIVE: 48b is the stronger result; 24b was the paper-running thesis. Both promoted.*

---

## Below Bridge Gate — Discoveries with Real Signal

### CLIMAX_VOLUME_BAR — Horizon Sweep (exhaustion_reversal, rv>70, BTC 2023-2024)

| Horizon | t | rob | n | q | exp (bps) | notes |
|---------|---|-----|---|---|-----------|-------|
| 12b | 2.09 | 0.52 | 123 | 0.018 | 38.7 | t passes, rob below phase2 |
| **24b** | **1.95** | **0.79** | **123** | **0.026** | **36.8** | best balance; structural ceiling |
| 24b rv>75 | 1.92 | 0.82 | 107 | 0.028 | 40.6 | tighter filter doesn't help t |
| 24b rv>80 | 1.73 | 0.55 | 97 | 0.042 | 39.0 | too tight |
| 24b 3yr | 1.17 | 0.63 | 213 | 0.120 | 22.5 | 2022 dilutes signal |
| 48b | 1.64 | 0.84 | 122 | 0.051 | 35.5 | original broad sweep |
| 96b | 0.34 | 0.51 | 122 | 1.000 | 8.6 | signal gone |

**Ceiling:** t≈1.95 at 24b. No tuning path to bridge gate.

### POST_DELEVERAGING_REBOUND — Horizon Sweep (exhaustion_reversal, rv>70, BTC 2023-2024)

| Horizon | t | rob | n | q | exp (bps) |
|---------|---|-----|---|---|-----------|
| 12b | 1.14 | 0.51 | 302 | 0.127 | 2.8 |
| 24b | 0.62 | 0.58 | 301 | 1.000 | 1.4 |
| **48b** | **1.95** | **0.68** | **301** | **0.025** | **14.6** | peak |

*Note: POST_DELEVERAGING_REBOUND and LIQUIDATION_EXHAUSTION_REVERSAL are aliases — identical 689 events, identical stats.*

### OI_SPIKE_POSITIVE — Horizon Sweep + Confirmation (exhaustion_reversal, rv>70, BTC 2023-2024)

| Config | Horizon | t | rob | n | q | exp (bps) |
|--------|---------|---|-----|---|---|-----------|
| 12b | 12b | 0 events | — | — | — | — |
| 24b | 24b | 0.53 | 0.42 | 63 | 1.000 | 10.2 |
| **48b** | **48b** | **1.65** | **0.65** | **63** | **0.050** | **44.1** | peak |
| H2-2024 confirm | 48b | 1.62 | 0.65 | 62 | 0.053 | 43.8 | signal live in H2-2024 |

### FORCED_FLOW_EXHAUSTION

| Config | Horizon | t | rob | n | q | exp (bps) |
|--------|---------|---|-----|---|---|-----------|
| broad sweep | 48b | 1.40 | 0.60 | 72 | 0.081 | 30.4 |

---

## VOL_SPIKE — Full Template and Horizon Sweep

*Correct template family: mean_reversion, continuation. exhaustion_reversal = incompatible (0 hypotheses generated).*

### Long direction — mean_reversion

| Horizon | rv filter | t | rob | n | q | exp (bps) |
|---------|-----------|---|-----|---|---|-----------|
| 12b | rv>70 | 2.95 | 0.59 | 682 | 0.0016 | 11.6 |
| 12b | rv>75 | 2.31 | 0.55 | 626 | 0.0103 | 9.3 |
| **24b** | **rv>70** | **3.59** | **0.62** | **681** | **0.0002** | **21.8** | **promoted** |
| 24b | rv>75 | 2.53 | 0.53 | 625 | 0.0057 | 16.1 |
| 24b H2-2024 | rv>70 | 3.24 | 0.47 | 672 | 0.0006 | 20.6 | confirmed live |
| 48b | rv>70 | 0.87 | 0.43 | 678 | 1.000 | 5.3 | signal gone |

### Short direction — continuation

| Horizon | t | rob | n | q | exp (bps) |
|---------|---|-----|---|---|-----------|
| 12b | −1.08 (long equiv) | 0.60 | 678 | — | −12.2 | price bounces up at 12b |
| 24b | 0.35 | 0.61 | 681 | 1.000 | 0.5 | flat |
| 48b | 1.08 | 0.60 | 678 | 0.141 | 8.4 | weak long-tail |

*Short 12b is negative (price bounces up immediately after spike). Delayed momentum at 48b but below gate.*

---

## VOL_SHOCK — Template Sweep

*VolShockRelaxationDetector fires during relaxation phase — no directional edge at any template/horizon.*

| Template | Horizon | t | rob | n |
|----------|---------|---|-----|---|
| mean_reversion | 12b | 0.62 | 0.41 | 318 |
| mean_reversion | 24b | −0.62 | 0.38 | 318 |
| continuation | 24b | 0.62 | 0.41 | 318 |

---

## VOLATILITY_TRANSITION Batch — Re-tested with mean_reversion

*All previously tested with exhaustion_reversal (incompatible — 0 hypotheses). Re-run with mean_reversion 24b, rv>70.*

| Event | t | rob | n | q | verdict |
|-------|---|-----|---|---|---------|
| BREAKOUT_TRIGGER | 0.79 | 0.46 | 20 | 1.00 | too few events under rv>70 |
| RANGE_COMPRESSION_END | — | — | — | 0.48 | negative exp (−7.4 bps) |
| VOL_CLUSTER_SHIFT | — | — | — | 0.95 | negative exp (−10.7 bps) |
| VOL_REGIME_SHIFT_EVENT | — | — | — | 1.00 | 0 qualifying events |
| VOL_RELAXATION_START | — | — | — | 1.00 | 0 qualifying events |
| BETA_SPIKE_EVENT | — | — | — | 1.00 | 0 qualifying events |

---

## Broad Sweep — One Pass (exhaustion_reversal, rv>70, 48b, BTC 2023-2024)

*Events tested simultaneously in the initial broad discovery sweep.*

| Event | Family | t | rob | q | exp (bps) | verdict |
|-------|--------|---|-----|---|-----------|---------|
| OI_SPIKE_POSITIVE | POSITIONING_EXTREMES | 1.65 | 0.65 | 0.050 | 44.1 | discovery |
| POST_DELEVERAGING_REBOUND | POSITIONING_EXTREMES | 1.95 | 0.68 | 0.025 | 14.6 | discovery |
| LIQ_EXHAUSTION_REVERSAL | POSITIONING_EXTREMES | 1.95 | 0.68 | 0.025 | 14.6 | discovery (= PDR alias) |
| CLIMAX_VOLUME_BAR | FORCED_FLOW_AND_EXHAUSTION | 1.64 | 0.84 | 0.051 | 35.5 | discovery |
| FORCED_FLOW_EXHAUSTION | FORCED_FLOW_AND_EXHAUSTION | 1.40 | 0.60 | 0.081 | 30.4 | discovery |
| TREND_EXHAUSTION_TRIGGER | FORCED_FLOW_AND_EXHAUSTION | — | — | — | 0 | no signal |
| VOL_SHOCK | VOLATILITY_TRANSITION | — | — | — | — | no signal (wrong template) |
| VOL_SPIKE | VOLATILITY_TRANSITION | — | — | — | — | dropped (wrong template) |
| FAILED_CONTINUATION | TREND_STRUCTURE | −0.81 | 0.51 | — | −13.8 | wrong direction (long) |

---

## Events with No Signal — Full Catalog

| Event | Best result | Reason |
|-------|-------------|--------|
| DELEVERAGING_WAVE | t=1.02, q=1.0 | below gate |
| OI_FLUSH | t=1.27, rob=0.79, q=0.10 | passes rob, fails t and FDR |
| OI_SPIKE_NEGATIVE (ETH) | t=0.94, rob=0.27 | no signal on ETH |
| VOL_SHOCK | t=0.62 | relaxation detector — no predictive timing |
| TREND_EXHAUSTION_TRIGGER | 0 events | no qualifying events under rv>70 |
| FAILED_CONTINUATION (long) | t=−0.81 | wrong direction |
| FAILED_CONTINUATION (short) | t≈+0.81 | real but below gate |
| BREAKOUT_TRIGGER | t=0.79, n=20 | too few events under rv>70 |
| RANGE_COMPRESSION_END | neg exp | negative signal |
| VOL_CLUSTER_SHIFT | neg exp | negative signal |
| VOL_REGIME_SHIFT_EVENT | 0 events | no qualifying events |
| VOL_RELAXATION_START | 0 events | no qualifying events |
| BETA_SPIKE_EVENT | 0 events | no qualifying events |

*Events not tested (require data not in lake): BASIS_DISLOCATION, SPOT_PERP_BASIS_SHOCK (need close_spot), LIQUIDATION_CASCADE (ETH — needs Binance liquidation ingest).*

---

## Template Compatibility Reference

| Event family | compatible templates | incompatible |
|---|---|---|
| VOLATILITY_EXPANSION / TRANSITION | `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout`, `trend_continuation`, `pullback_entry` | `exhaustion_reversal` |
| TREND_FAILURE_EXHAUSTION | `exhaustion_reversal` | `continuation`, `trend_continuation` |
| FORCED_FLOW_AND_EXHAUSTION | `exhaustion_reversal` | `continuation`, `trend_continuation` |
| TREND_STRUCTURE | `exhaustion_reversal`, `mean_reversion`, `impulse_continuation` | `continuation`, `trend_continuation`, `pullback_entry` |

*Check `estimated_hypothesis_count` in `validated_plan.json` after any proposal run. If 0, the template is incompatible.*

---

## Key Lake Run IDs (cached data for re-use)

| Scope | run_id | coverage |
|-------|--------|----------|
| BTC 2023-2024 (FORCED_FLOW events) | `broad_climax_volume_bar_20260416T202235Z_9787da0dd4` | BTC 5m 2023-2024 |
| BTC 2023-2024 (POSITIONING_EXTREMES) | `broad_oi_spike_positive_20260416T201712Z_2c9510827b` | BTC 5m 2023-2024 |
| BTC 2023-2024 (VOLATILITY_TRANSITION) | `broad_vol_spike_20260416T210045Z_68e0020707` | BTC 5m 2023-2024 |
| BTC 2023-2024 (VOLATILITY_TRANSITION) | `broad_vol_shock_20260416T202825Z_c5cd86c72e` | BTC 5m 2023-2024 |
| BTC 2022-2024 (3yr) | `liquidation_std_gate_3yr_20260416T090827Z_91dd43e2f6` | BTC 5m 2022-2024 |
