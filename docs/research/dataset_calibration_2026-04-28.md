# Dataset Calibration Readout - 2026-04-28

Scope: local `data/reports/phase2/**/evaluated_hypotheses.parquet`, with validation and promotion artifacts spot-checked for promoted/live confusion.

## Evidence Base

- Raw evaluated phase-2 rows: 2,102.
- Artifact runs: 48.
- Symbols present: BTCUSDT, ETHUSDT.
- Unique calibration rows after removing repeated benchmark duplicates: 1,577.
- Search-pass rows, using `t_stat_net >= 2.0`, `robustness_score >= 0.50`, `mean_return_net_bps >= 0`, `n >= 30`: 17.
- Deploy-metric-pass rows, using `t_stat_net >= 2.0`, `robustness_score >= 0.70`, `mean_return_net_bps >= 0`, `n >= 30`: 6.

Interpretation: the dataset has enough signal for prioritizing next templates and contexts, but not enough to justify broad registry or routing changes. Most value is concentrated in a few event/context/template cells.

## Strongest Cells

| Priority | Cell | Symbol | Run | Horizon | n | t_net | net bps | robust | p_fdr | Read |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---|
| 1 | `CLIMAX_VOLUME_BAR` + `exhaustion_reversal` + `CARRY_STATE=FUNDING_NEG` | BTCUSDT | `trend_fail_v1_01` | 24b | 472 | 3.8312 | 23.2171 | 0.7169 | 0.00006377 | Best governed follow-up lane. Clears deploy-like metric screen in local phase-2 evidence. |
| 2 | `BAND_BREAK` + `mean_reversion` + `VOL_REGIME=LOW` | ETHUSDT | `stat_stretch_eth_01` | 24b | 1,938 | 3.2765 | 11.0064 | 0.7029 | 0.00052548 | Best ETH calibration lane. Clears deploy-like metric screen with large sample. |
| 3 | `PRICE_DOWN_OI_DOWN` + `mean_reversion` + `VOL_REGIME=HIGH` | BTCUSDT | `supported_path_20260427T001920Z_price_down_oi_down` | 24b | 79 | 2.3456 | 41.9707 | 0.8387 | 0.00949829 | Strong but sample-thin and benchmark-repeated; useful as control, not as a new discovery frontier. |
| 4 | `FALSE_BREAKOUT` + `exhaustion_reversal` + `MS_TREND_STATE=BULLISH` | BTCUSDT | `trend_fail_v1_2021` | 48b | 293 | 2.0898 | 30.4336 | 0.7045 | 0.01832003 | Real candidate, but weaker than funding-negative climax volume. |
| 5 | `ZSCORE_STRETCH` + `mean_reversion` + `MS_TREND_STATE=CHOP` | ETHUSDT | `stat_stretch_eth_01` | 12b | 4,039 | 2.2981 | 8.8900 | 0.5022 | 0.01077877 | Search-pass only. High n; robustness too low for deployment. |
| 6 | `LIQUIDITY_GAP_PRINT` + `continuation` + `MS_TREND_STATE=BULLISH` | BTCUSDT | `liq_stress_01` | 48b | 1,518 | 2.3692 | 8.8192 | 0.5061 | 0.00891297 | Search-pass only. Treat as broad exploratory lane, not promotion lane. |
| 7 | `OVERSHOOT_AFTER_SHOCK` + `mean_reversion` + `MS_TREND_STATE=CHOP` | BTCUSDT | `stat_stretch_04` | 48b | 234 | 2.0936 | 33.1486 | 0.5853 | 0.01814953 | Already characterized: monitor-only, structurally fold-sparse. |

## Template Calibration

Keep:

- `exhaustion_reversal`: 570 unique tests, 9 search passes, 3 deploy-like metric passes. It is the strongest current template, but only with event-context matching. Best contexts: `CARRY_STATE=FUNDING_NEG`, then bullish trend false-breakout pockets.
- `mean_reversion`: 411 unique tests, 7 search passes, 3 deploy-like metric passes. Strongest in `BAND_BREAK / VOL_REGIME=LOW`, `PRICE_DOWN_OI_DOWN / VOL_REGIME=HIGH`, and statistical-stretch chop variants.

Deprioritize:

- `continuation`: 391 unique tests, 1 search pass, 0 deploy-like passes. Keep only for `LIQUIDITY_GAP_PRINT / MS_TREND_STATE=BULLISH / 48b` until a second supporting cell exists.
- `breakout_followthrough`, `false_breakout_reversal`, `reversal_or_squeeze`, `convexity_capture`, `volatility_expansion_follow`: no search-pass evidence in the current local table. Do not expand these templates without a narrower mechanism reason.

## Context Calibration

High-value contexts:

- `CARRY_STATE=FUNDING_NEG`: 72 unique tests, 6 search passes, 2 deploy-like passes. Best context in this dataset. It appears to separate trend-failure exhaustion into a tradable pocket.
- `VOL_REGIME=LOW`: 17 unique tests, 2 search passes, 1 deploy-like pass. Small test count but strong ETH `BAND_BREAK` evidence.
- `VOL_REGIME=HIGH`: 189 unique tests, 2 search passes, 2 deploy-like passes. Best as a positioning-extreme context, not broadly.
- `MS_TREND_STATE=BULLISH`: 173 unique tests, 3 search passes, 1 deploy-like pass. Useful for false breakouts and liquidity-gap continuation.

Weak or ambiguous contexts:

- `MS_TREND_STATE=CHOP`: 188 unique tests, 2 search passes, 0 deploy-like passes. Useful for statistical-dislocation research, not enough for deployment.
- `MS_TREND_STATE=BEARISH`: many positive-net cells but weak robustness in top rows. Use only when paired with a specific microstructure event such as `WICK_REVERSAL_PROXY`.
- `NONE` / `UNKNOWN`: broad slices produce occasional t-stat strength but poor robustness. Avoid unconditional expansions.

## Event Calibration

Promote to next validation lane:

- `CLIMAX_VOLUME_BAR`: strongest aggregate event. Best cell is funding-negative, long, 24b, `exhaustion_reversal`.
- `BAND_BREAK`: best ETH statistical-dislocation event. Focus low-vol mean reversion, 24b first.
- `FALSE_BREAKOUT`: keep as a second trend-failure lane, especially bullish-state 48b.

Keep as controls or monitor-only:

- `PRICE_DOWN_OI_DOWN`: strong supported-path control, but repeated benchmark data should not dominate search policy.
- `OVERSHOOT_AFTER_SHOCK`: good research thesis but robustness remains below deployment gate.
- `ZSCORE_STRETCH`: high sample, low robustness. Useful for threshold/context tuning, not immediate promotion.

Pause or retire from near-term expansion:

- `LIQUIDITY_SHOCK`, `LIQUIDITY_STRESS_DIRECT`, `OI_SPIKE_NEGATIVE`, `OI_SPIKE_POSITIVE`, `RANGE_BREAKOUT`, `TREND_ACCELERATION`: enough local attempts with no positive search-pass evidence.
- `FUNDING_FLIP`, `GAP_OVERSHOOT`, `ABSORPTION_PROXY`, `VOL_CLUSTER_SHIFT`, `RANGE_COMPRESSION_END`, `CHOP_TO_TREND_SHIFT`, `VOL_REGIME_SHIFT_EVENT`, `TREND_DECELERATION`, `SPREAD_BLOWOUT`: no search-pass evidence; keep only if a new mechanism narrows context sharply.

## Recommended Next Runs

1. Run a bounded validation-oriented proposal for `CLIMAX_VOLUME_BAR / exhaustion_reversal / CARRY_STATE=FUNDING_NEG / long / 24b / BTCUSDT`.
   - Stop condition: reject if validation stability remains below 0.70 or if forward/OOS checks fail.
   - Do not add extra contexts or horizons in the first pass.

2. Run an ETH validation-oriented proposal for `BAND_BREAK / mean_reversion / VOL_REGIME=LOW / long / 24b / ETHUSDT`.
   - Stop condition: reject if BTC cross-symbol replay is opposite-sign or if ETH stability falls below 0.70.
   - Do not merge with `ZSCORE_STRETCH`; it is a separate lower-robustness family.

3. If a third lane is needed, run `FALSE_BREAKOUT / exhaustion_reversal / MS_TREND_STATE=BULLISH / long / 48b / BTCUSDT`.
   - Stop condition: reject if `CARRY_STATE` split shows signal concentration only in a thin sub-slice.

4. Treat `PRICE_DOWN_OI_DOWN / VOL_REGIME=HIGH / 24b` as the benchmark control lane.
   - Use it to verify pipeline health and calibration math.
   - Do not let repeated supported-path runs overweight template policy.

## Guardrails

- Do not edit `spec/events/event_registry_unified.yaml`, `spec/events/regime_routing.yaml`, or template registries from this readout alone.
- Do not widen symbols, regimes, horizons, or template families as a rescue tactic.
- Tune by creating one bounded proposal per cell, then validate and compare failure reasons.
- Treat discovery-pass rows as research candidates, not deployment approval.
