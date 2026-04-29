# All Results — Edge Discovery Project

*Auto-generated. Do not edit manually — rerun `project/scripts/update_results_index.py`.*
*3 unique results across 3 events.*
*Gates: bridge = t ≥ 2.0 AND rob ≥ 0.70; phase2 = rob ≥ 0.60. exp = after-cost per trade (bps).*

---

## Summary — Best Result Per Event

| Event | Dir | Horizon | Template | t | rob | q | exp (bps) | Status |
|-------|-----|---------|----------|---|-----|---|-----------|--------|
| CLIMAX_VOLUME_BAR | long | 24b | exhaustion_reversal | 2.25 | 0.704 | 0.0122 | 26.0 | parked: forward failed |
| OVERSHOOT_AFTER_SHOCK | long | 48b | unknown | 2.09 | 0.585 | 0.0181 | 33.1 | monitor-only: robustness failed |
| PRICE_DOWN_OI_DOWN | long | 24b | unknown | 2.35 | 0.839 | 0.0095 | 42.0 | control: year-split pending |

---

## Parked Follow-Up Lanes

| Event | Lane | Reproduction run | Current reason |
|-------|------|------------------|----------------|
| BAND_BREAK | ETHUSDT / vol_regime=low / long / 24b / mean_reversion | `single_event_band_break__20260429T051949Z_d7bff7f5e9` | governed reproduction failed: t_net=0.9394, robustness=0.6691, no bridge candidates |
| FALSE_BREAKOUT | BTCUSDT / ms_trend_state=bullish / long / 48b / exhaustion_reversal | `single_event_false_break_20260429T052713Z_47ac6a4a04` | governed reproduction failed: t_net=0.8627, robustness=0.4052, no bridge candidates |

---

## CLIMAX_VOLUME_BAR

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | exhaustion_reversal | 2.25 | 0.704 | 309 | 0.0122 | 26.0 | parked: forward failed | `single_event_climax_volu_20260428T2127…` |

## OVERSHOOT_AFTER_SHOCK

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 48b | unknown | 2.09 | 0.585 | 234 | 0.0181 | 33.1 | monitor-only: robustness failed | `stat_stretch_04` |

## PRICE_DOWN_OI_DOWN

| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---|-----|---|---|-----------|--------|------------|
| long | 24b | unknown | 2.35 | 0.839 | 79 | 0.0095 | 42.0 | control: year-split pending | `supported_path_20260427T090041Z_price_…` |
