# All Results — Edge Discovery Project

*Auto-generated. Do not edit manually — rerun `project/scripts/update_results_index.py`.*
*3 unique results across 3 events.*
*Gates: bridge = t ≥ 2.0 AND rob ≥ 0.70; phase2 = rob ≥ 0.60. exp = after-cost per trade (bps).*

---

## Summary — Best Result Per Event

| Event | Dir | Horizon | Template | Context | t | rob | n | q | exp (bps) | Status |
|-------|-----|---------|----------|---------|---|-----|---|---|-----------|--------|
| CLIMAX_VOLUME_BAR | long | 24b | exhaustion_reversal | carry=funding_neg | 2.249 | 0.704 | 309 | — | — | bridge gate ⚠ concentration |
| PRICE_DOWN_OI_DOWN | long | 24b | mean_reversion | vol=high | 2.35 | 0.839 | 79 | 0.0095 | 42.0 | bridge gate ⚠ year-split pending |
| OVERSHOOT_AFTER_SHOCK | long | 48b | mean_reversion | chop | 2.09 | 0.585 | 234 | 0.0181 | 33.1 | t passes (rob below gate) |

---

## CLIMAX_VOLUME_BAR

**Warning:** 61% of funding_neg events (189/309) are from 2022 bear market. ETH cross-validation: t=0.69, rob=0.51 — does not replicate. 2023-2024 year-split required before promotion.

| dir | horizon | template | context | t | rob | n | exp (bps) | status | program_id |
|-----|---------|----------|---------|---|-----|---|-----------|--------|------------|
| long | 24b | exhaustion_reversal | carry=funding_neg | 2.249 | 0.704 | 309 | — | bridge gate | `single_event_climax_volu_20260428T212745Z` |

## PRICE_DOWN_OI_DOWN

**Warning:** vol=high slice is 23% of total events (n=79). Unconditional result: t=1.13. 2022 had elevated rv_96 (mean 0.00175 vs 0.00117 in 2023), which inflates the high-vol count. 2023-2024 year-split required.

| dir | horizon | template | context | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---------|---|-----|---|---|-----------|--------|------------|
| long | 24b | mean_reversion | vol=high | 2.35 | 0.839 | 79 | 0.0095 | 42.0 | bridge gate | `supported_path_20260427T001920Z_price_…` |

## OVERSHOOT_AFTER_SHOCK

| dir | horizon | template | context | t | rob | n | q | exp (bps) | status | program_id |
|-----|---------|----------|---------|---|-----|---|---|-----------|--------|------------|
| long | 48b | mean_reversion | chop | 2.09 | 0.585 | 234 | 0.0181 | 33.1 | t passes | `stat_stretch_04` |
