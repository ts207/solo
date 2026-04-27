# Research Thesis: OVERSHOOT_AFTER_SHOCK / chop / long / 48b

## Hypothesis

| Field | Value |
|---|---|
| Event | `OVERSHOOT_AFTER_SHOCK` |
| Context | `ms_trend_state = chop` |
| Direction | long |
| Horizon | 48 bars (4 hours at 5m) |
| Template | `mean_reversion` |
| Entry | episodic, 1-bar lag, overlap suppressed |
| Status | `research_promoted` — monitor-only, not deployable |

**Thesis**: In trend-indeterminate (chop) market regimes, post-shock price overshoots tend to repair over a 4-hour window. A long entry captures the downside-exhaustion version of this repair.

---

## BTC Canonical Metrics

Run: `stat_stretch_04` | Symbol: BTCUSDT | Period: 2022-01-01 – 2024-12-31

| Metric | Value |
|---|---|
| t_stat_net | **2.094** |
| robustness_score | 0.585 |
| net_mean_bps | 33.1 |
| n (events) | 234 |
| p_value | 0.018 |
| q_value | 0.018 |
| validation status | **validated** |
| promotion tier | **research_promoted** |
| deployment gate | ✗ fails `robustness >= 0.70` |

---

## ETH Cross-Symbol Metrics

Run: `stat_stretch_04` (ETHUSDT ingestion) | Period: 2022-01-01 – 2024-12-31

| Metric | Value |
|---|---|
| t_stat_net | 1.165 |
| robustness_score | 0.487 |
| net_mean_bps | 20.8 |
| n (events) | 246 |

Same sign as BTC. Weaker magnitude. The effect generalizes directionally but does not clear the canonical gate independently on ETHUSDT.

---

## Ablation Table

All variants tested against the baseline to establish the local optimum.

| Variant | t_net | robustness | net_bps | n | result |
|---|---|---|---|---|---|
| **BTC chop 48b (baseline)** | **2.094** | **0.585** | **33.1** | **234** | **canonical** |
| BTC chop 24b | 0.662 | 0.268 | 7.1 | 235 | horizon too short |
| BTC chop 72b | 1.329 | 0.511 | 20.6 | 233 | longer horizon weakens |
| BTC low_vol 48b | -0.435 | 0.275 | -4.5 | 85 | wrong context, reverses |
| BTC unconditional 48b | 0.746 | 0.428 | 6.8 | 657 | chop is load-bearing |
| ETH chop 48b | 1.165 | 0.487 | 20.8 | 246 | confirms sign, weaker |

**Key conclusions from ablations:**

- 48b is the correct horizon. Neither 24b nor 72b improves on it.
- Chop context is not incidental. Removing it drops t_net from 2.09 to 0.75 despite tripling event count.
- Low-vol is not a substitute for chop. It reverses the effect.
- ETH confirms directionality. The effect is not BTC-specific.

---

## Deployment Blocker

The thesis is not deployable because:

```
robustness_score = 0.585  <  deployment gate = 0.70
```

The robustness shortfall is a **fold sparsity problem**, not a sign inconsistency problem.

OVERSHOOT_AFTER_SHOCK in chop fires ~78 times per year on BTCUSDT. With 6 walk-forward OOS folds across 3 years, the per-fold event count is too low for some folds to compute valid OOS statistics. Both folds that did compute valid statistics showed positive returns.

There is no nearby hypothesis variant (horizon, context, threshold) that achieves the same t_net with higher robustness. The local optimum has been confirmed.

---

## Monitor-Only Decision

Do not trade this thesis. Do not loosen the robustness gate.

Hold as `research_promoted / monitor_only`. Collect forward evidence without capital exposure.

**Acceptance conditions for later promotion to deployment:**

- `robustness_score >= 0.70`
- `t_net >= 2.0` (maintained)
- `net_bps > 0` after costs
- Forward sign consistency holds (no fold reversal in new data)

---

## Evidence Gap

Additional independent forward evidence is required. The specific quantity depends on:

- Realized forward event rate in the chop regime
- Whether new observations preserve sign consistency across folds
- Cost stability in new market conditions
- Whether a second symbol can be evaluated with per-symbol sign tracking

The pipeline does not currently support multi-symbol pooled evaluation. Evaluating BTCUSDT and ETHUSDT as a joint panel (same hypothesis, pooled folds, per-symbol sign tracking) would be the correct way to assess cross-symbol evidence. That capability does not exist yet and is tracked separately.

---

## Artifact References

| Artifact | Path |
|---|---|
| Cell sweep | `data/reports/phase2/stat_stretch_04/` |
| Canonical candidates | `data/reports/edge_candidates/stat_stretch_04/` |
| Validation bundle | `data/reports/validation/stat_stretch_04/validation_bundle.json` |
| Promotion trace | `data/reports/promotions/stat_stretch_04/research_decision_trace.parquet` |
| Promoted theses | `data/live/theses/stat_stretch_04/promoted_theses.json` |
| Generated proposal | `data/reports/phase2/stat_stretch_04/generated_proposals/edge_cell_stat_stretch_04_overshoot_after_shock_repair_chop_ms_trend_state_chop_long_48b_mean_reversion_btcusdt_v1.yaml` |
| Ablation proposals | `spec/proposals/ablations/oasrep_*.yaml` |

---

*Characterized 2026-04-27. Branch: `offline-data-liq-exhaust-bundle`.*
