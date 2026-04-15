## Current state (2026-04-09)

### What has run

**Infrastructure (pipeline bugs fixed — all working)**
- 9 pipeline bugs fixed (dependency races, zero-output rejections, exit code handling, search engine event type routing, filter template mis-classification, DataFrame.attrs concat crash)
- Pipeline runs end-to-end, exit 0, all stages succeed or warn

**Shared lake cache populated**
- BTC+ETH 2021–2024 cleaned bars, features, market context written to shared lake
- Re-use `--run_id funding_extreme_combined` for same date range to skip data building (fastest cache)

---

### LIQUIDATION_CASCADE_PROXY — STOPPED

Run: `liq_proxy_combined` (BTC+ETH, 2021–2024)

| Symbol | Events | Best horizon | t_stat | mean bps |
|--------|--------|-------------|--------|----------|
| BTCUSDT | ~1000 | 60m long | 1.73 | 10.1 |
| ETHUSDT | ~930 | 60m long | 1.79 | 12.5 |

**Decision: STOP.** t ceiling ~1.8 (gate=2.0). Proxy fires on OI+volume coincidences including false positives. Best config locked: `oi_drop_quantile=0.98`, `vol_surge_quantile=0.90`.

---

### FUNDING_EXTREME_ONSET — STOPPED

4 runs completed (BTC+ETH, 2021–2024, 5m timeframe).

**Best result across all conditioning:**

| Filter | n | t_stat | robustness |
|--------|---|--------|------------|
| `only_if_regime` (rv>0.70) | 671 | 3.31 | **0.527** |
| `only_if_highvol` (rv>0.85) | 667 | 3.35 | 0.519 |
| unconditional | 684 | 3.27 | 0.475 |
| `only_if_trend` (logret>0.001) | 135 | 2.17 | 0.514 |
| `only_if_funding` | 532 | 2.88 | 0.471 |
| `only_if_oi` | 402 | 2.71 | 0.402 |

**Decision: STOP.** Robustness ceiling ~0.527 across all single-feature conditioning. Gate requires 0.6. Signal is real (BTC 60m long, t=3.27–3.35, passes FDR) but structurally regime-inconsistent in a way that no single filter resolves. ETH: no signal at any horizon.

**Watch list:** Revisit if a multi-feature regime classifier is available, or with cross-asset confirmation.

---

### Infrastructure facts

- `spec/events/LIQUIDATION_CASCADE_PROXY.yaml` — `oi_drop_quantile: 0.98` is best calibration (do not change)
- `spec/templates/registry.yaml` — has `only_if_regime`, `only_if_highvol`, `only_if_trend` filter templates; FUNDING_EXTREME_ONSET and BASIS_FUNDING_DISLOCATION family wired up
- `project/events/phase2.py` — LIQUIDATION_CASCADE_PROXY added to PHASE2_EVENT_CHAIN
- `--events EVENTNAME` correctly pins `phase2_event_type` in the planner
- Filter templates are correctly separated from expression templates in resolved search specs
- `promote_candidates` exits 1 + warns (not fails) for missing validation bundle — expected in discovery runs
- Lake cache at `data/lake/runs/funding_extreme_combined/` covers BTC+ETH 2021–2024 fully

**Adding a filter template to an event requires editing 3 files + rebuilding graph:**
1. `spec/templates/registry.yaml` — operator `compatible_families` must use event's `canonical_regime` (not `canonical_family`); add to event block AND families section
2. `spec/templates/event_template_registry.yaml` — mirror event templates
3. `spec/events/event_registry_unified.yaml` — mirror event templates
4. `python3 project/scripts/build_domain_graph.py`
