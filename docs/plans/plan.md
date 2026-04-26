# Edge — Profit-Maximization Plan

> **Project objective.** Maximize trading profit (live, after costs, after capital).
>
> **Plan principle.** Every item is ranked by expected profit per engineering-week. Discovery rate, statistical rigor, and engineering hygiene appear only as *means* to that end — never as ends in themselves. Items the codebase already handles well are noted briefly so engineering effort doesn't go there.

---

## 1. Profit decomposition

```
Annual profit  =  Σ_t (capital_t × Σ_strategies (size_st × pnl_per_unit_st))
              −  Σ_t cost_per_trade_t
              −  drawdown_to_killswitch_lag
              −  capital_idle_cost
```

Profit grows when any of these grows: **net edge per trade**, **trades per period at positive net**, **capital deployed at correct sizing**, **survival of edges in live**, **decorrelation across the portfolio (free Sharpe)**, **speed of cycling capital**, **speed of killing dead strategies**. Profit shrinks via the inverses.

This plan is organised by which lever each change pulls.

---

## 2. What Edge already does well (do not invest engineering hours here)

Verified against source — these are already solid:

| Lever | Existing surface |
|---|---|
| Risk-adjusted sizing | `project/portfolio/sizing.py:calculate_target_notional` — Kelly + vol-target + concentration cap, cluster + correlation adjustments |
| Live decay monitoring | `project/live/decay.py:default_decay_rules` — edge-decay (50%), slippage-spike (2×), hit-rate-decay (40%) |
| Drift alerts | `project/live/drift.py:146` — slippage_drift_ratio > 2.0 alert + fill-rate drift |
| Thesis state machine | `project/live/thesis_state.py` — eligible/active/paused/degraded/disabled |
| Kill-switch audit | `project/live/audit_log.py:191 KillSwitchEvent` |
| Risk budget | `project/portfolio/risk_budget.py:69` — exponential cluster-cap decay |
| Statistical machinery | Newey-West HAC + BH-FDR group keys + block bootstrap robustness |

Implication: the rest of this plan stays **off** these surfaces unless a specific calibration / wiring gap is named.

---

## 3. Where Edge currently leaks money — ranked by $ impact

| # | Leak | Effect on $ | Severity |
|---|---|---|---|
| **$1** | **In-sample over-fit walks into deploy** — forward confirmation optional (`docs/lifecycle/validate.md:79-99`); promotion does not require it (`promotion_service.py:56`) | Promoted theses bleed in live; each false-positive deploy can lose 20–100bp before decay rules trigger | Highest |
| **$2** | **Cost-blind ranking & multiplicity** — `_DEFAULT_PHASE2_MIN_T_STAT = 1.5` is gross (`phase2_search_engine.py:78`); BH-FDR runs on gross p-values; cost-survival is a separate downstream gate | Funnel preferentially promotes high-turnover / cost-fragile candidates that look statistical and lose money | Highest |
| **$3** | **Decay action is downsize-only, no auto-kill** — `default_decay_rules` set `action="downsize"` with `downsize_factor=0.50`; a bleeding strategy at 50% size still bleeds for weeks | A strategy losing 30bp/day at half-size still loses 15bp/day until a human kills it | High |
| **$4** | **No portfolio-level drawdown circuit-breaker** — per-thesis decay only; no global "portfolio down N% in K days → halt new entries" gate | Concurrent decay across correlated theses → portfolio-level drawdown unbounded by any tripwire | High |
| **$5** | **Slippage *variance* and *market impact* not in cost gate** — `expected_cost_per_trade_bps` is a point estimate; high-turnover edges look fine on mean cost and bleed at p95 | High-turnover strategies degrade unpredictably as size grows | High |
| **$6** | **Sizing uses `max_kelly_multiplier=5.0`** (`portfolio/sizing.py`) — full Kelly with edge mis-estimation is famously over-sized; "fractional Kelly" ≤ 0.5 is industry norm | Capital allocated to over-sized positions amplifies edge mis-estimation losses | High |
| **$7** | **Funding-rate awareness absent from cost gate for perp markets** | A long-bias edge that ignores funding can bleed funding-cost in regime where carry > alpha | Medium-high |
| **$8** | **Silent feasibility drop (CLAUDE.md:70)** — proposals testing nothing look like null results | Real edges undiscovered; opportunity cost | Medium |
| **$9** | **No decorrelation gate at promotion** — `max_profile_correlation: 0.90` configured (`promotion_service.py:122`) but verify enforced; correlated promotions add risk without alpha | Capital tied up in redundant exposure | Medium |
| **$10** | **Bound-search snooping** — horizons / thresholds hand-authored, selected post-hoc | Promoted edges have inflated in-sample; live degradation | Medium |
| **$11** | **Cell-first / regime-conditional discovery not the default** — Makefile lanes exist, not turned on | Regime-conditional edges undiscovered | Medium |
| **$12** | **No live alpha-decay measurement / half-life** — decay rules trigger on threshold crossings, not on fitted decay rate | Strategies survive past half-life, drag portfolio | Medium |
| **$13** | **No execution-side optimisation** — single-shot orders; no child-order schedule, no venue selection | Fixed leakage on every trade | Medium |
| **$14** | **No cost regime detection** — cost spec is static; live cost regime change (e.g. wider spreads on weekends) shows up only via drift alert | Strategies trade at mis-estimated costs during regime shifts | Low-medium |
| **$15** | **In-sample / live cost calibration gap** — research uses static `cost_model.yaml`; live observes actual; no closed-loop re-fit | Persistent bias in expected vs realised cost | Low-medium |

The top 6 leaks ($1–$6) account for the bulk of avoidable loss. Plan is built to close them first.

---

## 4. Items ranked by expected $ / engineering-week

> **Notation.** Each item names the leak it closes, expected impact (relative to current loss), the smallest credible code change, and tests.

### Tier 1 — biggest dollar leverage

#### T1.1  Mandatory forward-confirmation gate on `deploy` profile  *(closes $1)*

**Why $.** The largest single source of live drawdown is in-sample over-fits reaching live. Today the gap between in-sample and OOS is documented (`docs/lifecycle/validate.md:79-99`) but ungated. Closing this is the highest-$ change possible.

**Implementation**
- New `project/promote/forward_confirmation.py`:
  ```python
  @dataclass(frozen=True)
  class ForwardConfirmation:
      run_id: str; confirmed_at: dt.datetime
      oos_window_start: dt.datetime; oos_window_end: dt.datetime
      metrics: dict[str, float]   # t_stat_net, sharpe_net, mean_bps_net, n_events
  def validate_forward_confirmation(
      candidate_metrics, confirmation, *,
      drift_tolerance: float = 0.30,        # net t may not drop > 30%
      require_sign_agreement: bool = True,
      min_oos_events: int = 50,
  ) -> ConfirmationVerdict: ...
  ```
- `PromotionConfig` (`project/research/services/promotion_service.py:56`) gains `require_forward_confirmation: bool = False`. Profile defaults at `project/pipelines/stages/research.py:19,39`: research=False, deploy=True (flipped after one cycle of artifact backfill).
- `execute_promotion` (line 401): missing artifact → `degraded_state`; drift → `degraded_state`. No silent promotion.
- Producer CLI: `edge validate forward-confirm --run_id R --window <ISO>/<ISO>` reusing `project/eval/splits.py:163 build_repeated_walk_forward_folds`.

**Tests**
- `tests/promote/test_forward_confirmation.py` — pass / drift / sign-flip / missing / low-events.
- `tests/regressions/test_deploy_profile_forward_required.py` — deploy without artifact rejects; with artifact promotes.

**Estimated profit impact.** Largest of any single change. Even a 50% reduction in deploy-time drawdown from over-fits is significant on any non-trivial deployed capital.

---

#### T1.2  Cost-aware t-gate (net Newey-West HAC at the gate, BH-FDR on net p-values)  *(closes $2)*

**Why $.** Today's gross-only ranking and BH pool let high-gross / negative-net candidates compete with real edges and inflate the FDR critical value, burying genuine net-positive candidates and surfacing fragile ones. Net-only ranking promotes things that survive live cost.

**Note:** The conceptual gate already exists separately as `min_after_cost_expectancy_bps: 0.1` and `min_cost_survival_ratio: 0.80`, but the t-stat used for ranking + multiplicity is gross. T1.2 unifies these.

**Implementation**
- `project/research/phase2_cost_model.py` — formalise `expected_cost_per_trade_bps(features, hypothesis, *, cost_spec, multiplier=1.0) -> pd.Series`.
- `project/research/search/evaluator.py:evaluate_hypothesis` — compute `r_net = r_gross - cost_bps/1e4`; produce `t_stat_gross` AND `t_stat_net`, `mean_return_bps_gross/net`, `expected_cost_bps_per_trade`. Add `gate_status: str` enum.
- `spec/gates.yaml gate_v1_phase2`: add `min_t_stat_net: 1.5`, `min_mean_return_bps_net: 0.0`. Keep `min_t_stat: 2.0` as informational.
- `project/research/phase2_search_engine.py` — gate site reads net thresholds. **BH-FDR pool runs on net p-values** (this is the multiplicity recalibration that delivers most of the dollar value).
- Feature-flagged `gates.phase2.use_net_gate=false` for one cycle; flip in `spec/global_defaults.yaml`.

**Tests**
- `tests/research/search/test_evaluator_cost.py` — synthetic constant-cost: `t_net == HAC-t(r-c)`; high-gross / negative-net dropped.
- `tests/research/test_phase2_search_engine_gates.py` — gate ordering, `gate_status` terminal-once-dropped.

**Estimated profit impact.** Removes the largest source of false-positive promotions that survive the discovery funnel today. High-confidence dollar improvement.

---

#### T1.3  Auto-kill (not just downsize) on persistent decay  *(closes $3)*

**Why $.** A bleeding strategy at 50% size still bleeds. Industry baseline: after a strategy's edge has decayed below threshold for `2 × window_samples`, demote to `disabled`, not `downsize × 0.5`.

**Implementation**
- `project/live/decay.py` — extend `DecayRule` with a `disable_threshold_samples: int | None = None` field. Replace the conservative defaults with a tiered ladder:
  ```python
  DecayRule(metric="edge", threshold=0.50, window_samples=10,
            action="downsize", downsize_factor=0.50,
            disable_threshold_samples=20)            # auto-kill at 2× window
  DecayRule(metric="slippage", threshold=20.0, window_samples=5,
            action="downsize", downsize_factor=0.50,
            disable_threshold_samples=10)
  DecayRule(metric="hit_rate", threshold=0.40, window_samples=10,
            action="warn",
            disable_threshold_samples=20)
  ```
- `DecayMonitor.assess_thesis_health` — when `disable_threshold_samples` reached, transition state to `disabled` via `thesis_state.transition_to("disabled", reason=f"decay_{metric}_persistent")`.
- New audit event `decay_auto_disabled` with full reason chain.

**Re-enable path.** New CLI `edge deploy reinstate --thesis-id T` requires a fresh forward-confirmation artifact (T1.1) on a window starting after disable timestamp. Operator-explicit, not automatic.

**Tests**
- `tests/live/test_decay_auto_kill.py` — sustained breach for `2 × window` → state goes to `disabled`; brief breach → stays `degraded`.
- `tests/live/test_reinstate_path.py` — reinstate fails without fresh confirmation.

**Estimated profit impact.** Bounds the loss per decayed strategy. On a portfolio of N theses, this scales linearly with N.

---

#### T1.4  Portfolio-level drawdown circuit-breaker  *(closes $4)*

**Why $.** Per-thesis decay rules don't see correlated drawdowns. A 5-thesis portfolio where all five lose 1% the same week (correlated regime hit) needs a global tripwire.

**Implementation**
- `project/live/portfolio_circuit.py` (new) reads `live_pnl_window` from `project/live/runtime/` and emits `kill_switch_event` when:
  - rolling `N_days` portfolio drawdown ≥ `max_portfolio_dd_pct` (default 5%), **or**
  - rolling `M_days` realised vol ≥ `2 × target_vol`, **or**
  - per-symbol concentration breaches `concentration_cap_pct × 1.5` (transient breach via correlated entries).
- Action: halt new entries; existing positions managed by their own thesis decay rules.
- Manual reset: `edge deploy circuit-reset --reason "regime_change_acknowledged"` writes audit event.
- `spec/runtime/portfolio_circuit.yaml` exposes thresholds for tuning.

**Tests**
- `tests/live/test_portfolio_circuit.py` — synthetic PnL series triggering each branch.
- `tests/regressions/test_circuit_audit_chain.py` — kill_switch_event audit-log integrity.

**Estimated profit impact.** Bounds maximum drawdown. The single most important survival lever; without it, one bad regime can wipe out a quarter of profit.

---

#### T1.5  Fractional Kelly (cap multiplier ≤ 0.5)  *(closes $6)*

**Why $.** `portfolio/sizing.py:calculate_target_notional` accepts `max_kelly_multiplier=5.0`. Full Kelly is theoretically optimal *under perfect knowledge*; with realistic edge mis-estimation, fractional Kelly (0.25–0.5) **maximises expected log-wealth**. Going to fractional Kelly is a free Sharpe improvement.

**Implementation**
- Reduce default `max_kelly_multiplier` from 5.0 → 0.5 in `project/portfolio/sizing.py:calculate_target_notional` signature; expose via `spec/objectives/<profile>.yaml` so research vs deploy can differ.
- Add per-thesis `kelly_fraction` derived from forward-confirmation drift:
  ```
  kelly_fraction = base_fraction × max(0.1, 1 - drift_ratio)
  ```
  Theses with poor forward confirmation are sized smaller automatically.
- Document the change with a one-paragraph "why fractional" note in `spec/global_defaults.yaml`.

**Tests**
- `tests/portfolio/test_sizing_fractional.py` — `max_kelly_multiplier=0.5` produces ≤ 50% of full-Kelly notional for a known edge.
- `tests/portfolio/test_kelly_fraction_drift.py` — high drift → smaller fraction.

**Estimated profit impact.** Reduces tail-loss frequency dramatically; expected log-wealth growth typically improves vs full Kelly under realistic edge variance.

---

### Tier 2 — meaningful $, contained scope

#### T2.1  Slippage variance + market impact in cost gate  *(closes $5)*

- `project/research/phase2_cost_model.py` returns a `CostDistribution(mean_bps, std_bps, p95_bps, impact_bps_per_unit_size)`.
- New gate `min_cost_survival_p95: 0.50`.
- `spec/cost_model.yaml` extended with per-venue/symbol `slippage_std_bps` (initial heuristic; later fit to live data via T2.6).
- Sizing (`portfolio/sizing.py`) consumes `impact_bps_per_unit_size` as a soft sizing penalty.

#### T2.2  Funding-rate aware cost gate  *(closes $7)*

- For perp products, add `funding_cost_per_bar_bps` to per-trade cost based on hold duration × current funding rate.
- New row column `funding_cost_bps_per_trade`; included in `r_net` calc.
- Spec source: existing perp data already carries funding columns in `data/lake/features/...`.
- Gate: implicit via T1.2 (now baked into `r_net`); informational column persisted.

#### T2.3  Decorrelation gate at promotion  *(closes $9)*

- Verify (and if not, wire) `max_profile_correlation: 0.90` in `promotion_service.py:execute_promotion`. Add correlation check vs already-promoted theses' OOS PnL streams.
- New funnel slot `dropped:decorrelation` (consumed by Tier 3 funnel artifact below).
- Soft companion: regime-profile cosine similarity > 0.85 → warning, not gate.

#### T2.4  Bound-search cross-validation  *(closes $10)*

- New `project/research/search/bound_search.py` — Stage-0 sweep over authored axes (`horizon_bars`, `entry_lag_bars`, severity thresholds) on the **train** half; selects on **validation** half; final gate evaluation on **test**.
- Wrapped behind `proposal.search_control.bound_search.enabled`; opt-in initially.
- Discarded sweep candidates listed in funnel as `dropped:bound_search`.

#### T2.5  Cell-first regime-conditional discovery as default  *(closes $11)*

- Already exists as Makefile lanes (`make discover-cells-*`).
- A/B against flat search on the canonical proposal across two release cycles using the funnel artifact (T3.1).
- If `cell_first_promoted ≥ flat_promoted`, default `cell_first: true` in `project/pipelines/stages/research.py`.

#### T2.6  Live-fit cost model (closed-loop)  *(closes $14, $15)*

- Nightly script `project/scripts/fit_cost_model.py` reads OMS audit logs, fits per-venue/symbol `slippage_mean_bps`, `slippage_std_bps`, hour-of-day buckets, regime buckets.
- Writes `spec/cost_model.live.yaml` (read-only auto-generated). Research uses static `cost_model.yaml`; live runtime uses `live` overlay where present.
- Drift dashboard tile: research vs live cost delta.

#### T2.7  Alpha half-life monitoring  *(closes $12)*

- `project/live/decay.py` extended with a fitted decay model: per-thesis `edge_t = edge_0 × exp(-λ × t)` fit on rolling window. Surface `half_life_days` in dashboard.
- New decay rule `half_life < min_acceptable_days` → downsize then disable (uses T1.3 ladder).
- Operator can pre-deploy filter on expected half-life from research walk-forward.

---

### Tier 3 — supporting infrastructure (small $, large unblocking effect)

These don't directly produce $ but unblock items above and make survival numbers visible.

#### T3.1  Funnel artifact + index  *(supports $-tracking)*

- `data/reports/phase2/<run_id>/funnel.json` per-run survival counts (built from `gate_status` enum from T1.2).
- Append-only `data/reports/phase2/funnel_index.parquet`.
- CLI `edge discover funnel --run_id R`, `edge discover funnel-trend --program X`.
- Enables ranking proposal styles by survival rate, which informs where to invest research effort.

#### T3.2  Hard-error on silent feasibility failure  *(closes $8)*

- `project/research/search/feasibility.py` returns `FeasibilityReport`; `validate_agent_request` raises `FeasibilityError` if surviving < `min_feasible`. Both paths covered (experiment-config and legacy proposal).
- `--allow-empty` escape valve.
- $ via opportunity cost only; small but structural.

#### T3.3  Negative-result registry

- Append-only `data/artifacts/experiments/<program>/memory/negative_results.parquet`.
- Pre-search filter skips re-testing within TTL (per failure reason).
- Frees compute *and* shrinks BH pool → improves multiplicity.

#### T3.4  Auto-generate template/event registries from a single source

- `spec/events/event_registry_unified.yaml` becomes source of truth.
- `project/scripts/build_template_registries.py --check` in pre-commit + CI.
- Removes the most common cause of T3.2's silent failures.

#### T3.5  Hypothesis fingerprint dedup across proposals

- Stable hash on canonical hypothesis spec; cross-proposal dedup avoids BH-pool double-counting.

---

### Tier 4 — execution alpha and other long-tail $

#### T4.1  Order-shape execution layer  *(addresses $13)*

- Replace single-shot order with TWAP/VWAP child schedules (or Almgren-Chriss for high-impact entries).
- Implement venue-side: `project/live/execution/scheduler.py`. Plug into existing OMS path in `project/live/runner.py`.
- Calibration target: reduce realised-vs-expected slippage by ≥ 30% on size > liquidity_factor × 0.5.

#### T4.2  Venue selection / smart routing

- Lightweight router that picks venue per symbol on (spread, depth, fee, latency).
- Spec: `spec/runtime/venues.yaml`. Already partially handled by `EDGE_VENUE` env var; this generalises.

#### T4.3  Capital-efficiency / leverage profile per regime

- Existing risk-budget code already supports per-regime adjustments; expose a profile that reduces gross during high-vol regimes (already partial via `vol_target`) and **increases** gross during low-vol high-conviction regimes (currently capped one-way at line 38 of `portfolio/sizing.py:_resolve_volatility_adjustment`).
- Symmetric vol targeting under fractional-Kelly cap is a measured Sharpe lift.

#### T4.4  Tax-lot / funding optimization

- Pure live-side: prefer reducing positions held into adverse funding windows; minor effect, but adds up over many trades.

---

## 5. Sequencing — 4 weeks → measurably more $

> Order is by expected $ / engineering-week, with prerequisite chains respected.

| Week | Items | Why this order |
|---|---|---|
| **Week 1 — Stop the bleeding (top 4 leaks)** | **T1.1** forward-confirmation gate · **T1.2** net-t-gate (with feature flag default off + ship both columns) · **T1.5** fractional Kelly | Fixes the three biggest sources of $-loss. T1.5 is essentially a one-line config change with massive expected impact. T1.2 ships behind a flag this week and flips next week. |
| **Week 2 — Bound the downside** | **T1.3** auto-kill ladder · **T1.4** portfolio circuit-breaker · **T1.2 flip** flag to require net-gate · **T3.1** funnel artifact · **T3.2** feasibility hard-error | Auto-kill + circuit-breaker bound the worst-case live drawdown. Flipping T1.2 is safe once T1.3/T1.4 are live. T3.1 delivers the dashboard for everything that follows. |
| **Week 3 — Ratchet the gates** | **T2.1** slippage variance · **T2.2** funding-aware cost · **T2.3** decorrelation gate · **T2.6** live-fit cost model | All four tighten the cost picture; do them together because they share the same cost-spec surface. Decorrelation requires funnel from week 2. |
| **Week 4 — Coverage & long-tail** | **T2.4** bound search · **T2.5** cell-first comparison · **T2.7** alpha half-life · **T3.3** negative-result registry · **T3.4** registry auto-gen · **T4.1** execution scheduler (start) | Coverage and execution alpha are second-order to gating; they pay only when the gates above are tight. T4.1 is a multi-week effort started here. |

After week 2, **`require_forward_confirmation` defaults to True for the deploy profile** (`spec/global_defaults.yaml`). After week 3 the live cost overlay (`spec/cost_model.live.yaml`) is in production.

---

## 6. KPIs — track these in $, not in $-proxies

A profit-focused dashboard tracks four numbers weekly off `funnel_index.parquet` + live PnL:

| KPI | Definition | What it tells us |
|---|---|---|
| **Live Sharpe (rolling 30d)** of promoted theses, post-cost | Σ thesis PnL / Σ thesis vol | Ground truth |
| **In-sample → live decay ratio** | live `t_stat_net` / in-sample `t_stat_net` | T1.1 should drive this toward 1.0 |
| **Drawdown to kill-switch latency (median, p95)** | days from edge breach to `disabled` | T1.3 + T1.4 should drive this < window_samples |
| **Distinct-edge count (decorrelation-adjusted)** | promoted theses minus correlation-overlap demotions | T2.3 keeps this honest |

Secondary (operational):

- `t_net_passed / generated` — power per run (T1.2)
- `dropped:cost_survival / t_net_passed` — funnel cost-bleed (T2.1, T2.2)
- `negative_filter_skipped / generated` — efficiency (T3.3)
- `live_cost_drift_ratio` (research vs live cost) — closes via T2.6

---

## 7. Critical files referenced

**Profit-side already-built**
- `project/portfolio/sizing.py:calculate_target_notional` — sizing with Kelly etc. (T1.5)
- `project/portfolio/risk_budget.py:69` — exponential cluster cap
- `project/live/decay.py:default_decay_rules` — decay rules (T1.3, T2.7)
- `project/live/drift.py:146` — slippage drift (T2.6)
- `project/live/thesis_state.py` — state machine (T1.3)
- `project/live/audit_log.py:191` — kill-switch audit (T1.4)

**Discovery / promotion gates**
- `project/research/phase2_search_engine.py:78` — gate site (T1.2)
- `project/research/search/evaluator.py` — per-trade evaluator (T1.2)
- `project/research/phase2_cost_model.py` — cost model (T1.2, T2.1, T2.2)
- `project/research/services/promotion_service.py:56,122,401` — `PromotionConfig`, defaults, `execute_promotion` (T1.1, T2.3)
- `spec/gates.yaml gate_v1_phase2` — gate spec (T1.2, T2.1)
- `project/eval/splits.py:163` — repeated walk-forward folds (T1.1)

**Pipelines / profiles**
- `project/pipelines/stages/research.py:19,39` — research vs deploy profile defaults
- `spec/global_defaults.yaml` — feature-flag flips

**New surfaces**
- `project/promote/forward_confirmation.py` (T1.1)
- `project/validate/forward_confirm.py` (T1.1)
- `project/live/portfolio_circuit.py` (T1.4)
- `project/io/funnel.py` (T3.1)
- `project/research/search/bound_search.py` (T2.4)
- `project/research/knowledge/negative_results.py` (T3.3)
- `project/scripts/fit_cost_model.py` (T2.6)
- `project/live/execution/scheduler.py` (T4.1)

**Spec authoring**
- `spec/cost_model.yaml`, `spec/cost_model.live.yaml` (T2.1, T2.6)
- `spec/runtime/portfolio_circuit.yaml` (T1.4)
- `spec/objectives/<profile>.yaml` (T1.5)

---

## 8. Verification — end-to-end smoke after week 4

```bash
# T1.1 — forward confirmation blocks deploy without artifact
edge promote run --run_id R --profile deploy --symbols BTCUSDT
# expect: degraded_state ledger entries "forward_confirmation_missing"
edge validate forward-confirm --run_id R --window 2025-07-01/2025-09-30
edge promote run --run_id R --profile deploy --symbols BTCUSDT
# expect: theses written, funnel.forward_confirmed > 0

# T1.2 — net t-gate is load-bearing; BH-FDR runs on net p
edge discover funnel --run_id R
# expect: t_net_passed ≤ t_gross_passed, q-values computed from net p

# T1.3 — sustained edge decay → auto-disable
PYTHONPATH=. python3 -m pytest project/tests/live/test_decay_auto_kill.py -q

# T1.4 — portfolio drawdown trip
PYTHONPATH=. python3 -m pytest project/tests/live/test_portfolio_circuit.py -q

# T1.5 — fractional Kelly default
python3 -c "from project.portfolio.sizing import calculate_target_notional;
print(calculate_target_notional(...).notional)" # ≤ ½ of prior full-Kelly value

# T2.* tier — cost gate tightened
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml --run_id R2
edge discover funnel --run_id R2
# expect: dropped:cost_survival_p95 + dropped:decorrelation columns populated

# T2.6 — live cost drift visible
cat data/reports/dashboard/cost_drift.json
# expect: per-venue drift ratios, alert if > 1.5

# Architectural integrity
make minimum-green-gate
PYTHONPATH=. python3 -m pytest project/tests/test_architectural_integrity.py -q
```

After this gate is green, the four KPIs in §6 are tracked weekly. The plan succeeds when **live Sharpe ≥ 0.7 × in-sample Sharpe** and **drawdown-to-kill latency < 2 × window_samples** sustainably across two quarters.

---

# 9. Methodological proposals — worthy of consideration

> The plan above closes leaks in the **existing** method. This section asks a different question: **are there changes to the method itself that would make Edge a better profit-discovery system?** Each proposal is judged on whether it is worth the disruption.
>
> Worth: ✅ adopt · 🟡 pilot first · ❌ rejected (rationale given so it isn't re-proposed).

## M1. Replace point-estimate t-stat ranking with **expected-log-wealth contribution**  ✅

**Today.** The funnel ranks by Newey-West HAC t-stat. Promotion is a threshold pass/fail at `t_stat ≥ 1.5`. Sizing is a downstream stage that doesn't see the rank.

**Proposal.** Compute, per candidate, the **expected log-wealth contribution at fractional-Kelly size**:
```
U(h) = E[ log(1 + f* × r_net(h)) ] − λ × Var[ log(1 + f* × r_net(h)) ]
```
where `f*` is the fractional-Kelly size (T1.5) and `λ` is a small variance penalty for parameter uncertainty. Rank and promote on `U`, not on `t`.

**Why this is *the* methodologically correct objective for trading:**
- Profit compounds; log-wealth is what matters for terminal capital.
- A candidate with `t = 2.5` but tiny edge × tight liquidity contributes negligible `U`; a candidate with `t = 1.8` but persistent moderate edge contributes more. The current funnel has these inverted.
- Naturally penalises candidates whose net edge collapses under stress (the `Var` term subsumes much of T2.1 + T2.7).

**Implementation sketch.**
- `project/research/services/utility.py` (new) — `expected_log_wealth_contribution(record, *, kelly_fraction, lambda_var)`.
- Persist as column `log_wealth_contribution_bps` on `evaluation_results.parquet`.
- Promotion gate in `promotion_service.py` switches from t-stat to `log_wealth_contribution_bps > min_lwc_bps` (default 0.5 bps/trade for research, 1.0 for deploy).
- Backwards-compat: keep `t_stat_net` and `q_value` as informational columns for one release.

**Risk / why not rejected.** Statisticians sometimes object that log-utility is a strong assumption. For trading capital, it is the assumption most consistent with **survival + compounding**, which is the operator's actual goal. Worth adopting.

---

## M2. Replace BH-FDR with **empirical-Bayes shrinkage**  ✅

**Today.** BH-FDR by group key (`canonical_bh_group_key`, `core/stats.py:433-471`) controls FDR but produces binary decisions. A candidate at `q = 0.049` and one at `q = 0.051` are treated as opposite outcomes despite indistinguishable evidence.

**Proposal.** Estimate the prior distribution of true effect sizes empirically across all evaluated hypotheses in a run (Efron's empirical-Bayes / `f`/`f0` ratio, or a mixture-of-Gaussians fit). Shrink each candidate's `mean_return_bps_net` toward the prior mean by a factor proportional to its standard error. The shrunk posterior mean — call it `mu_post` — is what feeds sizing (M1's `U`).

**Why $.**
- Binary BH gates have a cliff at the threshold; capital allocation jumps discontinuously at `q = 0.05`. Empirical-Bayes shrinks smoothly; capital allocation is a continuous function of evidence strength.
- Shrunken estimates are **calibrated**: the realised mean of the top-10 promoted candidates is closer to expectation, which directly improves live-vs-research correlation.
- Naturally handles correlated tests (the prior is fit on the full sample of correlated candidates).

**Implementation sketch.**
- `project/eval/empirical_bayes.py` (new) — `fit_prior(candidates) -> Prior`, `shrink(candidate, prior) -> float`.
- Use `pyro` or roll a small mixture-EM fit (~150 LOC). Sample size constraint: prior fit only valid when `len(candidates) ≥ 50` per group; below that, fall back to BH.
- Adds columns `mu_post_bps`, `shrinkage_factor` to `evaluation_results.parquet`.
- M1's `U` consumes `mu_post_bps` instead of raw mean.

**Sequencing.** Land *after* M1 and T1.2. Empirical Bayes makes M1's `U` more accurate; without M1, EB has nothing to feed.

**Risk.** Mis-fit prior on a single bad run can mis-shrink everything that run. Mitigation: cap shrinkage at `0.5` (no candidate moves by more than half its SE), and require a fixed minimum group size before EB takes effect.

---

## M3. **Joint promote-and-size under portfolio log-wealth**  ✅

**Today.** Promotion is per-thesis (gate stack). Sizing is later, in `portfolio/sizing.py`. Decorrelation is post-hoc (T2.3 adds it as a soft filter at promotion). The three are sequential and locally optimal — never jointly optimal.

**Proposal.** Replace the per-thesis promotion gate with a **portfolio-level greedy selection** under expected log-wealth:

```
Given already-promoted set S, candidate h is promoted iff
    U_marginal(h | S) = E[log(1 + Σ_{s ∈ S∪{h}} f_s × r_s)] − E[log(1 + Σ_{s ∈ S} f_s × r_s)]
    > min_marginal_log_wealth
```

This is a portfolio-construction problem solved at promotion time. It is the **methodologically correct unification of three currently-separate concerns**: promotion, sizing, decorrelation.

**Why $.**
- Decorrelation is no longer a soft afterthought (T2.3); it is **structurally** in the objective: a candidate correlated with `S` has lower marginal `U` automatically.
- Sizing follows from the same optimization (the `f_s` are joint-optimal under the constraint).
- A weak-but-uncorrelated candidate can outscore a strong-but-correlated one, which is what the portfolio actually wants.
- Single-objective replaces three thresholds, which removes one cliff per stage.

**Implementation sketch.**
- New `project/promote/portfolio_selection.py` — greedy or convex (CVXPY) optimization over the candidate set.
- Inputs: per-candidate posterior mean (M2), per-candidate per-trade return time series (already in `evaluation_results.parquet`), pairwise correlation matrix (computed on-the-fly).
- Output: subset to promote + per-thesis Kelly fraction. Persisted into `data/live/theses/<run_id>/promoted_theses.json` alongside existing fields.
- Decay rules in live (T1.3) consume Kelly fraction directly — no separate sizing-policy stage needed for new theses.

**Sequencing.** Requires M1 + M2 + T2.3 plumbing. Land in week 5 of the plan above.

**Risk.** Optimization-time computation; for ~100 candidates, both greedy and CVXPY finish in seconds. Bigger candidate sets warrant a chunked greedy with covariance-block diagonalisation. Standard.

---

## M4. **Synthetic-control / placebo gate**  ✅

**Today.** BH-FDR controls FDR across hypotheses, but does not test **specificity**: that the apparent edge attaches to the *event*, not just to the regime in which the event tends to occur.

**Proposal.** For every hypothesis `h = (event, template, ...)`, also evaluate `h_placebo` where the event series is replaced by a placebo: randomly chosen timestamps with the same regime distribution and same density. Run the same evaluator. Reject `h` if the placebo achieves comparable `U` (M1).

**Why $.** Distinguishes a genuine causal microstructure edge (the event triggers the move) from a regime-fishing artefact (the event tends to fire when the regime alone would have produced the move anyway). The latter does not survive live, because the event-arrival timing isn't actually informative — you'd capture the same edge by trading the regime.

**Implementation sketch.**
- `project/research/search/placebo.py` — `build_placebo_series(events, regime_state, *, density_match=True, regime_match=True, n=500)` returns matched-density placebo timestamps.
- Each evaluated record gains `U_placebo_p95` (95th percentile of placebo `U` under bootstrap).
- New gate: `U(h) − U_placebo_p95(h) ≥ min_specificity_lift` (default 0.3 bps/trade).

**Tests.** Synthetic data: real signal at event timestamps; placebo should yield zero. Pure regime signal with no event-specific effect; placebo should *also* yield it → real `h` is rejected.

**Risk.** Compute roughly doubles per hypothesis. Mitigate by only running placebo on candidates that survive the cheap gates first (post-M1 ranking, top decile).

---

## M5. **Continuous Bayesian updating in live** (replace decay-threshold rules)  🟡 pilot

**Today.** Decay rules trigger on threshold crossings (`edge_decay`, `slippage_spike`, `hit_rate_decay` — `live/decay.py`). Auto-kill ladder (T1.3) tightens this but is still threshold-based.

**Proposal.** Maintain a **running posterior** on each thesis's edge from the moment it enters live:
```
prior:   N(mu_research, sigma_research²)  ← from M1+M2 in-sample
likelihood: each live trade updates posterior via Bayes
position size:  f_t = fractional_kelly(posterior_t)  ← continuous
```
A thesis is "disabled" if `P(edge > 0 | posterior_t) < 0.4` for `K` consecutive bars; "reinstated" if `P(edge > 0) > 0.7`. Sizing flows continuously between, not in fixed downsize tiers.

**Why $.**
- Smooth de-allocation as edge fades; capital reallocates to live winners without operator intervention.
- Reinstatement is automatic when evidence returns (e.g. regime cycle).
- Decay rules become a fallback for when the Bayesian path mis-fires, not the primary mechanism.

**Why pilot, not adopt.**
- Requires posterior implementation and battle-testing.
- Risk: overconfident in-sample prior (low `sigma_research`) → posterior moves slowly when edge breaks → bleed.
- Mitigation: cap prior precision at a value calibrated from historical research-vs-live drift.

**Pilot.** Run on one promoted thesis in shadow mode for one quarter, alongside the existing decay rules. Compare drawdown profile.

---

## M6. **Causal-graph-driven hypothesis generation**  🟡 pilot, longer horizon

**Today.** Hypotheses are operator-authored. The space is bounded by what humans propose.

**Proposal.** Fit a causal DAG over the event-and-state vocabulary using PC algorithm or NoTears on a clean lake. Edges in the inferred DAG that have not been tested as hypotheses become **automatically generated proposals**. Operator reviews and runs them through the existing funnel.

**Why $.**
- Expands coverage with low marginal effort.
- Surfaces non-obvious chains (e.g. `funding_persistence → depth_recovery → liquidation_cascade → exhaustion_reversal`) that no human would author.

**Why pilot.** Inferred DAGs from finite financial data are noisy; many edges are spurious. Worth testing as a **proposal-suggestion** tool, not as an autonomous proposer. Useful only after the funnel artifact (T3.1) is mature enough to filter the noise.

---

## M7. Things considered and **rejected** (do not re-propose)

- **❌ Continuous neural embedding of market state.** Loses the event-trigger × execution-template factorisation that gives Edge its interpretability and PIT discipline. High overfit risk under finite data. Not worth the disruption.
- **❌ Hidden Markov model regimes as primary state.** Authored regimes (`spec/states/state_registry.yaml`) are interpretable, PIT-clean, and stable across data updates. Latent regimes complicate lookahead and lose the ability to author hypotheses against named states.
- **❌ Drop the discrete promotion boundary.** Continuous capital allocation across all candidates above a floor is theoretically attractive but operationally unmanageable: too many state transitions to audit, too much capital fragmentation. Keep the discrete promote step; M5 already gives the live-side smoothing.
- **❌ Reinforcement-learning execution.** Premature; T4.1 (TWAP/VWAP/Almgren-Chriss) captures the bulk of available execution alpha at low risk.
- **❌ Replace walk-forward with leave-one-month-out.** Marginal benefit; walk-forward + embargo (`eval/splits.py:163`) is well-suited to the time-correlation structure here. Keep.

---

## 10. Methodological proposals — sequencing fit

The methodological changes integrate into the 4-week plan as follows. None of them should be attempted before the leak-closing tier (T1.*) lands — they are **multiplicative on a plumbed funnel, not substitutes for one**.

| Methodological | Depends on | Earliest sensible week |
|---|---|---|
| **M1** log-wealth ranking | T1.2 net columns, T1.5 fractional Kelly | Week 3 |
| **M2** empirical-Bayes shrinkage | M1 | Week 4 |
| **M3** joint portfolio promotion | M1 + M2 + T2.3 | Week 5 (post-plan) |
| **M4** placebo gate | T1.2, T3.1 funnel | Week 4 |
| **M5** Bayesian live posterior (pilot) | T1.3 auto-kill, M1 + M2 | Quarter 2 pilot |
| **M6** causal-DAG hypothesis suggestion | T3.1 funnel index, T3.4 registry auto-gen | Quarter 2+ |

The four ✅ items (M1, M2, M3, M4) together change Edge from a **pass/fail discovery funnel** into a **utility-maximising selection process**. That is the largest single methodological upgrade available — and it makes the dollar levers in §3 even more effective by routing capital optimally rather than by threshold.

---

## 11. Net effect of methodological upgrade

After M1–M4 land on top of the 4-week plan:

- "Promotion" is a **continuous score** (`U`), not a binary pass.
- Sizing, promotion, and decorrelation are **one optimization** (M3), not three.
- "False discovery rate" is replaced by **calibrated effect-size posteriors** (M2) — capital sized to evidence strength, not to a multiplicity threshold.
- "Specificity" — the question *was the edge really event-driven?* — is **gated** (M4), not assumed.
- The KPI hierarchy in §6 simplifies: the primary KPI becomes **portfolio expected-log-wealth growth (live)**, with every secondary KPI a diagnostic of why log-wealth growth is or isn't tracking research expectation.
