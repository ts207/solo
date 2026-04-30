# Mechanism-First Discovery

Default research mode is mechanism-first: define a market pressure, compile a
small bounded proposal set, then falsify aggressively before any paper-runtime
consideration.

## Research Loop

1. Select one active mechanism from `spec/mechanisms/registry.yaml`.
2. Compile one to three bounded proposals from the mechanism spec after required
   upstream evidence gates pass.
3. Run mechanism preflight before discovery.
4. Run discovery and `discover-doctor`.
5. Record search burden and refresh the results index.
6. Reproduce governed materialization.
7. Extract candidate traces and required specificity controls.
8. Run year/PnL split, cost stress, and forward confirmation.
9. Paper only after every required gate passes.
10. Park or kill after a major falsification failure.

## Evidence Classes

- `scouting_signal`: broad discovery output without a passing mechanism preflight.
- `candidate_signal`: mechanism-backed discovery candidate.
- `reproduced_signal`: current governed materialization reproduced the candidate.
- `research_edge`: validation passed, but forward confirmation is not yet passed.
- `confirmed_edge`: forward confirmation passed.
- `paper_edge`: paper execution passed.
- `deployable_thesis`: live-approved, capped, human-authorized thesis.
- `parked_candidate`: plausible but blocked by a required gate.
- `killed_candidate`: falsified; do not retry without new mechanism or data.

Nothing is an edge before forward confirmation. Nothing is tradable before paper
execution.

## Wave 1 Interfaces

Compile bounded forced-flow proposals:

```bash
PYTHONPATH=. python3 project/scripts/compile_mechanism_proposals.py \
  --mechanism forced_flow_reversal \
  --symbol BTCUSDT \
  --start 2022-01-01 \
  --end 2024-12-31 \
  --data-root data \
  --limit 3
```

Preflight a proposal before discovery:

```bash
PYTHONPATH=. python3 project/scripts/mechanism_preflight.py \
  --proposal data/reports/mechanisms/forced_flow_reversal/generated_proposals/forced_flow_oi_flush_highvol_long_h24_btc.yaml
```

Passing preflight classifies a proposal as `mechanism_backed`. A proposal with no
mechanism is `scouting_only` and cannot be promoted beyond `scouting_signal`
without a retroactive mechanism note and re-test. A proposal that violates its
mechanism is `mechanism_violation` and should be parked or killed rather than
mutated.

## Forced-Flow Reversal

`forced_flow_reversal` was the first active mechanism. It tests whether forced
deleveraging creates temporary downside pressure followed by short-horizon
reversal.

Current mechanism decision: `pause`. `PRICE_DOWN_OI_DOWN` is parked as
`context_proxy_and_year_pnl_concentration_2022`, and `OI_FLUSH` is killed as
`governed_reproduction_negative_t_stat`. Do not keep testing nearby forced-flow
variants unless there is a new ex-ante crisis/high-vol thesis, a stronger
liquidation/deleveraging observable, or a material data-quality upgrade.

The ex-ante crisis/high-vol reopen path is represented by the
`forced_flow_crisis_v1` regime matrix:

- primary: `vol_regime=high+carry_state=funding_neg+ms_trend_state=bearish`
- diagnostics: high-vol bearish, negative-carry bearish, and high-vol
  negative-carry rows.

The primary row must classify `stable_positive` before any forced-flow event-lift
or proposal work is allowed. A negative primary row keeps `forced_flow_reversal`
parked.

Allowed Wave 1 seeds are deliberately narrow:

- `OI_FLUSH`, `vol_regime=high`, `exhaustion_reversal`, long, 24 bars.
- `CLIMAX_VOLUME_BAR`, `carry_state=funding_neg`, `exhaustion_reversal`, long, 24 bars.
- `LIQUIDATION_EXHAUSTION_REVERSAL`, `vol_regime=high`, `exhaustion_reversal`,
  long, 24 bars.

Forbidden rescue actions include dropping bad years after seeing results,
changing horizon or context after failure, adding symbols after failure,
loosening gates, and promoting without specificity controls or forward
confirmation.

## PRICE_DOWN_OI_DOWN Handling

`PRICE_DOWN_OI_DOWN` is a forced-flow clue, not a deployable edge. Its current
status is parked as `context_proxy_and_year_pnl_concentration_2022`. Do not keep
iterating on this event formulation. Reopen only under a new ex-ante crisis/high-vol
regime thesis, or move to a stronger forced-flow observable such as `OI_FLUSH`.

## Funding Squeeze

`funding_squeeze` is the next active mechanism family. It tests whether extreme
funding and crowded perpetual positioning create unwind or squeeze pressure that
resolves as reversal or continuation after a stress trigger.

Funding-squeeze proposals are gated on event-lift evidence. The compiler may
emit a proposal only when a matching `event_lift.json` report has:

- `decision == advance_to_mechanism_proposal`
- `promotion_eligible == true`
- `audit_only == false`
- `scorecard_decision == allow_event_lift`

Audit-only, parked, scorecard-blocked, or otherwise non-promotable event-lift
reports are hard rejections. The compile tuple must match the event-lift report
on `mechanism_id`, `event_id`, `regime_id`, `symbol`, `direction`, and
`horizon_bars`.

Compile from a specific passing event-lift run:

```bash
PYTHONPATH=. python3 project/scripts/compile_mechanism_proposals.py \
  --mechanism funding_squeeze \
  --symbol BTCUSDT \
  --start 2022-01-01 \
  --end 2024-12-31 \
  --data-root data \
  --limit 1 \
  --require-event-lift-pass \
  --event-lift-run-id <event_lift_run_id> \
  --regime-id 'vol_regime=high+carry_state=funding_neg' \
  --event-id FUNDING_EXTREME_ONSET \
  --direction long \
  --horizon-bars 24 \
  --template-id exhaustion_reversal
```

If `--event-lift-run-id` is omitted, the compiler searches
`data/reports/event_lift/*/event_lift.json` for the latest matching non-audit
passing event-lift result. If the mechanism has multiple allowed templates,
`--template-id` is required; this keeps template choice explicit instead of
encoding narrative assumptions in the compiler.

Do not reuse failed forced-flow evidence as support.
