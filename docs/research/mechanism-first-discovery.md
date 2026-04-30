# Mechanism-First Discovery

Default research mode is mechanism-first: define a market pressure, compile a
small bounded proposal set, then falsify aggressively before any paper-runtime
consideration.

## Research Loop

1. Select one active mechanism from `spec/mechanisms/registry.yaml`.
2. Compile one to three bounded proposals from the mechanism spec.
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
  --proposal data/reports/mechanisms/forced_flow_reversal/generated_proposals/forced_flow_price_down_oi_down_highvol_long_h24_btc.yaml
```

Passing preflight classifies a proposal as `mechanism_backed`. A proposal with no
mechanism is `scouting_only` and cannot be promoted beyond `scouting_signal`
without a retroactive mechanism note and re-test. A proposal that violates its
mechanism is `mechanism_violation` and should be parked or killed rather than
mutated.

## Forced-Flow Reversal

`forced_flow_reversal` is the first active mechanism. It tests whether forced
deleveraging creates temporary downside pressure followed by short-horizon
reversal.

Allowed Wave 1 seeds are deliberately narrow:

- `PRICE_DOWN_OI_DOWN`, `vol_regime=high`, `mean_reversion`, long, 24 bars.
- `CLIMAX_VOLUME_BAR`, `carry_state=funding_neg`, `exhaustion_reversal`, long, 24 bars.
- `LIQUIDATION_EXHAUSTION_REVERSAL`, `vol_regime=high`, `exhaustion_reversal`,
  long, 24 bars.

Forbidden rescue actions include dropping bad years after seeing results,
changing horizon or context after failure, adding symbols after failure,
loosening gates, and promoting without specificity controls or forward
confirmation.

## PRICE_DOWN_OI_DOWN Handling

`PRICE_DOWN_OI_DOWN` is a forced-flow clue, not a deployable edge. Its current
status should remain parked/review until specificity controls and post-discovery
falsification exist. If controls fail or the evidence remains a 2022 artifact,
kill it. If controls pass and non-2022 evidence is acceptable, keep it only as a
regime-conditional research candidate.
