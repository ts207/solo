# Research Vocabulary

This project uses one operational model for research and runtime:

```text
Data -> Features -> Detectors -> Events -> Context / Regime / State -> Filters -> Templates -> Proposal -> Validation -> Promotion -> Thesis -> Policy
```

## Canonical terms

| Term | Meaning | Example |
|---|---|---|
| Detector | Finds an event in market data. | `VOL_SHOCK` detector |
| Event | The output emitted by a detector. | `VOL_SHOCK` fired at a timestamp |
| Regime | Broad market mode. | calm, transition, high volatility, stressed liquidity |
| State | Numeric representation of a regime. | `ms_vol_state = 2` |
| Context | Facts around an event. | spread, depth, funding, OI, volatility state |
| Filter | Explicit pass/fail eligibility rule. | `ms_vol_state >= 1`, `spread_bps <= 5` |
| Template | Trade behavior after an eligible event. | continuation, mean reversion |
| Proposal | One bounded experiment specification. | BTCUSDT 5m `VOL_SHOCK` continuation h12 |
| Candidate | Unvalidated research result. | ranked discovery row |
| Thesis | Validated and promoted artifact. | `data/live/theses/<run_id>/promoted_theses.json` |
| Policy | Runtime decision layer. | trade, skip, reduce size, kill switch |

## Rules

A detector is not a strategy. It only says an event happened.

A context is not a signal by itself. It describes the market state around an event.

A filter must be explicit. If a detector uses context internally, the event metadata must say whether that context was present, missing, or defaulted.

A template defines behavior, not eligibility. For example, `continuation` and `mean_reversion` are trade-shape hypotheses, not context filters.

A proposal is an experiment, not an edge.

A thesis exists only after validation and promotion.

## Example

```text
Detector: VOL_SHOCK
Event: VOL_SHOCK fired at 10:35
Regime/state: ms_vol_state = 2
Context: spread_bps = 2.5, funding fresh, OI rising
Filter: ms_vol_state >= 1 and spread_bps <= 5
Template: continuation
Proposal: BTCUSDT 5m VOL_SHOCK continuation h12
Thesis: only if validation and promotion pass
Policy: runtime trades, skips, or reduces size based on thesis, execution context, and risk caps
```

## Signal context vs execution context

Use two buckets:

```text
signal_context:
  volatility state
  OI state
  funding state
  basis state
  trend/compression state

execution_context:
  spread_bps
  depth_usd
  expected_cost_bps
  ticker_fresh
  funding_fresh
  open_interest_fresh
  market_state_complete
  is_execution_tradable
```

A signal can be statistically valid and still untradable if execution context is poor.

## Research workflow

Start with one atomic proposal:

```text
detector + template + direction + horizon
```

Then analyze results by context/regime. Add exactly one context filter only if the mechanism justifies it and the next run is predeclared.

Do not try many filters and keep the best result.
