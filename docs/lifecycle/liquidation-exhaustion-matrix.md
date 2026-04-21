# Liquidation Exhaustion Matrix

This is the bounded edge-discovery matrix for `LIQUIDATION_EXHAUSTION_REVERSAL` on `BTCUSDT` 5m.

## Execution Boundary

Do not start broad runs until the offline parquet execution issue is fixed.

Allowed before that fix:

- `discover plan`
- dry-run / plan-only
- proposal generation
- compatibility review

Blocked before that fix:

- `discover run`
- validation against newly executed runs
- promotion
- thesis export

## Phase A: Horizon Sweep

Run plans only until execution is repaired:

```bash
make liquidation-exhaustion-plan-matrix

PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h03_base_v1.yaml
PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h12_base_v1.yaml
PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h24_base_v1.yaml
```

| Run | Proposal | Horizon | Purpose |
|---|---|---:|---|
| A1 | `single_event_liq_exhaust_exhaustion_reversal_long_h03_base_v1.yaml` | 3 | fast bounce |
| A2 | `single_event_liq_exhaust_exhaustion_reversal_long_h12_base_v1.yaml` | 12 | medium repair |
| A3 | `single_event_liq_exhaust_exhaustion_reversal_long_h24_base_v1.yaml` | 24 | slower unwind bounce |

Choose one best horizon after execution and validation. Do not carry all three forward unless the evidence shows genuinely different, non-redundant behavior.

## Later Phases

Phase B conditions only the winning Phase A horizon:

- high-vol regime
- non-high-vol regime
- funding stress
- OI flush / OI stress
- liquidity stress
- no extra context filter

Phase C varies template only after Phase B:

- `exhaustion_reversal`
- `repair`
- `mean_reversion`
- `continuation`

Phase D ports only a clear BTC winner to `ETHUSDT`.

## Decision Rules

Minimum keep rules:

- post-cost expectancy > 0
- adequate event count/support
- OOS sign does not flip
- not concentrated in one tiny slice
- not dominated by a sibling thesis
- runtime semantics are clean enough to trade

Kill immediately:

- `n_events < 20`
- negative post-cost expectancy
- OOS sign flip
- one-month or one-burst dependence
- survival only without realistic costs
- duplicate of a stronger sibling
- runtime cannot use it cleanly

Rank surviving candidates by post-cost expectancy, stability, support, uniqueness, then runtime simplicity.
