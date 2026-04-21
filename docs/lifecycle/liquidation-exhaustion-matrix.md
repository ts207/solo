# Liquidation Exhaustion Matrix

This is the bounded edge-discovery matrix for `LIQUIDATION_EXHAUSTION_REVERSAL` on `BTCUSDT` 5m.

## Execution Boundary

Do not start broad runs from this family. The offline finalization path now
keeps evaluated-but-rejected hypotheses in `evaluation_results.parquet`, so
the Phase A liquidation-exhaustion runs may execute end to end.

Allowed:

- `discover plan`
- `discover run` for the bounded Phase A and Phase B liquidation-exhaustion
  proposals
- result review from `evaluation_results.parquet`, `evaluated_hypotheses.parquet`,
  and `discovery_quality_summary.json`

Still blocked until a Phase B bridge survivor is clear:

- promotion
- thesis export
- broad event/template/symbol expansion

## Phase A: Horizon Sweep

Run:

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

Latest execution snapshot:

| Run | Run ID | Events | After-cost expectancy | t-stat | Robustness | Bridge candidates |
|---|---|---:|---:|---:|---:|---:|
| A1 | `single_event_liq_exhaust_20260421T100537Z_6f3334cb79` | 686 | -1.064988 bps | 0.8225 | 0.3848 | 0 |
| A2 | `single_event_liq_exhaust_20260421T100700Z_82ea9e71d4` | 686 | -0.562739 bps | 0.5987 | 0.4098 | 0 |
| A3 | `single_event_liq_exhaust_20260421T100821Z_c6f564def5` | 685 | 2.229247 bps | 1.2198 | 0.4077 | 0 |

Choose one best horizon after execution and validation. Do not carry all three forward unless the evidence shows genuinely different, non-redundant behavior.

## Later Phases

Phase A decision:

- Drop h03 and h12 for now because both are negative after costs.
- Freeze h24 as the only Phase B branch because it is directionally positive
  after costs but too weak unconditioned to emit a bridge candidate.

## Phase B: H24 Context Conditioning

Run plans first:

```bash
make liquidation-exhaustion-plan-matrix PHASE=B

PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h24_high_vol_v1.yaml
PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h24_low_vol_control_v1.yaml
```

| Run | Proposal | Context | Purpose |
|---|---|---|---|
| B1 | `single_event_liq_exhaust_exhaustion_reversal_long_h24_high_vol_v1.yaml` | `vol_regime=high` | high-vol stress branch |
| B6 | `single_event_liq_exhaust_exhaustion_reversal_long_h24_low_vol_control_v1.yaml` | `vol_regime=low` | negative control |

Latest execution snapshot:

| Run | Run ID | Events | After-cost expectancy | t-stat | Robustness | Bridge candidates | Decision |
|---|---|---:|---:|---:|---:|---:|---|
| B1 | `single_event_liq_exhaust_20260421T102546Z_1a9c6494ef` | 288 | -1.866159 bps | 0.0250 | 0.4106 | 0 | reject |
| B6 | `single_event_liq_exhaust_20260421T103141Z_ccb04aff6a` | 103 | 16.682463 bps | 1.9368 | 0.4676 | 0 | exploratory only |

Phase B result:

- The intended high-vol stress branch weakens the h24 rebound and fails after
  costs.
- The low-vol control is stronger than the base result, but still emits zero
  bridge candidates because `min_t_stat=2.0` is not met.
- Do not validate, promote, or export B6 from this run. Treat it as a new
  bounded hypothesis seed, not as a bridge survivor.

Blocked until proposal/runtime support is explicit:

- severity: `severity_bucket=extreme` passes proposal planning but evaluates as
  `unknown_context_mapping`.
- funding stress: `funding_bps=negative` and `funding_bps=extreme_negative`
  pass proposal planning but evaluate as `unknown_context_mapping`.
- liquidity stress: `ms_spread_state=wide` passes proposal planning but evaluates
  as `missing_context_state_column`.
- OI flush / OI stress is not currently a first-class context label accepted by
  the proposal validator.

Phase C varies template only after Phase B:

- `exhaustion_reversal`
- `mean_reversion`
- `continuation`

Literal `repair` is blocked in the current legal template surface. Registered
repair-like templates such as `basis_repair` and `desync_repair` target
`basis_repair_h`, which the current event-response evaluator rejects as an
unsupported label target for this branch.

## Phase C: Low-Vol H24 Template Comparison

Run:

```bash
make liquidation-exhaustion-plan-matrix PHASE=C

PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h24_low_vol_phase_c_v1.yaml
PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_mean_reversion_long_h24_low_vol_v1.yaml
PYTHONPATH=. ./.venv/bin/python -m project.cli discover plan --proposal spec/proposals/single_event_liq_exhaust_continuation_long_h24_low_vol_v1.yaml
```

| Run | Proposal | Template | Purpose |
|---|---|---|---|
| C1 | `single_event_liq_exhaust_exhaustion_reversal_long_h24_low_vol_phase_c_v1.yaml` | `exhaustion_reversal` | reproduce current low-vol branch |
| C3 | `single_event_liq_exhaust_mean_reversion_long_h24_low_vol_v1.yaml` | `mean_reversion` | test mild mean-reversion interpretation |
| C4 | `single_event_liq_exhaust_continuation_long_h24_low_vol_v1.yaml` | `continuation` | falsification branch |

Latest execution snapshot:

| Run | Run ID | Events | After-cost expectancy | t-stat | Robustness | Bridge candidates | Decision |
|---|---|---:|---:|---:|---:|---:|---|
| C1 | `single_event_liq_exhaust_20260421T104428Z_f3e5ab3fb7` | 103 | 16.682463 bps | 1.9368 | 0.4676 | 0 | reject |
| C3 | `single_event_liq_exhaust_20260421T104541Z_301327817e` | 103 | 10.543119 bps | 1.2998 | 0.4640 | 0 | reject |
| C4 | `single_event_liq_exhaust_20260421T104654Z_9f74f087de` | 103 | 16.682463 bps | 1.9368 | 0.4676 | 0 | reject |

Phase C result:

- `exhaustion_reversal` reproduces B6 exactly, but still fails `min_t_stat=2.0`
  and emits zero bridge candidates.
- `mean_reversion` is weaker than the low-vol baseline.
- `continuation` does not beat or cleanly falsify the rebound interpretation; it
  matches the same metrics as `exhaustion_reversal` and still fails the bridge.
- Under current gates and runtime semantics, kill this family as non-promotable.
  Do not validate, promote, export, port to ETH, or widen templates from this
  branch.

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
