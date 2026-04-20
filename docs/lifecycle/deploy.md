# Deploy stage

The deploy stage launches the runtime against a promoted thesis batch.

It answers this question:

**Can the runtime safely load this thesis batch and operate in paper or live mode under the configured environment, runtime mode, and risk gates?**

## CLI surface

```bash
edge deploy list-theses
edge deploy inspect-thesis  --run_id <run_id>
edge deploy bind-config     --run_id <run_id>
edge deploy paper           --run_id <run_id> --config <config.yaml>
edge deploy live            --run_id <run_id> --config <config.yaml>
edge deploy status          --run_id <run_id>
edge deploy export          --run_id <run_id>
```

Use `list-theses` and `inspect-thesis` before startup to confirm a thesis batch exists and that its deployment states match the intended mode.

Use `bind-config` to create a run-bound paper config instead of editing templates manually.

## What deploy consumes

Deploy requires two things:

1. a promoted thesis batch under `data/live/theses/<run_id>/`,
2. a runtime config describing how to launch the engine.

The deploy CLI verifies thesis-batch existence before handing off to the live engine launcher.

## Code path

```text
project/cli.py
  → project/scripts/run_live_engine.py
      → project/live/runner.py
          → thesis_store.py
          → thesis_reconciliation.py
          → event detection / context build / decision / OMS / kill-switch
```

The CLI is the gatekeeper. `run_live_engine.py` normalizes config and environment. `project/live/runner.py` owns the actual runtime loop.

## Paper mode versus live mode

The stage supports both `paper` and `live`, but deployment state checks differ.

### Paper launch requirement

The thesis batch must contain at least one thesis whose deployment state is compatible with paper usage — typically `paper_only` or `live_enabled`.

### Live launch requirement

The thesis batch must contain at least one thesis with `deployment_state = live_enabled`.

Not every promoted batch is legally deployable in live mode.

## Runtime config contract

Key config rules:
- `runtime_mode` must be `monitor_only` or `trading`
- `strategy_runtime.implemented=true` requires an explicit thesis source
- only one thesis source should be set at a time: typically `thesis_run_id` or `thesis_path`

`edge deploy ... --run_id <run_id>` overrides `strategy_runtime.thesis_run_id` for that launch session in memory — it does not rewrite the YAML config on disk.

## Important nuance: default paper config may be monitor-only

Not every paper config is a thesis-trading config. The checked-in paper profiles include both monitor-oriented configs and thesis-bound trading configs. Launching paper mode with a monitor-only config does not create a thesis-driven paper trader.

## Standard bring-up

```bash
# 1. verify theses
edge deploy list-theses
edge deploy inspect-thesis --run_id <run_id>

# 2. bind a config
edge deploy bind-config --run_id <run_id>

# 3. optional startup certification
python project/scripts/certify_paper_startup.py \
  --config project/configs/live_paper_<run_id>.yaml

# 4. launch
edge deploy paper \
  --run_id <run_id> \
  --config project/configs/live_paper_<run_id>.yaml
```

For the full operational sequence with env vars, failure handling, and restart procedures, see [../operator/runbook.md](../operator/runbook.md).

## Environment requirements

```bash
export EDGE_ENVIRONMENT=paper         # or live
export EDGE_VENUE=binance             # or bybit
export EDGE_LIVE_CONFIG=<path>        # bound config yaml
export EDGE_LIVE_SNAPSHOT_PATH=<path> # state snapshot json
# Plus venue-specific API key/secret vars
```

Treat missing environment state as a deployment blocker. Do not work around it in config.

## What happens at runtime startup

When the runner starts:

1. loads the thesis batch from path or run id,
2. registers or hydrates thesis runtime state,
3. reconciles the current thesis batch against persisted prior-batch metadata,
4. constructs venue, data, OMS, and risk components,
5. begins the market-data-driven runtime loop.

The thesis reconciliation step detects dangerous situations such as removed theses that were previously active, downgraded deployment states, or unexpected batch changes across restarts.

## The online decision loop

```text
market data → event detection → context build → thesis match / decision → order planning → OMS → health and kill-switch checks
```

The runtime is thesis-aware. It does not trade every detector signal — it gates each signal through the thesis store, context, and runtime admission policy.

## Runtime decision path (hardened)

The live engine decision path runs through these modules in sequence:

| Module | Responsibility |
|--------|---------------|
| `project/live/decision_ranker.py` | Rank thesis matches by expected utility: EV × regime reliability × fill probability |
| `project/live/trade_valuator.py` | Estimate fill probability, win probability, edge confidence, net utility |
| `project/live/sizing_allocator.py` | Compute position size with overlap, slippage, confidence, and participation cap adjustments |
| `project/live/order_planner.py` | Convert accepted valuations into executable trade intents |
| `project/live/contradiction_model.py` | Assess signal contradictions and apply penalty bps |
| `project/live/regime_reliability.py` | Score regime match between thesis and current market context |
| `project/live/signal_monitor.py` | Monitor signal silence and fill calibration; emit warnings and alerts |
| `project/portfolio/` | Shared admission, overlap, sizing, and risk budget policy |

These modules are separately testable and the decision path is instrumented. The runtime emits a `signal_monitor` block in its metrics snapshot.

## Shared policy surfaces

Risk and admission policy is shared between runtime and research:

- `project/portfolio/admission_policy.py`
- `project/portfolio/thesis_overlap.py`
- `project/portfolio/risk_budget.py`
- `project/portfolio/sizing.py`

That shared layer reduces drift between what research/execution assumes and what live runtime enforces.

## Failure and fail-closed behavior

Deploy is intentionally fail-closed:
- thesis package missing,
- deployment state incompatible with requested mode,
- runtime config invalid,
- environment incomplete for trading mode,
- thesis reconciliation degrades into an unsafe state,
- kill-switch conditions trigger during operation.

When the kill-switch trips in trading mode, the runtime blocks or terminates trading rather than continuing in a degraded state.

## Operational rules

- Do not bypass `bind-config` for normal paper startup.
- Do not run live mode against paper-only theses.
- Do not treat monitor-only startup as equivalent to thesis-driven trading startup.
- If thesis-store schema or reconciliation behavior changes, update export, loader, reconciliation logic, and tests together.
- Investigate startup reconciliation issues before forcing runtime through them.

## What deploy proves

A successful deploy proves:
- the thesis package exists and is coherent,
- the deployment state is compatible with the requested mode,
- the config is valid,
- the environment is sufficient,
- the runtime can load and reconcile the thesis batch,
- the engine can enter its decision loop without failing startup gates.

For concrete operator steps and restart/failure procedures: [../operator/runbook.md](../operator/runbook.md)
