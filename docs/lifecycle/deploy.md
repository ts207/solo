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
edge deploy status
```

Use `list-theses` and `inspect-thesis` before startup to confirm that a thesis batch exists and that its deployment states match the intended mode.

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

The thesis batch must contain at least one thesis whose deployment state is compatible with paper usage, typically `paper_only` or `live_enabled`.

### Live launch requirement

The thesis batch must contain at least one thesis with `deployment_state = live_enabled`.

This means that not every promoted batch is legally deployable in live mode.

## Runtime config contract

Key config rules:
- `runtime_mode` must be `monitor_only` or `trading`
- `strategy_runtime.implemented=true` requires an explicit thesis source
- only one thesis source should be set at a time, typically `thesis_run_id` or `thesis_path`

A practical nuance:

`edge deploy ... --run_id <run_id>` overrides `strategy_runtime.thesis_run_id` for that launch session in memory. It does not rewrite the YAML config on disk.

## Important nuance: default paper config may be monitor-only

Not every paper config is a thesis-trading config.

The checked-in paper profiles include both:
- monitor-oriented paper configs,
- thesis-bound paper trading templates.

That distinction matters. Launching paper mode with a monitor-only config does not automatically create a thesis-driven paper trader.

## Standard bring-up

Typical sequence:

```bash
# 1. verify theses
edge deploy list-theses
edge deploy inspect-thesis --run_id <run_id>

# 2. bind a config
edge deploy bind-config --run_id <run_id>

# 3. optional startup certification
python project/scripts/certify_paper_startup.py --config project/configs/live_paper_<run_id>.yaml

# 4. launch
edge deploy paper --run_id <run_id> --config project/configs/live_paper_<run_id>.yaml
```

For a detailed operational version, see [../operator/runbook.md](../operator/runbook.md).

## Environment requirements

The runtime performs environment and config checks before allowing trading mode.

Representative environment variables include:
- `EDGE_ENVIRONMENT`
- `EDGE_VENUE`
- `EDGE_LIVE_CONFIG`
- `EDGE_LIVE_SNAPSHOT_PATH`
- venue-specific credentials such as `EDGE_BYBIT_*` or `EDGE_BINANCE_*`

Treat missing environment state as a deployment blocker, not as something to work around in code.

## What happens at runtime startup

When the runner starts, it typically:

1. loads the thesis batch from path or run id,
2. registers or hydrates thesis runtime state,
3. reconciles the current thesis batch against persisted prior-batch metadata,
4. constructs venue, data, OMS, and risk components,
5. begins the market-data-driven runtime loop.

The thesis reconciliation step is important. It exists to detect dangerous situations such as:
- removed theses that were previously active,
- downgraded deployment states,
- unexpected batch changes across restarts.

## The online decision loop

The runtime’s conceptual loop is:

```text
market data → event detection → context build → thesis match / decision → order planning → OMS → health and kill-switch checks
```

The runtime is thesis-aware. It does not simply trade every detector signal. It uses the thesis store, context, and runtime gating to decide whether a signal is actually actionable.

## Shared policy surfaces

Risk and admission policy is deliberately shared across runtime and execution-oriented components.

Important policy modules include:
- `project/portfolio/admission_policy.py`
- `project/portfolio/thesis_overlap.py`
- `project/portfolio/risk_budget.py`
- `project/portfolio/sizing.py`

That shared layer is there to reduce drift between what research/execution assumes and what live runtime actually enforces.

## Failure and fail-closed behavior

Deploy is intentionally fail-closed in several scenarios:
- thesis package missing,
- deployment state incompatible with requested mode,
- runtime config invalid,
- environment incomplete for trading mode,
- thesis reconciliation degrades into an unsafe state,
- kill-switch conditions trigger during operation.

When the kill-switch trips in trading mode, the runtime is designed to block or terminate trading actions rather than continue in a degraded state.

## Operational rules

- Do not bypass `bind-config` for normal paper startup.
- Do not run live mode against paper-only theses.
- Do not treat monitor-only startup as equivalent to thesis-driven trading startup.
- If thesis-store schema or reconciliation behavior changes, update export, loader, reconciliation logic, and tests together.
- Investigate startup reconciliation issues before forcing runtime through them.

## What deploy proves

A successful deploy proves more than “the process started.” It proves that:
- the thesis package exists,
- the deployment state is compatible,
- the config is coherent,
- the environment is sufficient,
- the runtime can load and reconcile the thesis batch,
- the engine can enter its decision loop without failing the startup gates.

For concrete operator steps and restart/failure procedures, see [../operator/runbook.md](../operator/runbook.md).
