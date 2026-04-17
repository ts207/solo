# Deploy stage

## Scope

The deploy stage is the runtime execution surface. In the CLI it is intentionally narrow and routes through the live-engine launcher.

CLI surface from `project/cli.py`:

- `edge deploy list-theses`
- `edge deploy inspect-thesis --run_id <run_id>`
- `edge deploy paper --run_id <run_id> --config <config.yaml>`
- `edge deploy live --run_id <run_id> --config <config.yaml>`
- `edge deploy status`

## Checked-in runtime configs

The repo currently ships three notable runtime config files:

- `project/configs/live_paper.yaml` — monitor-only paper profile
- `project/configs/live_production.yaml` — monitor-only production profile
- `project/configs/live_paper_btc_thesis_v1.yaml` — thesis-bound paper trading template with `strategy_runtime.implemented=true` and a placeholder `thesis_run_id`

`project/configs/golden_workflow.yaml` is referenced by these configs but is not itself a live-engine config.

## Live-engine launcher

The launcher implementation lives in `project/scripts/run_live_engine.py`.

## Run ID semantics in `edge deploy`

`edge deploy inspect-thesis --run_id <run_id>` reads the exported package for that run directly.

`edge deploy paper --run_id <run_id>` and `edge deploy live --run_id <run_id>` use `run_id` in two places:

- deployment gating: the CLI confirms that `data/live/theses/<run_id>/promoted_theses.json` exists and that the batch contains the right deployment states
- runtime selection: the launcher forwards `run_id` into the live engine by overriding `strategy_runtime.thesis_run_id` and forcing `strategy_runtime.implemented=true`

The config file still matters. `runtime_mode` remains whatever the config declares, and `strategy_runtime.thesis_path` inside the file still wins when you launch the engine directly without the deploy wrapper. If the config is monitor-only, `edge deploy ... --run_id <run_id>` still runs a monitor-only session; use a thesis-ready trading config when you want thesis-driven paper or live execution.

### Config contract

`load_live_engine_config()` enforces:

- top-level config must be a mapping
- `runtime_mode` must be `monitor_only` or `trading`
- `strategy_runtime.load_latest_theses` is not supported
- exactly zero or one of `strategy_runtime.thesis_path` and `strategy_runtime.thesis_run_id` may be set
- `strategy_runtime.implemented=true` requires an explicit thesis source

### Runtime environment validation

`validate_live_runtime_environment()` enforces stronger checks in `trading` mode:

- environment name must resolve from config/path naming
- `EDGE_ENVIRONMENT` must match the resolved environment
- `EDGE_VENUE` must be `binance` or `bybit`
- `EDGE_LIVE_CONFIG` must point at the active config
- `EDGE_LIVE_SNAPSHOT_PATH` must be set and aligned
- venue API credentials must exist for paper or production mode

In `monitor_only` mode, the validation surface is lighter.

## Live package consumption

The runtime stack consumes exported promoted theses rather than raw promotion tables.

Relevant modules:

- `project/live/thesis_store.py`
- `project/live/thesis_specs.py`
- `project/live/runner.py`
- `project/live/retriever.py`
- `project/live/order_planner.py`
- `project/live/kill_switch.py`
- `project/live/health_checks.py`

The live thesis store is indexed through `data/live/theses/index.json` and run-scoped packages under `data/live/theses/<run_id>/`.

## Portfolio and overlap policy

Shared portfolio-admission logic now lives in `project/portfolio/admission_policy.py`. Both engine and live runtime consume that shared policy instead of maintaining a live-owned copy.

Related modules:

- `project/portfolio/thesis_overlap.py`
- `project/portfolio/risk_budget.py`
- `project/portfolio/sizing.py`
- `project/engine/risk_allocator.py`
- `project/live/retriever.py`

## Live/paper entry points outside `edge deploy`

There are also direct entry points worth knowing:

- `edge-live-engine`
- `python -m project.scripts.run_live_engine`
- replay/runtime utilities under `project/runtime/` and `project/pipelines/runtime/`

## Canonical commands

```bash
edge deploy list-theses
edge deploy inspect-thesis --run_id <run_id>
edge deploy paper --run_id <run_id> --config project/configs/live_paper.yaml
edge deploy live  --run_id <run_id> --config project/configs/live_production.yaml
edge deploy status
```

## Operational cautions

- `edge deploy ... --run_id <run_id>` now overrides `strategy_runtime.thesis_run_id` for that launch, but it does not change `runtime_mode`; a monitor-only config stays monitor-only.
- Do not treat bundle-only export as the default deployment path; the CLI text explicitly labels that as compatibility behavior.
- Do not run `trading` mode without a validated config, a non-placeholder thesis source, and a full environment-variable check.
- If you change thesis-store schemas, update `live_export.py`, live-store readers, and the tests together.
