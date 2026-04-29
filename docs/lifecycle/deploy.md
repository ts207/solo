# Deploy

Deploy launches monitor, paper, or live runtime against explicit promoted thesis artifacts. The deploy stage does not discover or promote hypotheses.

## Current CLI Surface

The actual deploy subcommands are:

```bash
edge deploy export --run_id <run_id>
edge deploy bind-config --run_id <run_id>
edge deploy inspect --run_id <run_id>
edge deploy paper-run --config <config.yaml>
edge deploy live-run --config <config.yaml>
edge deploy status --run_id <run_id>
```

Use `paper-run` and `live-run` for runtime launch.

## Canonical Makefile Surface

The root Makefile keeps config binding and runtime launch separate:

```bash
make export RUN_ID=<run_id>
make bind-config RUN_ID=<run_id>
make paper-run CONFIG=project/configs/live_paper_<run_id>.yaml
make live-run CONFIG=project/configs/live_trading_<run_id>.yaml
make deploy-status RUN_ID=<run_id> CONFIG=project/configs/live_paper_<run_id>.yaml
```

`make deploy-paper` remains only as a compatibility alias for `make bind-config`.

## Thesis Export

Runtime consumes:

```text
data/live/theses/<run_id>/promoted_theses.json
```

The thesis store index is:

```text
data/live/theses/index.json
```

Canonical runtime loading must use either:

- `strategy_runtime.thesis_run_id`
- `strategy_runtime.thesis_path`

Implicit latest thesis resolution is disabled on the canonical path.

## Bind Config

Generate a paper config from a promoted run:

```bash
edge deploy bind-config --run_id <run_id>
```

Or through Make:

```bash
make bind-config RUN_ID=<run_id>
```

The generated config includes:

- `runtime_mode`
- freshness streams
- `strategy_runtime.thesis_run_id`
- event detector settings

If `--thesis_path` is passed explicitly, bind-config writes `strategy_runtime.thesis_path` instead of `strategy_runtime.thesis_run_id`. It must never write both.

If no promoted thesis bundle exists, bind-config fails and instructs you to run export first.

## Runtime Config Requirements

`project/scripts/run_live_engine.py` validates:

- `runtime_mode` is `monitor_only`, `simulation`, or `trading`.
- `strategy_runtime` is a mapping.
- `strategy_runtime.load_latest_theses` is not used.
- only one of `strategy_runtime.thesis_path` or `strategy_runtime.thesis_run_id` is set.
- `strategy_runtime.implemented=true` has an explicit thesis input.
- synthetic microstructure defaults are rejected where unsafe.

Trading mode additionally requires environment validation.

## Paper and Live Environment Variables

Trading mode requires:

```bash
export EDGE_ENVIRONMENT=paper        # or production
export EDGE_VENUE=bybit              # or binance
export EDGE_LIVE_CONFIG=<config.yaml>
export EDGE_LIVE_SNAPSHOT_PATH=<snapshot.json>
```

Bybit paper credentials:

```bash
export EDGE_BYBIT_PAPER_API_KEY=<key>
export EDGE_BYBIT_PAPER_API_SECRET=<secret>
```

Binance paper credentials:

```bash
export EDGE_BINANCE_PAPER_API_KEY=<key>
export EDGE_BINANCE_PAPER_API_SECRET=<secret>
export EDGE_BINANCE_PAPER_API_BASE=https://testnet.binancefuture.com
```

Production credentials use the corresponding non-paper variables. Generic `EDGE_API_KEY` and `EDGE_API_SECRET` are accepted as fallbacks in some venue paths.

## Startup Checks

No-credential paper startup certification:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/certify_paper_startup.py
```

Print resolved runtime metadata:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/run_live_engine.py \
  --config project/configs/live_paper_<run_id>.yaml \
  --print_session_metadata
```

Inspect deployment state:

```bash
edge deploy inspect --run_id <run_id> --config <config.yaml>
edge deploy status --run_id <run_id> --config <config.yaml>
```

## Runtime Admission Control

`ThesisStore.from_path(..., strict_live_gate=True)` checks:

- thesis payload schema
- historical trust status
- deployment gate state
- live approval requirements

Live approval required states cannot be used as if they were automatically tradable.

## Stop Conditions

Do not launch paper or live runtime when:

- `promoted_theses.json` is missing.
- the thesis store fails strict loading.
- config has no explicit thesis input.
- `runtime_mode=trading` but `strategy_runtime.implemented` is false.
- required environment variables are missing.
- venue preflight fails.
- deployment status reports missing or stale state.
