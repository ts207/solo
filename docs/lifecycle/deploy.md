# Deploy stage

## CLI

```bash
edge deploy list-theses
edge deploy inspect-thesis  --run_id <run_id>
edge deploy bind-config     --run_id <run_id>
edge deploy paper           --run_id <run_id> --config <config.yaml>
edge deploy live            --run_id <run_id> --config <config.yaml>
edge deploy status
```

---

## Environment setup

Six env vars are required before `edge deploy paper|live` passes pre-flight:

```bash
export EDGE_ENVIRONMENT=paper          # or: live
export EDGE_VENUE=bybit                # or: binance
export EDGE_LIVE_CONFIG=<path>         # path to the bound config yaml
export EDGE_LIVE_SNAPSHOT_PATH=<path>  # path to the state snapshot json

# Bybit:
export EDGE_BYBIT_PAPER_API_KEY=<key>
export EDGE_BYBIT_PAPER_API_SECRET=<secret>

# Binance:
export EDGE_BINANCE_PAPER_API_BASE=https://testnet.binancefuture.com
export EDGE_BINANCE_PAPER_API_KEY=<key>
export EDGE_BINANCE_PAPER_API_SECRET=<secret>
```

---

## Standard bring-up

```bash
# 1. Verify a promoted thesis batch exists
edge deploy list-theses
edge deploy inspect-thesis --run_id <run_id>   # look for deployment_state=paper_only

# 2. Bind a config
edge deploy bind-config --run_id <run_id>
# Writes: project/configs/live_paper_<run_id>.yaml

# 3. Run startup certification (no credentials needed)
PYTHONPATH=. python3 project/scripts/certify_paper_startup.py

# 4. Set env vars and deploy
export EDGE_LIVE_CONFIG=project/configs/live_paper_<run_id>.yaml
export EDGE_LIVE_SNAPSHOT_PATH=artifacts/live_state_paper_btc_thesis.json
edge deploy paper --run_id <run_id> --config $EDGE_LIVE_CONFIG
```

---

## Code path

The deploy CLI routes to `project/scripts/run_live_engine.py`.

The live runtime loop:
```
project/live/runner.py
  ├─ thesis_reconciliation.py    ← load and reconcile thesis batch on startup
  ├─ event_detector.py           ← detect live events from market data
  ├─ context_builder.py          ← build market context for decision
  ├─ decision.py                 ← decide trade intent from event + thesis + context
  ├─ order_planner.py            ← build order plan from trade intent
  ├─ oms.py                      ← submit orders to venue
  └─ kill_switch.py              ← enforce kill conditions
```

---

## Config contract

`runtime_mode` must be `monitor_only` or `trading`. A monitor-only config stays monitor-only even when launched with `--run_id` — the CLI does not override `runtime_mode`.

`edge deploy ... --run_id <run_id>` overrides `strategy_runtime.thesis_run_id` in the config for that launch. It does not rewrite the config file.

`strategy_runtime.implemented=true` requires an explicit thesis source (`thesis_path` or `thesis_run_id`). Only one may be set.

---

## Checked-in runtime configs

| File | Role |
|------|------|
| `project/configs/live_paper.yaml` | Monitor-only paper profile |
| `project/configs/live_production.yaml` | Monitor-only production profile |
| `project/configs/live_paper_btc_thesis_v1.yaml` | Thesis-bound paper trading template |

Bound configs for specific runs are written by `edge deploy bind-config` as `project/configs/live_paper_<run_id>.yaml`.

---

## Portfolio and risk

Shared admission logic lives in `project/portfolio/admission_policy.py`. Both the backtest engine and the live runtime consume this shared policy.

Relevant modules:
- `project/portfolio/thesis_overlap.py` — overlap detection
- `project/portfolio/risk_budget.py` — risk allocation
- `project/portfolio/sizing.py` — position sizing
- `project/engine/risk_allocator.py` — execution-side risk
- `project/live/retriever.py` — thesis retrieval with admission filter

---

## Operational rules

- Do not run `trading` mode without a validated config, a non-placeholder thesis source, and a full env-var check
- Do not bypass `bind-config`; it clones the template and injects thesis_run_id safely
- If thesis-store schemas change, update `live_export.py`, live-store readers, and related tests together
- The reconciliation bug fix (2026-04-16): `reconcile_thesis_batch` previously resolved `persist_dir.parent.parent` to `.` on second startup, causing a kill-switch on clean session restart — this is fixed
