# Operator Runbook

**System:** Edge paper-trading engine  
**Last certified:** 2026-04-16  
**Baseline tag:** `paper-engine-startup-certified`

---

## Prerequisites

### Environment

```bash
# Python 3.11+ (3.12 tested and working)
# Active venv
source .venv/bin/activate

# Verify CLI resolves
edge --help
```

### Required env vars for paper trading

```bash
export EDGE_ENVIRONMENT=paper
export EDGE_VENUE=binance                             # or bybit
export EDGE_BINANCE_PAPER_API_BASE=https://testnet.binancefuture.com
export EDGE_BINANCE_PAPER_API_KEY=<testnet api key>
export EDGE_BINANCE_PAPER_API_SECRET=<testnet api secret>
export EDGE_LIVE_CONFIG=<path to bound config yaml>
export EDGE_LIVE_SNAPSHOT_PATH=artifacts/live_state_paper_btc_thesis.json
```

All six must be set before `edge deploy paper` will pass the pre-flight check.

---

## Standard bring-up sequence

### Step 1 — Confirm a promoted thesis batch exists

```bash
edge deploy list-theses
edge deploy inspect-thesis --run_id <RUN_ID>
```

Look for `deployment_state=paper_only` and `status=pending_blueprint`.

### Step 2 — Bind the config

```bash
edge deploy bind-config --run_id <RUN_ID>
# Writes: project/configs/live_paper_<RUN_ID>.yaml
```

The bound config is a clone of `project/configs/live_paper_btc_thesis_v1.yaml`
with `thesis_run_id` replaced. Do not edit it manually after binding.

### Step 3 — Run startup certification (no credentials needed)

```bash
python project/scripts/certify_paper_startup.py \
  --config project/configs/live_paper_<RUN_ID>.yaml \
  --snapshot_path artifacts/live_state_paper_btc_thesis.json \
  --out artifacts/paper_startup_certification.json
```

All 7 checks must pass before proceeding. If any fail, do not start the engine.

Key checks and what they verify:

| Check | Failure means |
|---|---|
| `config_load` | YAML is malformed or path is wrong |
| `session_metadata` | Config fields are missing or invalid |
| `runner_construction` | Thesis store missing, reconciliation failed, or import error |
| `thesis_details` | No theses loaded (export step failed or wrong run_id) |
| `metrics_snapshot` | Path not writable |
| `state_snapshot` | `artifacts/` directory not writable |
| `deploy_run_summary` | Same |

### Step 4 — Set env vars and start the paper engine

```bash
export EDGE_ENVIRONMENT=paper
export EDGE_VENUE=binance
export EDGE_BINANCE_PAPER_API_BASE=https://testnet.binancefuture.com
export EDGE_BINANCE_PAPER_API_KEY=<key>
export EDGE_BINANCE_PAPER_API_SECRET=<secret>
export EDGE_LIVE_CONFIG=project/configs/live_paper_<RUN_ID>.yaml
export EDGE_LIVE_SNAPSHOT_PATH=artifacts/live_state_paper_btc_thesis.json

edge deploy paper \
  --run_id <RUN_ID> \
  --config project/configs/live_paper_<RUN_ID>.yaml
```

---

## Shutdown

Send SIGINT (Ctrl-C) or SIGTERM. The runner handles both:

```
loop.add_signal_handler(sig, lambda: asyncio.create_task(runner.stop()))
```

`runner.stop()` → `_shutdown_runtime()`:
1. Stops the data manager (closes WS connections)
2. Cancels all background tasks
3. Closes the order manager
4. Persists the final runtime metrics snapshot

**Do not kill -9** unless absolutely necessary. The final snapshot write on shutdown is what makes restart safe.

---

## Restart from snapshot

The `snapshot_path` in the config is a **write-only path** during a session — the engine writes its state there continuously, but does not load it on restart. On restart, the engine:

1. Creates a fresh `LiveStateStore` (empty account/position state)
2. Reconciles against the live exchange via `account_snapshot_fetcher` (requires credentials)
3. The snapshot file is overwritten with fresh state

The reconciliation audit log at `live/persist/thesis_reconciliation.json` IS loaded on restart. The reconciler detects whether the current thesis batch is the same as the previous run (`unchanged=N`) or different (`added=N`).

**Expected restart log output:**
```
Thesis batch reconciliation: previous=<RUN_ID> current=<RUN_ID> added=0 unchanged=1 ...
```

If this shows `added=1 unchanged=0` on what should be a same-batch restart, check that `live/persist/thesis_batch_metadata.json` was not deleted.

---

## Failure handling reference

### Kill-switch triggers

The engine self-triggers a kill switch under these conditions:

| Trigger | Condition | Behavior |
|---|---|---|
| `EXCHANGE_DISCONNECT` | WS max retries (5) exhausted; exponential backoff 1s→32s | Cancels orders, flattens positions, shuts down |
| `ACCOUNT_SYNC_LOSS` | Account snapshot fetch fails ≥4 times consecutively (15s interval) | Same |
| `STALE_DATA` | Feed goes stale for >60s (`stale_threshold_sec`) | Same |
| `MICROSTRUCTURE_BREAKDOWN` | Pre-trade health gate fails | Blocks new orders; does not shutdown |
| `EXCESSIVE_DRAWDOWN` | Portfolio drawdown exceeds cap | Same |
| `FEATURE_DRIFT` | PSI drift >0.25 on tier-1 features | Same |
| `MANUAL` | Operator-triggered | Same |

Kill-switch state is persisted to `artifacts/live_state_paper_btc_thesis.json` with `kill_switch.is_active=true`.

### WS reconnect behavior

```
max_retries: 5
backoff: 1s * 2^n + jitter (0–1s)
  attempt 1: ~1s delay
  attempt 2: ~2s delay
  attempt 3: ~4s delay
  attempt 4: ~8s delay
  attempt 5: ~16s delay
on exhaustion: triggers EXCHANGE_DISCONNECT kill switch
stable-reset: retry count resets if connected for >60s
```

### Thesis batch reconciliation failures

If the reconciler fails to load the previous batch metadata:

1. The kill switch is set to `thesis_batch_reconciliation_degraded`
2. In `runtime_mode=trading` with `implemented=true`, startup is **aborted**
3. Operator must investigate `live/persist/thesis_batch_metadata.json`

Common cause: `live/persist/` was wiped without clearing the reference. Fix: delete `live/persist/thesis_batch_metadata.json` to force a clean-session start (reconciler treats missing metadata as "no previous batch").

---

## Research lifecycle → deploy pipeline

Full pipeline from research to running paper trade:

```
1. edge discover run --proposal spec/proposals/<PROPOSAL>.yaml
   → run_id assigned (e.g. productive_golden_path_20260416T041539Z_c7e116bafd)

2. edge validate run --run_id <RUN_ID>
   → validates candidates

3. edge promote run --run_id <RUN_ID> --symbols BTCUSDT
   → promotes validated candidates to theses

4. edge promote export --run_id <RUN_ID>
   → writes data/live/theses/<RUN_ID>/promoted_theses.json

5. edge deploy bind-config --run_id <RUN_ID>
   → writes project/configs/live_paper_<RUN_ID>.yaml

6. python project/scripts/certify_paper_startup.py --config ... --out ...
   → all 7 checks must pass

7. edge deploy paper --run_id <RUN_ID> --config ...
   → starts paper engine (requires env vars)
```

---

## File locations

| Artifact | Path |
|---|---|
| Promoted theses | `data/live/theses/<RUN_ID>/promoted_theses.json` |
| Bound config | `project/configs/live_paper_<RUN_ID>.yaml` |
| State snapshot (written during session) | `artifacts/live_state_paper_btc_thesis.json` |
| Runtime metrics snapshot | `artifacts/live_runtime_metrics_paper_btc_thesis.json` |
| Thesis reconciliation audit | `live/persist/thesis_reconciliation.json` |
| Thesis batch metadata (restart continuity) | `live/persist/thesis_batch_metadata.json` |
| Runtime alerts log | `artifacts/live_runtime_alerts_paper_btc_thesis.jsonl` |
| Startup certification manifest | `artifacts/paper_startup_certification.json` |
| Deploy run summary | `artifacts/deploy_run_summary.json` |

`artifacts/` and `live/persist/` are gitignored — they are runtime output, not source.
