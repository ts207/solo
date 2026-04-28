# Tiny-Live Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix tiny-live runbook, perform artifact-chain rehearsal, and add E2E admission smoke test.

**Architecture:** Operational rehearsal to verify the safety gates for real capital deployment.

**Tech Stack:** Python, Makefile, Pytest.

---

### Task 1: Fix Runbook Launch Command

**Files:**
- Modify: `docs/operator/tiny_live_runbook.md`

- [ ] **Step 1: Update launch command and add warning**

Change section 6 from:
```bash
edge deploy live-run --config project/configs/live_trading_<run_id>.yaml
```
to include:
```bash
# REAL CREDENTIALS REQUIRED
export EDGE_BYBIT_API_KEY=<real_key>
export EDGE_BYBIT_API_SECRET=<real_secret>

edge deploy live-run --config project/configs/live_trading_<run_id>.yaml
```
and add:
```text
Only run this after bind-config has succeeded with runtime_mode=trading and the generated config has been inspected.
```

- [ ] **Step 2: Run hygiene check**
```bash
make check-hygiene
```

- [ ] **Step 3: Commit**

```bash
git add docs/operator/tiny_live_runbook.md
git commit -m "docs: fix tiny-live launch command and add safety warning"
```

### Task 2: Artifact-Chain Rehearsal

**Files:**
- Create: `data/reports/validation/rehearsal_run/forward_confirmation.json`
- Create: `data/reports/paper/rehearsal_thesis/paper_quality_summary.json`
- Create: `data/reports/approval/rehearsal_thesis/live_approval.json`
- Create: `data/live/theses/rehearsal_run/promoted_theses.json`

- [ ] **Step 1: Setup rehearsal directories**
```bash
mkdir -p data/reports/validation/rehearsal_run
mkdir -p data/reports/paper/rehearsal_thesis
mkdir -p data/reports/approval/rehearsal_thesis
mkdir -p data/live/theses/rehearsal_run
```

- [ ] **Step 2: Create OOS forward confirmation artifact**
```json
{
  "schema_version": "oos_frozen_thesis_replay_v1",
  "run_id": "rehearsal_run",
  "method": "oos_frozen_thesis_replay_v1",
  "metrics": {
    "status": "success",
    "mean_return_net_bps": 5.2,
    "t_stat_net": 2.1,
    "trade_count": 45
  }
}
```

- [ ] **Step 3: Create paper quality summary artifact**
```json
{
  "schema_version": "paper_quality_v1",
  "thesis_id": "rehearsal_thesis",
  "metrics": {
    "trade_count": 35,
    "mean_net_bps": 4.1,
    "max_drawdown_bps": 120,
    "paper_gate_ready": true
  }
}
```

- [ ] **Step 4: Create live approval artifact**
```json
{
  "schema_version": "live_approval_v1",
  "thesis_id": "rehearsal_thesis",
  "approved_state": "live_enabled",
  "approved_by": "operator",
  "approved_at_utc": "2026-04-27T00:00:00Z",
  "cap_profile_id": "tiny_live_v1",
  "risk_acknowledgement": true
}
```

- [ ] **Step 5: Create promoted theses with tiny caps**
```json
{
  "run_id": "rehearsal_run",
  "theses": [
    {
      "thesis_id": "rehearsal_thesis",
      "deployment_state": "live_enabled",
      "cap_profile": {
        "max_notional": 50.0,
        "max_position_notional": 50.0,
        "max_daily_loss": 10.0,
        "max_active_orders": 3,
        "max_active_positions": 1
      }
    }
  ]
}
```

- [ ] **Step 6: Run bind-config rehearsal**
```bash
make bind-config RUN_ID=rehearsal_run DATA_ROOT=data RUNTIME_MODE=trading
```

- [ ] **Step 7: Inspect generated YAML**
```bash
cat project/configs/live_trading_rehearsal_run.yaml
```

### Task 3: Add E2E Admission Smoke Test

**Files:**
- Create: `project/tests/live/test_tiny_live_admission_e2e.py`
- Modify: `Makefile`

- [ ] **Step 1: Write E2E test**
Write a test that:
1. Mocks/Creates a temporary data directory with all required artifacts.
2. Invokes the admission logic (via CLI or internal API).
3. Asserts it passes with all artifacts.
4. Asserts it fails if approval is missing.
5. Asserts it fails if caps exceed `tiny_live_v1`.

- [ ] **Step 2: Run test to verify it passes**
```bash
pytest project/tests/live/test_tiny_live_admission_e2e.py -v
```

- [ ] **Step 3: Update `minimum-green-gate` in Makefile**
Add `project/tests/live/test_tiny_live_admission_e2e.py` to the list of tests.

- [ ] **Step 4: Run `make minimum-green-gate`**

- [ ] **Step 5: Commit**
```bash
git add project/tests/live/test_tiny_live_admission_e2e.py Makefile
git commit -m "test: add e2e tiny-live admission smoke test"
```
