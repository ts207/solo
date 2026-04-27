# Sprint 1 — Safety Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement safety gates for thesis deployment and runtime admission, ensuring research-only theses cannot be accidentally traded.

**Architecture:**
- **Monitor Regression Test:** Ensure the monitor script is reliable and correctly assesses deployment readiness.
- **Deploy Admission Guard:** A new module `project/live/deploy_admission.py` to gate config binding based on thesis state and monitor readiness.
- **Runtime Admission Guard:** A new module `project/live/runtime_admission.py` used by the `LiveEngineRunner` to block unsafe runtime modes for a given thesis.
- **Makefile Integration:** Shortcut for monitoring the lead thesis.

**Tech Stack:** Python, pytest, Edge framework primitives.

---

### Task 1: Monitor Regression Tests

**Files:**
- Create: `project/tests/scripts/test_monitor_research_thesis.py`
- Test: `project/tests/scripts/test_monitor_research_thesis.py`

- [ ] **Step 1: Write the failing tests for monitor script**

```python
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from project.scripts.monitor_research_thesis import build_report

def test_monitor_report_schema():
    """Verify the report has the expected schema version and slug."""
    with patch("project.scripts.monitor_research_thesis._load_eval_results") as m_eval:
        m_eval.return_value = None # Empty state
        report = build_report(run_id="test_run", data_root=Path("/tmp"))
        assert report["schema_version"] == "monitor_report_v1"
        assert report["thesis_slug"] == "oasrep_chop_long_48b"

def test_deployment_ready_logic():
    """Verify deployment_ready is true only when all gates pass."""
    with patch("project.scripts.monitor_research_thesis._load_eval_results") as m_eval:
        # Mocking eval results that pass all gates
        # robustness >= 0.70, t_net >= 2.0, net_bps > 0
        m_eval.return_value = {
            "n": 100,
            "mean_return_bps": 50.0,
            "mean_return_net_bps": 30.0,
            "hit_rate": 0.55,
            "mae_mean_bps": 10.0,
            "mfe_mean_bps": 60.0,
            "t_stat_net": 2.5,
            "robustness_score": 0.75,
            "expected_cost_bps_per_trade": 10.0,
            "sharpe": 1.5,
            "stress_score": 0.8,
            "placebo_shift_effect": 0.1,
            "placebo_random_entry_effect": 0.05
        }
        report = build_report(run_id="test_run", data_root=Path("/tmp"))
        assert report["deployment_ready"] is True
        assert report["gate_progress_to_0_70"] >= 1.0

        # Now mock a failure (robustness < 0.70)
        m_eval.return_value["robustness_score"] = 0.585
        report = build_report(run_id="test_run", data_root=Path("/tmp"))
        assert report["deployment_ready"] is False
        assert report["gate_progress_to_0_70"] < 1.0
        assert "robustness < 0.70" in report["deployment_blocker"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest project/tests/scripts/test_monitor_research_thesis.py -v`
Expected: PASS (The logic already exists in the script, so this is a regression check. If it fails, fix the script or the test).

- [ ] **Step 3: Commit**

```bash
git add project/tests/scripts/test_monitor_research_thesis.py
git commit -m "test: add monitor regression tests"
```

---

### Task 2: Deploy Admission Guard

**Files:**
- Create: `project/live/deploy_admission.py`
- Modify: `project/cli.py`
- Test: `project/tests/live/test_deploy_admission.py`

- [ ] **Step 1: Write the failing tests for deploy admission**

```python
import pytest
from pathlib import Path
from project.live.deploy_admission import assert_deploy_admission

def test_deploy_admission_monitor_only():
    """research_promoted + runtime_mode=monitor_only -> pass"""
    # We'll need to mock a thesis artifact for this
    # For now, let's just test the logic with hypothetical inputs if possible, 
    # or use real artifact structure.
    pass

def test_deploy_admission_trading_blocked():
    """research_promoted + runtime_mode=trading -> fail"""
    with pytest.raises(PermissionError, match="Trading mode blocked"):
        assert_deploy_admission(
            thesis_state="research_promoted",
            runtime_mode="trading",
            deployment_ready=False
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest project/tests/live/test_deploy_admission.py -v`
Expected: FAIL with "ImportError" (module doesn't exist yet).

- [ ] **Step 3: Implement `project/live/deploy_admission.py`**

```python
from pathlib import Path

def assert_deploy_admission(
    *,
    thesis_state: str,
    runtime_mode: str,
    deployment_ready: bool = False,
) -> None:
    """
    Gates deployment based on thesis state and monitor readiness.
    
    Rules:
    - If thesis is research_promoted or monitor_only and deployment_ready != True:
      - monitor_only: allowed
      - simulation: blocked (unless explicitly paper_enabled, but that's next phase)
      - trading: blocked
    """
    runtime_mode = runtime_mode.lower()
    
    if thesis_state in ["research_promoted", "monitor_only"]:
        if not deployment_ready:
            if runtime_mode == "trading":
                raise PermissionError(f"Trading mode blocked for thesis in state '{thesis_state}' with deployment_ready=False")
            if runtime_mode == "simulation":
                raise PermissionError(f"Simulation mode blocked for thesis in state '{thesis_state}' with deployment_ready=False. Requires paper_enabled state.")
    
    # live_enabled checks would go here in future phases
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest project/tests/live/test_deploy_admission.py -v`
Expected: PASS

- [ ] **Step 5: Hook into `project/cli.py`**

Modify `_run_deploy_bind_config` to call `assert_deploy_admission`. 
You'll need to load the thesis artifact to get its state, and potentially the monitor report to get `deployment_ready`.

- [ ] **Step 6: Commit**

```bash
git add project/live/deploy_admission.py project/cli.py project/tests/live/test_deploy_admission.py
git commit -m "feat: add deploy admission guard"
```

---

### Task 3: Runtime Admission Guard

**Files:**
- Create: `project/live/runtime_admission.py`
- Modify: `project/live/runner.py`
- Test: `project/tests/live/test_runtime_admission.py`

- [ ] **Step 1: Write the failing tests for runtime admission**

```python
import pytest
from project.live.runtime_admission import validate_runtime_mode_against_theses

def test_runtime_admission_trading_refuses_research():
    theses = [MagicMock(deployment_state="monitor_only")]
    with pytest.raises(ValueError, match="cannot run in trading mode"):
        validate_runtime_mode_against_theses("trading", theses)

def test_runtime_admission_monitor_accepts_research():
    theses = [MagicMock(deployment_state="monitor_only")]
    validate_runtime_mode_against_theses("monitor_only", theses) # Should not raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest project/tests/live/test_runtime_admission.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `project/live/runtime_admission.py`**

```python
def validate_runtime_mode_against_theses(runtime_mode: str, theses: list) -> None:
    runtime_mode = runtime_mode.lower()
    for thesis in theses:
        state = getattr(thesis, "deployment_state", "unknown")
        if runtime_mode == "trading":
            if state != "live_enabled":
                raise ValueError(f"Thesis in state '{state}' cannot run in trading mode. Requires 'live_enabled'.")
        elif runtime_mode == "simulation":
            if state not in ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]:
                raise ValueError(f"Thesis in state '{state}' cannot run in simulation mode. Requires paper-enabled state.")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest project/tests/live/test_runtime_admission.py -v`
Expected: PASS

- [ ] **Step 5: Hook into `project/live/runner.py`**

Call `validate_runtime_mode_against_theses` in `LiveEngineRunner.__init__` or `run()`.

- [ ] **Step 6: Commit**

```bash
git add project/live/runtime_admission.py project/live/runner.py project/tests/live/test_runtime_admission.py
git commit -m "feat: add runtime admission guard"
```

---

### Task 4: Makefile Integration

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add `monitor-lead-thesis` target**

```makefile
monitor-lead-thesis:
	@PYTHONPATH=. $(PYTHON) project/scripts/monitor_research_thesis.py --run_id stat_stretch_04 --data_root data
```

- [ ] **Step 2: Verify the target**

Run: `make monitor-lead-thesis`
Expected: Runs the monitor script (even if it finds no data, it should execute).

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: add monitor-lead-thesis Makefile target"
```
