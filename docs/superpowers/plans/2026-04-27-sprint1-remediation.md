# Sprint 1 — Safety Gates Remediation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix correctness gaps in Sprint 1 implementation: improve monitor formatting, use schema-validated `ThesisStore` for admission, and expand test coverage for live safety.

**Architecture:**
- **Monitor Fix:** Robust `None` handling in `_build_summary`.
- **Deploy Admission Guard (V2):** Accepts `thesis_path`, uses `ThesisStore.from_path(strict_live_gate=True)`.
- **CLI Enhancement:** Add `--monitor_report` arg to `bind-config`, pass to `assert_deploy_admission`.
- **Expanded Testing:** Test approval/cap enforcement and monitor report integration.
- **Main Gate Integration:** Add new tests to `minimum-green-gate`.

**Tech Stack:** Python, pytest, Edge framework primitives.

---

### Task 1: Fix Monitor Summary Formatting

**Files:**
- Modify: `project/scripts/monitor_research_thesis.py`

- [ ] **Step 1: Implement robust formatting helpers**

```python
def _fmt_float(value: float | None, digits: int = 3) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"

def _fmt_bps(value: float | None) -> str:
    return "NA" if value is None else f"{value:.1f}"

def _fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.1%}"
```

- [ ] **Step 2: Update `_build_summary`**

```python
def _build_summary(
    *, n: int | None, t_net: float | None, robustness: float | None,
    net_bps: float | None, gate_progress: float | None, deployment_ready: bool,
) -> str:
    lines = [
        f"n={n if n is not None else 'NA'}  "
        f"t_net={_fmt_float(t_net)}  "
        f"robustness={_fmt_float(robustness)}  "
        f"net_bps={_fmt_bps(net_bps)}",
        f"gate_progress={_fmt_pct(gate_progress)}  deployment_ready={deployment_ready}",
    ]
    if not deployment_ready:
        if robustness is not None:
            lines.append(
                f"Gap to gate: robustness needs {0.70 - robustness:.3f} more "
                f"(current {robustness:.3f} → target 0.70)"
            )
        else:
            lines.append("Gap to gate: robustness data missing (target 0.70)")
    return "  |  ".join(lines)
```

- [ ] **Step 3: Run regression tests**

Run: `PYTHONPATH=. pytest project/tests/scripts/test_monitor_research_thesis.py -v`
Expected: PASS

---

### Task 2: Enhance `deploy_admission.py`

**Files:**
- Modify: `project/live/deploy_admission.py`

- [ ] **Step 1: Update `assert_deploy_admission` signature and implementation**

```python
from pathlib import Path
import json
from project.live.thesis_store import ThesisStore
from project.live.contracts.promoted_thesis import LIVE_TRADEABLE_STATES

def assert_deploy_admission(
    *,
    thesis_path: Path,
    runtime_mode: str,
    monitor_report_path: Path | None = None,
) -> None:
    """
    Gates deployment based on validated thesis artifacts and monitor readiness.
    """
    runtime_mode = runtime_mode.lower()
    
    # 1. Load and validate via ThesisStore (applies DeploymentGate)
    # Raises RuntimeError on schema mismatch or DeploymentGate violations
    store = ThesisStore.from_path(thesis_path, strict_live_gate=True)
    theses = store.all()
    if not theses:
        raise ValueError(f"Thesis artifact {thesis_path} contains no theses")

    # 2. Determine monitor readiness
    deployment_ready = False
    if monitor_report_path and monitor_report_path.exists():
        try:
            report = json.loads(monitor_report_path.read_text(encoding="utf-8"))
            deployment_ready = report.get("deployment_ready", False)
        except Exception:
             pass

    # 3. Mode-specific admission
    for thesis in theses:
        state = thesis.deployment_state
        
        if runtime_mode == "trading":
            if state not in LIVE_TRADEABLE_STATES:
                raise PermissionError(f"Trading mode blocked: thesis {thesis.thesis_id} is in state '{state}'. Requires 'live_enabled'.")
            if not deployment_ready:
                 raise PermissionError(f"Trading mode blocked: monitor report deployment_ready=False for thesis {thesis.thesis_id}.")

        if runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled", "paper_only"]
            if state not in paper_compatible:
                # Optional: check if monitor report has high robustness even if not paper_enabled yet
                if state == "promoted" and deployment_ready:
                    continue
                raise PermissionError(f"Simulation mode blocked: thesis {thesis.thesis_id} is in state '{state}'. Requires paper-enabled state.")
```

---

### Task 3: Update CLI and `bind-config`

**Files:**
- Modify: `project/cli.py`

- [ ] **Step 1: Add `--monitor_report` to `bind-config` parser**

- [ ] **Step 2: Update `_run_deploy_bind_config` to use the new admission guard**

```python
def _run_deploy_bind_config(args: argparse.Namespace) -> int:
    from project.live.deploy_admission import assert_deploy_admission
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_path_override = _path_or_none(args.thesis_path)
    thesis_path = thesis_path_override or _thesis_path_for_run(
        data_root=data_root,
        run_id=args.run_id,
    )
    if not thesis_path.exists():
        raise FileNotFoundError(f"thesis artifact not found: {thesis_path}")

    monitor_report_path = _path_or_none(getattr(args, "monitor_report", None))
    runtime_mode = str(args.runtime_mode).strip().lower() or "monitor_only"
    
    # Assert admission
    try:
        assert_deploy_admission(
            thesis_path=thesis_path,
            runtime_mode=runtime_mode,
            monitor_report_path=monitor_report_path
        )
    except (PermissionError, RuntimeError, ValueError) as e:
        _emit_json({"status": "error", "message": str(e)})
        return 1

    # ... rest of the function ...
```

---

### Task 4: Expand Deploy Admission Tests

**Files:**
- Modify: `project/tests/live/test_deploy_admission.py`

- [ ] **Step 1: Add tests for live approval and cap enforcement**

Test:
- `live_enabled` + missing approval fails
- `live_enabled` + missing caps fails
- `trading` + `monitor_only` fails
- `simulation` + `monitor_only` fails
- `monitor_only` + `monitor_only` passes
- `trading` + `live_enabled` + `deployment_ready=False` fails

---

### Task 5: Main Gate Integration

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add Sprint 1 tests to `minimum-green-gate` target**

```make
minimum-green-gate:
    ...
    @PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/scripts/test_monitor_research_thesis.py
    @PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_deploy_admission.py
```
