# Sprint 1 — Final Safety Hardening Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finalize Sprint 1 safety gates by strengthening tests, hardening admission policy, and improving Makefile integration.

**Architecture:**
- **Makefile Update:** Expose `MONITOR_REPORT` to `bind-config`.
- **Admission Hardening:** Remove `paper_only` from simulation-compatible states (strict ladder).
- **Artifact-Backed Tests:** Add real `promoted_theses.json` fixture tests to verify `DeploymentGate` enforcement.

**Tech Stack:** Python, pytest, Makefile.

---

### Task 1: Makefile Enhancement

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add `MONITOR_REPORT` variable and pass it to `bind-config`**

```make
MONITOR_REPORT ?=
...
bind-config:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@mkdir -p "$(OUT_DIR)"
	@$(CLI) deploy bind-config --run_id "$(RUN_ID)" --out_dir "$(OUT_DIR)" --runtime_mode "$(RUNTIME_MODE)" --symbols "$(SYMBOLS)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) $(if $(MONITOR_REPORT),--monitor_report "$(MONITOR_REPORT)",)
```

---

### Task 2: Harden Simulation Policy

**Files:**
- Modify: `project/live/deploy_admission.py`
- Modify: `project/live/runtime_admission.py`

- [ ] **Step 1: Remove `paper_only` from simulation-compatible states in `deploy_admission.py`**

```python
        if runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"] # Removed paper_only
            if state not in paper_compatible:
                ...
```

- [ ] **Step 2: Remove `paper_only` from simulation-compatible states in `runtime_admission.py`**

```python
        elif runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"] # Removed paper_only
            if state not in paper_compatible:
                ...
```

---

### Task 3: Artifact-Backed Admission Tests

**Files:**
- Modify: `project/tests/live/test_deploy_admission.py`

- [ ] **Step 1: Add tests using real temporary `promoted_theses.json` files**

Test cases:
- `live_enabled` + missing approval record -> should raise `RuntimeError` (via `DeploymentGate`)
- `live_enabled` + missing cap profile -> should raise `RuntimeError`
- `trading` + `monitor_only` thesis -> should raise `PermissionError`
- `simulation` + `monitor_only` thesis -> should raise `PermissionError`
- `monitor_only` + `monitor_only` thesis -> should pass

---

### Task 4: Final Verification

- [ ] **Step 1: Run all tests**

```bash
PYTHONPATH=. python3 -m pytest project/tests/scripts/test_monitor_research_thesis.py -q
PYTHONPATH=. python3 -m pytest project/tests/live/test_deploy_admission.py -q
PYTHONPATH=. python3 -m pytest project/tests/live/test_runtime_admission.py -q
make minimum-green-gate
```
