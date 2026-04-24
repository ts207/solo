# Tech Debt Cleanup Phase 1 & 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore pytest collection integrity and eliminate noise via automated hygiene fixers.

**Architecture:** We will first manually fix the `ImportError` issues blocking `pytest` from discovering tests. Once the test runner can collect tests successfully, we will use `ruff check . --fix` to enforce standardized syntax and clean up unused imports.

**Tech Stack:** Python, pytest, ruff

---

### Task 1: Fix `project.events` Import Error in Test

**Files:**
- Modify: `project/tests/events/test_scoring_integration.py`

- [ ] **Step 1: Run test collection to verify failure**

Run: `pytest project/tests/events/test_scoring_integration.py --collect-only`
Expected: FAIL with "cannot import name 'arbitrate_events' from 'project.events'"

- [ ] **Step 2: Fix the import statement**

Change the import in `project/tests/events/test_scoring_integration.py` to import `arbitrate_events` directly from the `arbitration` module.

Change:
```python
from project.events import score_event_frame, arbitrate_events, EventScoreColumns
```
To:
```python
from project.events import score_event_frame, EventScoreColumns
from project.events.arbitration import arbitrate_events
```

- [ ] **Step 3: Run test collection to verify it passes**

Run: `pytest project/tests/events/test_scoring_integration.py --collect-only`
Expected: PASS (no ImportError)

- [ ] **Step 4: Commit**

```bash
git add project/tests/events/test_scoring_integration.py
git commit -m "test: fix arbitrate_events import in test_scoring_integration"
```

### Task 2: Fix `SPEC_ROOT` Import Error in Test

**Files:**
- Modify: `project/tests/specs/test_arbitration_specs.py`

- [ ] **Step 1: Run test collection to verify failure**

Run: `pytest project/tests/specs/test_arbitration_specs.py --collect-only`
Expected: FAIL with "cannot import name 'SPEC_ROOT' from 'project.tests.conftest'"

- [ ] **Step 2: Fix the import statement**

In `project/tests/specs/test_arbitration_specs.py`, change:
```python
from project.tests.conftest import SPEC_ROOT
```
to:
```python
from project.spec_registry import SPEC_ROOT
```

- [ ] **Step 3: Run test collection to verify it passes**

Run: `pytest project/tests/specs/test_arbitration_specs.py --collect-only`
Expected: PASS (no ImportError)

- [ ] **Step 4: Commit**

```bash
git add project/tests/specs/test_arbitration_specs.py
git commit -m "test: fix SPEC_ROOT import in test_arbitration_specs"
```

### Task 3: Verify Global Test Collection

- [ ] **Step 1: Run global collection**

Run: `pytest project/tests --collect-only`

- [ ] **Step 2: Fix any remaining collection errors (if applicable)**
If there are other import errors, fix them using the same pattern as Task 1 & 2. Search for the missing symbol and update the import.
(If collection succeeds, skip to Step 3).

- [ ] **Step 3: Commit**

```bash
# Only if additional fixes were made
git commit -am "test: fix remaining import errors blocking collection"
```

### Task 4: Automated Linting Hygiene

- [ ] **Step 1: Run Ruff auto-fix globally**

Run: `ruff check . --fix`
Expected: Applies fixes for unused imports, unused variables, and sorts imports.

- [ ] **Step 2: Verify fixes**

Run: `ruff check .`
Expected: Remaining errors are mostly `E501` (Line too long) or structural issues. The `F401` and `I001` should be mostly resolved.

- [ ] **Step 3: Commit**

```bash
git commit -am "style: apply automated ruff fixes globally"
```
