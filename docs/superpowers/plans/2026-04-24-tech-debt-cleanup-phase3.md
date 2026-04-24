# Tech Debt Cleanup Phase 3: Core Integrity & Type Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve structural import errors blocking the test suite and fix foundational type errors in the core layer.

**Architecture:** 
1. **Import Realignment:** We will fix the redirection of key functions that moved to `project/core` but are still being imported from legacy locations (research/engine).
2. **Foundational Typing:** We will resolve Pyright errors in `project/core/`, focusing on ambiguity between Series/DataFrame and correcting invalid attribute access on numeric types.

**Tech Stack:** Python, pytest, pyright, ruff

---

### Task 1: Realign Foundation Imports (The "Big Redirect")

Many symbols moved to `project/core` but callers still use `project.research` or `project.engine`.

- [ ] **Step 1: Fix `bh_adjust` imports**
Search for `from project.research.gating import bh_adjust` and replace with `from project.core.stats import bh_adjust`.
Run: `grep -r "project.research.gating import bh_adjust" project`
Apply fixes.

- [ ] **Step 2: Fix `estimate_transaction_cost_bps` imports**
Search for `from project.engine.execution_model import estimate_transaction_cost_bps` and replace with `from project.core.execution_costs import estimate_transaction_cost_bps`.
Run: `grep -r "project.engine.execution_model import estimate_transaction_cost_bps" project`
Apply fixes.

- [ ] **Step 3: Fix `_evaluate_continuation_quality` imports**
Callers are trying to import this from `promotion_decision_support` but it should be imported from `promotion_gate_evaluators` (or `promotion_decision_support` needs to re-export it). Let's fix the callers to go directly to `promotion_gate_evaluators`.
Run: `grep -r "from project.research.promotion.promotion_decision_support import.*_evaluate_continuation_quality" project`
Apply fixes.

- [ ] **Step 4: Verify Collection**
Run: `pytest project/tests --collect-only`
Expected: Significant reduction in 123 collection errors.

- [ ] **Step 5: Commit**
```bash
git commit -am "refactor: realign core imports from legacy locations"
```

### Task 2: Fix Foundational Type Errors in `project/core/causal_primitives.py`

This file has many "Series | DataFrame" ambiguity errors.

- [ ] **Step 1: Run Pyright on file**
Run: `pyright project/core/causal_primitives.py`

- [ ] **Step 2: Add explicit type narrowing or casts**
In `causal_primitives.py`, functions returning `Series` often perform operations that could theoretically return a `DataFrame` (like some `.shift()` or `.rolling()` on a DataFrame). We need to ensure we are operating on and returning `Series`.

Example Fix:
```python
# From:
return df[col].shift(1)
# To:
result = df[col].shift(1)
if isinstance(result, pd.Series):
    return result
raise TypeError("Expected Series")
```
(Or use `cast(pd.Series, ...)` if we are certain).

- [ ] **Step 3: Verify fixes**
Run: `pyright project/core/causal_primitives.py`
Expected: Zero errors.

- [ ] **Step 4: Commit**
```bash
git commit -am "fix(type): resolve Series/DataFrame ambiguity in causal_primitives"
```

### Task 3: Fix Foundational Type Errors in `project/core/context_quality.py`

- [ ] **Step 1: Run Pyright on file**
Run: `pyright project/core/context_quality.py`

- [ ] **Step 2: Resolve invalid attribute access**
Fix cases where methods like `dropna`, `empty`, `mean` are called on types that Pyright thinks might be `float` or `ndarray`. Use `pd.Series(...)` wrapping or type guards.

- [ ] **Step 3: Verify fixes**
Run: `pyright project/core/context_quality.py`
Expected: Zero errors.

- [ ] **Step 4: Commit**
```bash
git commit -am "fix(type): resolve attribute access issues in context_quality"
```

### Task 4: Final Collection Verification

- [ ] **Step 1: Run global collection**
Run: `pytest project/tests --collect-only`
Expected: **Zero collection errors.** All 3451+ tests should be discoverable.

- [ ] **Step 2: Commit**
```bash
git commit -am "test: restore 100% test collection integrity"
```
