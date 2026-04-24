# Tech Debt Cleanup Design

## Overview
A comprehensive, phased plan to resolve significant technical debt across the Edge repository, prioritizing a bottom-up approach that restores test verification before attempting large-scale type safety refactoring.

## Context
Initial diagnostics revealed:
- **Linting:** ~5,500 Ruff errors (mostly import sorting, unused variables, and long lines).
- **Type Checking:** ~15,000 Pyright errors (many related to Pandas/NumPy types, missing attributes, and unannotated function returns).
- **Tests:** `pytest project/tests` fails during collection due to `ImportError` and missing modules, preventing any tests from running.

## Architecture & Phasing
The cleanup will follow a "Bottom-Up (Foundation First)" strategy to ensure changes can be verified safely.

### Phase 1: Test Collection & Import Integrity
- **Goal:** Allow `pytest` to collect and run tests, regardless of pass/fail status.
- **Scope:** Address all `ImportError`, `SyntaxError`, and circular dependencies that block test discovery.
- **Key Targets:** `project/events/__init__.py`, `project/tests/conftest.py`, and any other files throwing collection errors.
- **Success Criteria:** `pytest --collect-only` completes successfully.

### Phase 2: Automated Hygiene & Syntax
- **Goal:** Eliminate low-value noise and standardize formatting to reveal structural issues.
- **Scope:** Run automated fixers globally.
- **Key Targets:** 
  - Run `ruff check . --fix` to resolve import sorting (`I001`), unused imports (`F401`), unused variables (`F841`), and trailing whitespace (`W293`).
  - Resolve boolean comparison errors (`E712`).
- **Success Criteria:** Zero remaining automatically-fixable Ruff errors.

### Phase 3: Core Type Safety (The "Engine Room")
- **Goal:** Lock down foundational contracts so upstream components have reliable type signatures.
- **Scope:** Resolve `pyright` errors in foundational layers.
- **Key Targets:** 
  - `project/core/` (e.g., `causal_primitives.py`)
  - `project/contracts/` (e.g., `schemas.py`)
  - `project/schemas/`
- **Success Criteria:** Zero Pyright errors in the target directories.

### Phase 4: Domain & Research Typing
- **Goal:** Fix typing in heavily logic-driven modules.
- **Scope:** Resolve `pyright` errors downstream from core.
- **Key Targets:** 
  - `project/engine/`
  - `project/research/`
  - `project/strategy/`
  - `project/apps/`
  - `project/cli.py`
- **Success Criteria:** Overall Pyright errors reduced to near zero.

### Phase 5: Test Execution & Verification
- **Goal:** Ensure business logic hasn't been compromised during refactoring.
- **Scope:** Fix test failures that were unmasked by Phase 1 and caused by structural changes in Phases 3/4.
- **Key Targets:** All failing tests under `project/tests/`.
- **Success Criteria:** `pytest` reports 100% pass rate.

## Testing Strategy
The entire design is oriented around restoring and leveraging the test suite. Each phase is a prerequisite for the next, with the final phase ensuring the structural changes did not break the functional requirements.
