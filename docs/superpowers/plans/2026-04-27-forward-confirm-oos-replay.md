# True OOS Forward Confirmation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current snapshot-based forward confirmation with a true Out-Of-Sample (OOS) rerun of a frozen thesis to prevent selection leakage and ensure research integrity.

**Architecture:** Independent OOS orchestration that reuses deterministic evaluation components (`evaluate_hypothesis_batch`) while enforcing strict OOS boundaries and frozen thesis identities.

**Tech Stack:** Python, pandas, project.research evaluation primitives.

---

### Task 1: CI Branch Trigger Patch
*Isolated task to fix CI configuration.*

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Update CI branch triggers**
    Update `.github/workflows/ci.yml` to trigger on both `main` and `Main`.
    ```yaml
    on:
      pull_request:
      push:
        branches: [main, Main]
    ```
- [ ] **Step 2: Commit CI fix**
    ```bash
    git add .github/workflows/ci.yml
    git commit -m "ci: update branch triggers to include Main"
    ```

---

### Task 2: Deprecate Snapshot Method and Add Leakage Guard
*Ensure the old unsafe behavior is blocked before implementing the new one.*

**Files:**
- Modify: `project/validate/forward_confirm.py`
- Create: `project/tests/validate/test_forward_confirm_leakage.py`

- [ ] **Step 1: Write an anti-regression test for leakage**
    Create `project/tests/validate/test_forward_confirm_leakage.py` to ensure no sorting or selection logic is used in the loader.
    ```python
    import pytest
    from project.validate.forward_confirm import _load_frozen_thesis
    from pathlib import Path

    def test_load_frozen_thesis_no_selection_leakage():
        # This test will be updated as the loader is implemented
        # For now, it's a placeholder to remind us to check for forbidden methods
        forbidden = ["sort_values", "idxmax", "nlargest", "rank_score"]
        with open("project/validate/forward_confirm.py", "r") as f:
            content = f.read()
            for word in forbidden:
                assert word not in content, f"Forbidden word '{word}' found in forward_confirm.py"
    ```
- [ ] **Step 2: Replace `build_forward_confirmation_payload` with a failing placeholder**
    Update `project/validate/forward_confirm.py` to raise an error if the old snapshot method is used.
    ```python
    def build_forward_confirmation_payload(*, run_id: str, window: str, data_root: Path | None = None) -> dict[str, Any]:
        raise RuntimeError(
            "forward-confirm currently cannot use phase2 candidate snapshots; "
            "implement oos_frozen_thesis_replay_v1"
        )
    ```
- [ ] **Step 3: Run the leakage test**
    Run: `pytest project/tests/validate/test_forward_confirm_leakage.py`
    Expected: PASS (assuming no forbidden words yet).
- [ ] **Step 4: Commit deprecation**
    ```bash
    git add project/validate/forward_confirm.py project/tests/validate/test_forward_confirm_leakage.py
    git commit -m "feat: deprecate snapshot-based forward confirmation and add leakage guards"
    ```

---

### Task 3: Implement Frozen Thesis Loading
*Add logic to identify the exact thesis to replay without selection.*

**Files:**
- Modify: `project/validate/forward_confirm.py`

- [ ] **Step 1: Implement `_load_frozen_thesis`**
    Add the following helper to `project/validate/forward_confirm.py`:
    ```python
    from project.domain.hypotheses import HypothesisSpec
    from project.specs.manifest import load_run_manifest

    def _load_frozen_thesis(
        run_id: str, 
        proposal_path: Path | None = None, 
        candidate_id: str | None = None, 
        data_root: Path | None = None
    ) -> HypothesisSpec:
        if proposal_path:
            from project.spec_registry import load_yaml_path
            doc = load_yaml_path(proposal_path)
            # Logic to convert proposal doc to HypothesisSpec
            ... 
        
        # Priority 2: promoted_theses.json
        # Priority 3: run_manifest.json
        # Priority 4: candidate_id lookup in phase2_candidates.parquet (NO SORTING)
        ...
        raise ValueError(f"No frozen thesis identity found for run {run_id}")
    ```
- [ ] **Step 2: Verify no sorting in implementation**
    Check `project/validate/forward_confirm.py` for any `sort_values`, `idxmax`, etc.
- [ ] **Step 3: Commit loader**
    ```bash
    git add project/validate/forward_confirm.py
    git commit -m "feat: implement frozen thesis loading for OOS replay"
    ```

---

### Task 4: Implement OOS Replay Orchestration
*Orchestrate the rerun over the OOS window.*

**Files:**
- Modify: `project/validate/forward_confirm.py`
- Create: `project/tests/validate/test_forward_confirm_oos.py`

- [ ] **Step 1: Implement `oos_frozen_thesis_replay_v1`**
    Replace the placeholder in `forward_confirm` with the actual replay logic.
    ```python
    from project.research.search.evaluator import evaluate_hypothesis_batch
    from project.research.search.search_feature_utils import prepare_search_features_for_symbol

    def forward_confirm(
        *,
        run_id: str,
        window: str,
        proposal_path: Path | None = None,
        candidate_id: str | None = None,
        data_root: Path | None = None,
    ) -> dict[str, Any]:
        root = Path(data_root) if data_root is not None else get_data_root()
        start, end = _parse_window(window)
        thesis = _load_frozen_thesis(run_id, proposal_path, candidate_id, root)
        
        # Load OOS data (with warmup)
        # detect events, compute metrics
        # filter signals where exit_ts > end
        ...
        
        return {
            "run_id": run_id,
            "method": "oos_frozen_thesis_replay_v1",
            "metrics": metrics,
            ...
        }
    ```
- [ ] **Step 2: Add functional OOS tests**
    Create `project/tests/validate/test_forward_confirm_oos.py` to verify the end-to-end flow with synthetic data.
- [ ] **Step 3: Run all tests**
    Run: `pytest project/tests/validate/`
- [ ] **Step 4: Commit OOS replay**
    ```bash
    git add project/validate/forward_confirm.py project/tests/validate/test_forward_confirm_oos.py
    git commit -m "feat: implement oos_frozen_thesis_replay_v1"
    ```
