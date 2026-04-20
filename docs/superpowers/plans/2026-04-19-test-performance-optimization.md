# Test Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce test suite execution time by 70%+ through metadata caching, data isolation, and I/O optimization.

**Architecture:** 
- Process-level caching for spec/ontology hashes to eliminate 9s overhead per discovery call.
- Environment isolation in `conftest.py` to prevent 13GB data lake leakage.
- Lazy resolution of `DATA_ROOT` to ensure test-time overrides are respected.
- In-memory feature caching for research services.

**Tech Stack:** Python, pytest, functools.lru_cache

---

### Task 1: Spec and Ontology Hash Caching

**Files:**
- Modify: `project/specs/utils.py`
- Modify: `project/specs/ontology.py`
- Modify: `project/specs/manifest.py`

- [ ] **Step 1: Cache spec hashes in `project/specs/utils.py`**
Modify `get_spec_hashes` to use `functools.lru_cache(maxsize=1)`.

```python
from functools import lru_cache

@lru_cache(maxsize=1)
def get_spec_hashes(project_root: Path) -> Dict[str, str]:
    # ... existing implementation ...
```

- [ ] **Step 2: Cache ontology hashes in `project/specs/ontology.py`**
Wrap `ontology_spec_hash` and `ontology_component_hashes` with `lru_cache(maxsize=1)`.

- [ ] **Step 3: Cache Git commit in `project/specs/manifest.py`**
Implement a cached wrapper for `_git_commit`.

```python
@lru_cache(maxsize=1)
def _cached_git_commit(project_root: Path) -> str:
    return _git_commit(project_root)
```

- [ ] **Step 4: Verify speedup with profiling script**
Run `PYTHONPATH=. .venv/bin/python profile_discovery.py` and verify `cumtime` for `start_manifest` drops significantly.

- [ ] **Step 5: Commit**
```bash
git add project/specs/utils.py project/specs/ontology.py project/specs/manifest.py
git commit -m "perf: cache spec and ontology hashes in manifest initialization"
```

### Task 2: Refactor Module-Level `DATA_ROOT`

**Files:**
- Modify: `project/research/research_core.py`
- Modify: `project/research/evaluate_naive_entry.py`
- Modify: `project/io/universe.py`
- Modify: `project/research/helpers/loading.py`

- [ ] **Step 1: Refactor `research_core.py` to lazy resolution**
Replace `DATA_ROOT = get_data_root()` with a property or function wrapper.

```python
def get_research_data_root() -> Path:
    return get_data_root()
```

- [ ] **Step 2: Update callers of `research_core.DATA_ROOT`**
Update other modules that import and use `research_core.DATA_ROOT`.

- [ ] **Step 3: Refactor other module-level assignments**
Apply similar lazy resolution to `evaluate_naive_entry.py`, `universe.py`, and `loading.py`.

- [ ] **Step 4: Commit**
```bash
git add project/research/research_core.py project/research/evaluate_naive_entry.py project/io/universe.py project/research/helpers/loading.py
git commit -m "perf: refactor module-level DATA_ROOT to lazy resolution to prevent leakage"
```

### Task 3: Enforce Environmental Isolation in `conftest.py`

**Files:**
- Modify: `project/tests/conftest.py`

- [ ] **Step 1: Add session-scoped environment isolation fixture**
Implement a fixture that sets `EDGE_DATA_ROOT` and `BACKTEST_DATA_ROOT` to a temporary directory before any other tests run.

```python
@pytest.fixture(scope="session", autouse=True)
def isolate_data_lake(tmp_path_factory):
    tmp_root = tmp_path_factory.mktemp("edge_test_data")
    os.environ["EDGE_DATA_ROOT"] = str(tmp_root)
    os.environ["BACKTEST_DATA_ROOT"] = str(tmp_root)
    return tmp_root
```

- [ ] **Step 2: Add check for production data access**
Implement a check (possibly using `pytest_runtest_teardown`) that asserts no files were accessed under the real `data/lake`.

- [ ] **Step 3: Commit**
```bash
git add project/tests/conftest.py
git commit -m "test: enforce environment isolation and prevent production data lake access"
```

### Task 4: In-Memory Feature Caching

**Files:**
- Modify: `project/research/phase2.py`

- [ ] **Step 1: Implement `load_features` caching**
Add an internal cache for `load_features` that is only active when `PYTEST_CURRENT_TEST` is in the environment.

```python
_FEATURE_CACHE = {}

def load_features(...):
    cache_key = (run_id, symbol, timeframe)
    if os.getenv("PYTEST_CURRENT_TEST") and cache_key in _FEATURE_CACHE:
        return _FEATURE_CACHE[cache_key].copy()
    
    # ... load ...
    
    if os.getenv("PYTEST_CURRENT_TEST"):
        _FEATURE_CACHE[cache_key] = df
    return df
```

- [ ] **Step 2: Verify with slow service tests**
Run `pytest project/tests/research/services/test_candidate_discovery_service.py` and verify timing improvement.

- [ ] **Step 3: Commit**
```bash
git add project/research/phase2.py
git commit -m "perf: add in-memory feature caching for tests"
```

### Task 5: Parallelization and Marker Audit

**Files:**
- Modify: `pyproject.toml`
- Modify: `Makefile`

- [ ] **Step 1: Configure `pytest-xdist`**
Add `pytest-xdist` to `project.optional-dependencies.dev` in `pyproject.toml`.

- [ ] **Step 2: Update `Makefile` for parallel execution**
Update the `test` target to use `-n auto`.

- [ ] **Step 3: Audit and apply markers**
Apply `@pytest.mark.slow` to `test_research_yield.py` and other identified slow tests.

- [ ] **Step 4: Final verification**
Run the full suite with `make test` and verify total time.

- [ ] **Step 5: Commit**
```bash
git add pyproject.toml Makefile project/tests/research/test_research_yield.py
git commit -m "perf: enable parallel test execution and audit slow markers"
```
