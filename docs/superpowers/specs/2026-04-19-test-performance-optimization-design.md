# Design Doc: Test Performance Optimization

## Problem Statement
The current test suite (~3300 tests) suffers from significant performance bottlenecks, leading to excessive execution times (estimated 20+ minutes for a full run). 

### Key Bottlenecks Identified:
1.  **Manifest Metadata Re-computation (Critical):** Every discovery run triggers `start_manifest`, which takes ~9 seconds to re-hash over 480 YAML files in the `spec/` directory.
2.  **Data Lake Leakage:** Core modules resolve `get_data_root()` to the production 13GB `data/` directory. Tests that fail to explicitly override this default hit real datasets, causing massive I/O overhead.
3.  **Redundant Disk I/O:** `load_features` and `execute_candidate_discovery` repeatedly load Parquet files and perform expensive `merge_asof` joins for every test case.
4.  **Sequential Execution:** The suite runs sequentially, failing to utilize multi-core capabilities.

## Proposed Solutions

### 1. Global Metadata Caching
Introduce process-level caching for expensive metadata operations that do not change during a test run.
- **Spec Hashes:** Cache `get_spec_hashes` and `ontology_spec_hash` in `project/specs/utils.py` and `project/specs/ontology.py`.
- **Git Commit:** Cache the result of `git rev-parse HEAD` in `project/specs/manifest.py`.

### 2. Environmental Hardening and Data Isolation
Ensure tests are strictly isolated from the production data lake.
- **Fixture-Based Isolation:** Add a session-scoped fixture in a root `conftest.py` that sets `EDGE_DATA_ROOT` to a temporary directory *before* any other imports.
- **Lazy Resolution:** Refactor module-level `get_data_root()` calls (e.g., in `research_core.py`) to use lazy initialization or function wrappers.
- **Safety Assertions:** Add a check to verify that `data/lake` is not accessed during test execution.

### 3. I/O and Service Optimization
Reduce the cost of data loading in research services.
- **Feature Caching:** Implement an in-memory cache for `load_features` when running in `pytest`.
- **Lightweight Discovery Mode:** Introduce a "dry-run" or "mock-data" mode for `execute_candidate_discovery` for use in service-level unit tests.

### 4. Parallelization and Hygiene
Improve resource utilization and test suite organization.
- **Xdist Support:** Configure `pytest-xdist` in `pyproject.toml` and the `Makefile`.
- **Marker Audit:** Properly apply `@pytest.mark.slow` and `@pytest.mark.integration` to heavy tests to allow for fast development cycles.

## Success Criteria
- **Discovery Setup Time:** Reduced from 9 seconds to <100ms per call after initial caching.
- **Total Suite Execution:** Reduced by at least 70% (target <5 minutes for a full parallel run).
- **Data Isolation:** Zero access to the 13GB `data/lake` directory during standard test runs.

## Architecture & Components
- **`project/specs/cache.py` (New):** Optional shared caching utility if needed across modules.
- **`project/tests/conftest.py`:** Hardened with environmental isolation logic.
- **`project/research/phase2.py`:** Updated with feature caching logic.
- **`project/specs/manifest.py`:** Updated to use cached metadata.

## Implementation Plan
1. Implement Spec and Git hash caching.
2. Refactor module-level `get_data_root` calls.
3. Implement `conftest.py` environmental hardening.
4. Implement `load_features` caching.
5. Configure `pytest-xdist` and audit test markers.
