from __future__ import annotations

from project.scripts.check_protected_paths import get_violations

def test_get_violations_no_violations():
    modified = ["project/core/logic.py", "README.md"]
    violations = get_violations(modified)
    assert violations == []

def test_get_violations_with_direct_match():
    modified = ["project/configs/live_production.yaml", "project/core/logic.py"]
    violations = get_violations(modified)
    assert violations == ["project/configs/live_production.yaml"]

def test_get_violations_with_glob_match():
    modified = ["project/configs/live_trading_run1.yaml", "README.md"]
    violations = get_violations(modified)
    assert violations == ["project/configs/live_trading_run1.yaml"]

def test_get_violations_with_prefix_match():
    modified = ["data/live/theses/run_123/promoted_theses.json"]
    violations = get_violations(modified)
    assert violations == ["data/live/theses/run_123/promoted_theses.json"]

def test_get_violations_with_env_match():
    modified = [".env", ".env.production", "deploy/env/edge-live.env"]
    violations = get_violations(modified)
    assert set(violations) == {".env", ".env.production", "deploy/env/edge-live.env"}
