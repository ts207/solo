"""Regression test: forbid strategy name string parsing outside registry.

This test ensures that production code doesn't parse strategy names using
string methods (startswith, replace, split, partition). All strategy name
parsing should go through the centralized registry functions:
- parse_strategy_name()
- resolve_strategy()
- is_dsl_strategy()
"""

from pathlib import Path
import re

# Forbidden patterns - these indicate string-based strategy name parsing
FORBIDDEN = [
    re.compile(r'\.startswith\(\s*["\']dsl_interpreter_v1'),
    re.compile(r'\.replace\(\s*["\']dsl_interpreter_v1__'),
    re.compile(r'\.split\(\s*["\']__["\']\s*\)'),
    re.compile(r'\.partition\(\s*["\']__["\']\s*\)'),
]

# Allowlist: files where these patterns are explicitly allowed
ALLOW = {
    "project/strategy/runtime/registry.py",  # Centralized parser uses partition("__")
}


def test_no_strategy_name_string_parsing_in_production():
    """Regression test: forbid string-based strategy name parsing in production code."""
    root = Path(__file__).resolve().parents[1]
    project = root / "project"

    offenders = []
    for path in project.rglob("*.py"):
        rel = str(path.relative_to(root)).replace("\\", "/")
        if rel in ALLOW:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pat in FORBIDDEN:
            if pat.search(text):
                offenders.append((rel, pat.pattern))

    assert not offenders, (
        "Forbidden strategy-name parsing patterns found in production code:\n"
        + "\n".join(f"{p} :: {pat}" for p, pat in offenders)
        + "\n\nUse strategies.registry.parse_strategy_name() or is_dsl_strategy() instead."
    )


def test_is_dsl_strategy_exact_match_not_substring():
    """Invariant: is_dsl_strategy must match exact base, not substring."""
    from project.strategy.runtime.registry import is_dsl_strategy, parse_strategy_name

    # Valid DSL names
    assert is_dsl_strategy("dsl_interpreter_v1")[0] is True
    assert is_dsl_strategy("dsl_interpreter_v1__myblueprint")[0] is True

    # Invalid - contains dsl_interpreter_v1 but not as base
    assert is_dsl_strategy("other_dsl_interpreter_v1")[0] is False
    assert is_dsl_strategy("prefix_dsl_interpreter_v1__suffix")[0] is False
    assert is_dsl_strategy("dsl_interpreter_v1_suffix")[0] is False

    # Verify parse_strategy_name extracts correct base
    base, _ = parse_strategy_name("other_dsl_interpreter_v1")
    assert base == "other_dsl_interpreter_v1"  # Entire string is base
