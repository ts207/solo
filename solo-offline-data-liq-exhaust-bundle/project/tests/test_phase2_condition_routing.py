"""
Tests for Phase 2 condition routing into the DSL compiler.

Root cause being tested
-----------------------
the Phase 2 compatibility helpers set the `condition` field to:
    cond_name if cond_name == "all" else f"all__{cond_name}"

The `"all__<name>"` format is NOT recognized by normalize_entry_condition in
contract_v1.py, which only accepts:
  - "" / "all"                     → unconditional
  - SESSION_CONDITION_MAP keys     → session filters
  - BULL_BEAR_CONDITION_MAP keys   → bull/bear filters
  - VOL_REGIME_CONDITION_MAP keys  → vol regime filters (e.g. "vol_regime_high")
  - "<feature> <op> <value>"       → numeric comparisons
  - "symbol_XXX"                   → single-symbol routing

Any other string (including "all__vol_regime_high") raises NonExecutableConditionError,
causing the blueprint compiler to silently skip every conditioned candidate.

Fix contract
------------
The condition field must use the raw conditioning name directly (not prefixed).
If the raw name is executable (is_executable_condition returns True), use it as-is.
If it is NOT executable (e.g. "severity_bucket_extreme_5pct"), fall back to "all".
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Part 1: Prove the root cause
# ---------------------------------------------------------------------------


class TestConditionSemantics:
    """
    Establish the ground truth for what condition strings do and don't produce
    runtime enforcement nodes.

    The 'all__' prefix was added to contract_v1.py as a pass-through for
    Phase 2 discovery buckets. It does NOT raise NonExecutableConditionError,
    but it silently drops any runtime enforcement — even for buckets that have
    a valid runtime mapping (e.g., 'vol_regime_high').

    The correct fix is to have Phase 2 emit the raw condition name directly
    (without the 'all__' prefix) so that known conditions produce proper nodes.
    """

    def test_all__vol_regime_high_raises_error(self):
        """
        'all__vol_regime_high' is now strictly rejected by normalize_entry_condition.
        """
        from project.strategy.dsl.contract_v1 import (
            normalize_entry_condition,
            NonExecutableConditionError,
        )

        with pytest.raises(NonExecutableConditionError):
            normalize_entry_condition(
                "all__vol_regime_high",
                event_type="VOL_SHOCK",
                candidate_id="test_cand",
            )

    def test_plain_vol_regime_high_produces_condition_node(self):
        """'vol_regime_high' WITHOUT the prefix IS recognized and produces a runtime node."""
        from project.strategy.dsl.contract_v1 import normalize_entry_condition

        canonical, nodes, sym = normalize_entry_condition(
            "vol_regime_high",
            event_type="VOL_SHOCK",
            candidate_id="test_cand",
        )
        assert canonical == "vol_regime_high"
        assert len(nodes) == 1
        assert nodes[0].feature == "vol_regime_code"
        assert nodes[0].operator == "=="
        assert nodes[0].value == pytest.approx(2.0)
        assert sym is None

    def test_all_is_unconditional(self):
        """'all' is always executable and produces no condition nodes."""
        from project.strategy.dsl.contract_v1 import normalize_entry_condition

        canonical, nodes, sym = normalize_entry_condition(
            "all",
            event_type="VOL_SHOCK",
            candidate_id="test_cand",
        )
        assert canonical == "all"
        assert nodes == []
        assert sym is None

    def test_severity_bucket_is_not_executable(self):
        """'severity_bucket_extreme_5pct' has no runtime feature — NOT executable."""
        from project.strategy.dsl.contract_v1 import is_executable_condition

        assert not is_executable_condition("severity_bucket_extreme_5pct"), (
            "'severity_bucket_extreme_5pct' is a research label, not a runtime feature"
        )


# ---------------------------------------------------------------------------
# Part 2: Fix contract — _condition_for_cond_name helper
# ---------------------------------------------------------------------------


class TestConditionForCondName:
    """
    project.research.condition_routing must expose condition_for_cond_name(cond_name, run_symbols)
    that returns the correct DSL-safe condition string.
    """

    def test_helper_exists(self):
        """condition_for_cond_name must be importable from the canonical routing module."""
        from project.research import condition_routing

        assert hasattr(condition_routing, "condition_for_cond_name"), (
            "condition_for_cond_name() helper must be defined"
        )

    def test_all_returns_all(self):
        """cond_name='all' must return 'all'."""
        from project.research.condition_routing import condition_for_cond_name

        assert condition_for_cond_name("all") == "all"

    def test_empty_returns_all(self):
        """Empty string must return 'all'."""
        from project.research.condition_routing import condition_for_cond_name

        assert condition_for_cond_name("") == "all"

    def test_vol_regime_high_returned_directly(self):
        """'vol_regime_high' is executable — must be returned as-is (not prefixed)."""
        from project.research.condition_routing import condition_for_cond_name

        result = condition_for_cond_name("vol_regime_high")
        assert result == "vol_regime_high", (
            f"'vol_regime_high' is an executable condition; got '{result}'"
        )

    def test_vol_regime_low_returned_directly(self):
        """'vol_regime_low' is executable."""
        from project.research.condition_routing import condition_for_cond_name

        assert condition_for_cond_name("vol_regime_low") == "vol_regime_low"

    def test_session_asia_returned_directly(self):
        """'session_asia' is an executable session condition."""
        from project.research.condition_routing import condition_for_cond_name

        assert condition_for_cond_name("session_asia") == "session_asia"

    def test_severity_bucket_falls_back_to_all(self):
        """'severity_bucket_extreme_5pct' is NOT executable — must fall back to 'all'."""
        from project.research.condition_routing import condition_for_cond_name

        result = condition_for_cond_name("severity_bucket_extreme_5pct")
        assert result == "all", (
            f"Non-executable conditioning 'severity_bucket_extreme_5pct' must map to 'all'; got '{result}'"
        )

    def test_arbitrary_unknown_name_falls_back_to_all(self):
        """Unknown condition names must fall back to 'all' in permissive (strict=False) mode."""
        from project.research.condition_routing import condition_for_cond_name

        result = condition_for_cond_name("some_research_bucket_xyz", strict=False)
        assert result == "all"

    def test_returned_value_is_always_executable(self):
        """Whatever condition_for_cond_name returns must always be executable."""
        from project.research.condition_routing import condition_for_cond_name
        from project.strategy.dsl.contract_v1 import is_executable_condition

        test_inputs = [
            "all",
            "",
            "vol_regime_high",
            "vol_regime_low",
            "vol_regime_mid",
            "session_asia",
            "session_eu",
            "session_us",
            "bull_bear_bull",
            "bull_bear_bear",
            "severity_bucket_extreme_5pct",
            "severity_bucket_top_10pct",
            # Note: "some_unknown_bucket" and "col__val" are tested in permissive (strict=False) mode
            # because in strict=True mode they return '__BLOCKED__' which is intentionally not executable
        ]
        for inp in test_inputs:
            # Use permissive mode to test the safe-fallback path
            result = condition_for_cond_name(inp, strict=False)
            assert is_executable_condition(result), (
                f"condition_for_cond_name({inp!r}, strict=False) returned {result!r}, "
                f"which is not executable"
            )

    def test_all_prefix_format_never_returned(self):
        """The 'all__<name>' prefix format must NEVER be returned."""
        from project.research.condition_routing import condition_for_cond_name

        test_inputs = [
            "vol_regime_high",
            "severity_bucket_extreme_5pct",
            "session_us",
            "col_val",
            "unknown",
        ]
        for inp in test_inputs:
            # Test in both modes — legacy format must never appear in either
            for strict in (True, False):
                result = condition_for_cond_name(inp, strict=strict)
                assert not result.startswith("all__"), (
                    f"condition_for_cond_name({inp!r}, strict={strict}) returned '{result}' "
                    f"which uses the broken 'all__' prefix format"
                )
