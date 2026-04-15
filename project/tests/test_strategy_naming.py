"""Tests for strategy naming conventions and registry resolution."""

import pytest
from project.strategy.runtime.registry import (
    parse_strategy_name,
    get_strategy,
    resolve_strategy,
    is_dsl_strategy,
)


class TestParseStrategyName:
    """Tests for parse_strategy_name function."""

    def test_basic_name_no_variant(self):
        """Basic strategy name without variant returns base only."""
        base, variant = parse_strategy_name("dsl_interpreter_v1")
        assert base == "dsl_interpreter_v1"
        assert variant is None

    def test_name_with_variant(self):
        """Strategy name with variant returns both parts."""
        base, variant = parse_strategy_name("dsl_interpreter_v1__myblueprint")
        assert base == "dsl_interpreter_v1"
        assert variant == "myblueprint"

    def test_name_with_embedded_underscores(self):
        """Variant can contain additional underscores."""
        base, variant = parse_strategy_name("dsl_interpreter_v1__my__blueprint")
        assert base == "dsl_interpreter_v1"
        assert variant == "my__blueprint"

    def test_empty_base_raises(self):
        """Empty base name raises ValueError."""
        with pytest.raises(ValueError, match="empty base"):
            parse_strategy_name("__invalid")

    def test_empty_variant_raises(self):
        """Empty variant (trailing__) raises ValueError."""
        with pytest.raises(ValueError, match="empty variant"):
            parse_strategy_name("dsl_interpreter_v1__")

    def test_empty_name_raises(self):
        """Empty name raises ValueError."""
        with pytest.raises(ValueError, match="Invalid strategy name"):
            parse_strategy_name("")

    def test_whitespace_stripped(self):
        """Whitespace is stripped from name."""
        base, variant = parse_strategy_name("  dsl_interpreter_v1  ")
        assert base == "dsl_interpreter_v1"
        assert variant is None


class TestGetStrategy:
    """Tests for get_strategy function."""

    def test_get_base_strategy(self):
        """Getting a base strategy returns the strategy directly."""
        strategy = get_strategy("dsl_interpreter_v1")
        assert strategy is not None

    def test_get_aliased_strategy(self):
        """Getting a strategy with variant returns AliasedStrategy."""
        strategy = get_strategy("dsl_interpreter_v1__myblueprint")
        assert strategy is not None
        # Should be an alias, not the base
        assert type(strategy).__name__ == "_AliasedStrategy"

    def test_unknown_strategy_raises(self):
        """Unknown strategy raises ValueError."""
        with pytest.raises(ValueError, match="Unknown strategy"):
            get_strategy("nonexistent_strategy")


class TestResolveStrategy:
    """Tests for resolve_strategy function."""

    def test_resolve_no_variant(self):
        """Resolving name without variant returns None for variant."""
        resolved = resolve_strategy("dsl_interpreter_v1")
        assert resolved.base == "dsl_interpreter_v1"
        assert resolved.variant is None
        assert resolved.metadata == {}

    def test_resolve_with_variant(self):
        """Resolving name with variant returns variant in metadata."""
        resolved = resolve_strategy("dsl_interpreter_v1__myblueprint")
        assert resolved.base == "dsl_interpreter_v1"
        assert resolved.variant == "myblueprint"
        assert resolved.metadata == {"variant": "myblueprint"}


class TestIsDslStrategy:
    """Tests for is_dsl_strategy function."""

    def test_dsl_base_is_dsl(self):
        """dsl_interpreter_v1 is recognized as DSL."""
        is_dsl, variant = is_dsl_strategy("dsl_interpreter_v1")
        assert is_dsl is True
        assert variant is None

    def test_dsl_with_variant_is_dsl(self):
        """dsl_interpreter_v1__variant is recognized as DSL."""
        is_dsl, variant = is_dsl_strategy("dsl_interpreter_v1__myblueprint")
        assert is_dsl is True
        assert variant == "myblueprint"

    def test_unknown_is_not_dsl(self):
        """Unknown strategies return False."""
        is_dsl, variant = is_dsl_strategy("nonexistent__variant")
        assert is_dsl is False
        assert variant is None


class TestSensitiveParamsContract:
    """Tests verifying that variant cannot change sensitive execution params."""

    def test_variant_does_not_affect_execution_lag_default(self):
        """
        Verify that different variants resolve to the same base strategy,
        ensuring that variant name alone cannot change execution behavior.
        """
        # Different variants should resolve to the same base
        s1 = get_strategy("dsl_interpreter_v1__variant_a")
        s2 = get_strategy("dsl_interpreter_v1__variant_b")

        # Both should be AliasedStrategy wrapping the same base
        assert type(s1).__name__ == "_AliasedStrategy"
        assert type(s2).__name__ == "_AliasedStrategy"

        # The _base attribute should be the same object
        assert s1._base is s2._base

    def test_resolve_strategy_metadata_is_readonly(self):
        """
        Verify that resolve_strategy returns metadata but doesn't
        automatically merge it into params.
        """
        resolved = resolve_strategy("dsl_interpreter_v1__myblueprint")

        # Metadata is returned
        assert resolved.metadata == {"variant": "myblueprint"}

        # But the strategy itself doesn't have the variant injected
        # (this is the contract - variant is metadata only)
        assert not hasattr(resolved.strategy, "variant")
