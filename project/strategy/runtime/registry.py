from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from project.strategy.runtime.base import Strategy
from project.strategy.runtime.dsl_interpreter_v1 import DslInterpreterV1

_REGISTRY: dict[str, Strategy] = {
    "dsl_interpreter_v1": DslInterpreterV1(),
}

_SYMBOL_SUFFIX_PATTERN = re.compile(r"^(?P<base>[a-z0-9_]+)_(?P<symbol>[A-Z0-9]+)$")


def parse_strategy_name(name: str) -> tuple[str, str | None]:
    """
    Parse a strategy name into (base, variant).

    Examples:
        "dsl_interpreter_v1" -> ("dsl_interpreter_v1", None)
        "dsl_interpreter_v1__myblueprint" -> ("dsl_interpreter_v1", "myblueprint")
        "dsl_interpreter_v1__my__blueprint" -> ("dsl_interpreter_v1", "my__blueprint")
        "__invalid" -> raises ValueError (empty base)
        "dsl_interpreter_v1__" -> raises ValueError (empty variant)

    Returns:
        tuple of (base_strategy_name, variant_or_none)
    """
    if not name or not name.strip():
        raise ValueError(f"Invalid strategy name: {name!r}")

    base, sep, variant = name.strip().partition("__")

    if not base:
        raise ValueError(f"Invalid strategy name (empty base): {name!r}")

    if sep and variant == "":
        raise ValueError(f"Invalid strategy name (empty variant): {name!r}")

    return base, (variant if sep else None)


@dataclass
class _AliasedStrategy:
    name: str
    _base: Strategy

    @property
    def required_features(self) -> list[str]:
        return list(getattr(self._base, "required_features", []) or [])

    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        out = self._base.generate_positions(bars, features, dict(params or {}))
        if not hasattr(out, "attrs"):
            return out
        metadata = out.attrs.get("strategy_metadata", {}) if isinstance(out.attrs, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.setdefault("base_strategy_id", getattr(self._base, "name", ""))
        metadata["strategy_id"] = self.name
        out.attrs["strategy_metadata"] = metadata
        return out


@dataclass
class _SymbolScopedStrategy:
    name: str
    symbol: str
    _base: Strategy

    @property
    def required_features(self) -> list[str]:
        return list(getattr(self._base, "required_features", []) or [])

    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        merged_params = dict(params or {})
        merged_params.setdefault("strategy_symbol", self.symbol)
        out = self._base.generate_positions(bars, features, merged_params)
        if not hasattr(out, "attrs"):
            return out
        metadata = out.attrs.get("strategy_metadata", {}) if isinstance(out.attrs, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.setdefault("base_strategy_id", getattr(self._base, "name", ""))
        metadata["strategy_id"] = self.name
        metadata["strategy_symbol"] = self.symbol
        out.attrs["strategy_metadata"] = metadata
        return out


def get_strategy(name: str) -> Strategy:
    key = name.strip()
    base, variant = parse_strategy_name(key)

    if base in _REGISTRY:
        if variant is not None:
            return _AliasedStrategy(name=key, _base=_REGISTRY[base])
        return _REGISTRY[base]

    match = _SYMBOL_SUFFIX_PATTERN.match(key)
    if match:
        base_name = match.group("base")
        symbol = match.group("symbol")
        if base_name in _REGISTRY:
            return _SymbolScopedStrategy(name=key, symbol=symbol, _base=_REGISTRY[base_name])

    available = ", ".join(sorted(_REGISTRY.keys()))
    raise ValueError(f"Unknown strategy '{name}'. Available strategies: {available}")


@dataclass
class ResolvedStrategy:
    strategy: Strategy
    base: str
    variant: str | None
    metadata: dict


def resolve_strategy(name: str) -> ResolvedStrategy:
    """
    Resolve a strategy name to a strategy instance with metadata.

    Returns a ResolvedStrategy containing:
    - strategy: the Strategy instance
    - base: the base strategy name (e.g., "dsl_interpreter_v1")
    - variant: the variant part if present (e.g., "myblueprint"), None otherwise
    - metadata: dict with "variant" key if variant is present
    """
    base, variant = parse_strategy_name(name)
    strategy = get_strategy(name)
    metadata = {"variant": variant} if variant is not None else {}
    return ResolvedStrategy(
        strategy=strategy,
        base=base,
        variant=variant,
        metadata=metadata,
    )


def is_dsl_strategy(name: str) -> tuple[bool, str | None]:
    """
    Check if a strategy name resolves to a DSL interpreter strategy.

    Returns:
        (is_dsl, variant) - tuple of (whether it's a DSL strategy, the variant if any)
    """
    try:
        resolved = resolve_strategy(name)
    except ValueError:
        return False, None

    is_dsl = resolved.base == "dsl_interpreter_v1"

    return is_dsl, resolved.variant


def list_strategies() -> list[str]:
    return sorted(_REGISTRY.keys())
