from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import pandas as pd

from project.strategy.runtime.dsl_runtime.interpreter import DslInterpreterV1 as _InterpreterImpl

# njit shim: real numba.njit if available, fallback otherwise.
try:
    from numba import njit  # type: ignore
except ImportError:

    def njit(*args, **kwargs):
        if args and callable(args[0]) and not kwargs:
            return args[0]

        def _decorator(fn):
            return fn

        return _decorator


@dataclass
class DslInterpreterV1:
    """
    Façade for the Strategy DSL interpreter.
    Logic is decomposed into project.strategy.dsl.* and project.strategy.runtime.dsl_runtime.*.
    """

    name: str = "dsl_interpreter_v1"
    required_features: List[str] = field(default_factory=lambda: ["high_96", "low_96"])

    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        impl = _InterpreterImpl(name=self.name, required_features=self.required_features)
        return impl.generate_positions(bars, features, params)


def generate_positions_numba(
    timestamps,
    close=None,
    high=None,
    low=None,
    open_prices=None,
    eligible=None,
    entry_sides=None,
    stop_offsets=None,
    target_offsets=None,
    allow_intrabar_exits=False,
    cooldown_bars=0,
    reentry_lockout_bars=0,
    arm_bars_base=0,
    time_stop_bars=10,
    break_even_r=0.0,
    trailing_stop_type=0,
    trailing_offsets=None,
    invalidation_mask=None,
    priority_randomisation=True,
    random_rolls=None,
):
    """Keyword-argument wrapper around the core numba state machine."""
    import numpy as np

    from project.strategy.runtime.dsl_runtime.interpreter import (
        generate_positions_numba as _core,
    )

    n = len(timestamps)
    if close is None:
        close = np.zeros(n, dtype=np.float64)
    if high is None:
        high = close.copy()
    if low is None:
        low = close.copy()
    if open_prices is None:
        open_prices = close.copy()
    if eligible is None:
        eligible = np.zeros(n, dtype=bool)
    if entry_sides is None:
        entry_sides = np.ones(n, dtype=np.float64)
    if stop_offsets is None:
        stop_offsets = np.zeros(n, dtype=np.float64)
    if target_offsets is None:
        target_offsets = np.zeros(n, dtype=np.float64)
    if trailing_offsets is None:
        trailing_offsets = np.zeros(n, dtype=np.float64)
    if invalidation_mask is None:
        invalidation_mask = np.zeros(n, dtype=bool)
    if random_rolls is None:
        random_rolls = np.zeros(n, dtype=np.float64)

    return _core(
        timestamps,
        open_prices,
        close,
        high,
        low,
        eligible,
        entry_sides,
        stop_offsets,
        target_offsets,
        bool(allow_intrabar_exits),
        int(cooldown_bars),
        int(reentry_lockout_bars),
        int(arm_bars_base),
        int(time_stop_bars),
        float(break_even_r),
        int(trailing_stop_type),
        trailing_offsets,
        invalidation_mask,
        bool(priority_randomisation) if isinstance(priority_randomisation, (bool, int)) else False,
        random_rolls,
    )


def _build_blueprint(raw: dict):
    """Shim that delegates to the canonical blueprint builder."""
    from project.strategy.dsl.normalize import build_blueprint

    return build_blueprint(raw)
