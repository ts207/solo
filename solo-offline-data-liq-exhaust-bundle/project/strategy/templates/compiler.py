import pandas as pd
from project.strategy.templates.spec import StrategySpec
from project.strategy.templates.data_bundle import DataBundle

import numpy as np

try:
    from numba import njit  # type: ignore
except ImportError:

    def njit(*args, **kwargs):
        if args and callable(args[0]) and not kwargs:
            return args[0]

        def _decorator(fn):
            return fn

        return _decorator


@njit
def _compile_loop(
    ent_arr: np.ndarray, ext_arr: np.ndarray, cap: float, cooldown_bars: int
) -> np.ndarray:
    n = len(ent_arr)
    pos_arr = np.zeros(n, dtype=np.float64)
    in_position = False
    cooldown_until = 0

    for i in range(n):
        if ext_arr[i] and in_position:
            pos_arr[i] = 0.0
            in_position = False
            cooldown_until = i + cooldown_bars
        elif ent_arr[i] and not in_position and i >= cooldown_until:
            pos_arr[i] = cap
            in_position = True
        elif i > 0:
            pos_arr[i] = pos_arr[i - 1]
    return pos_arr


def compile_positions(spec: StrategySpec, bundle: DataBundle) -> tuple[pd.Series, pd.DataFrame]:
    """Compile a spec into an integer position series avoiding lookahead."""
    idx = bundle.prices.index

    entries = bundle.get_event_signal(spec.primary_event_id, spec.entry_signal)
    exits = bundle.get_event_signal(spec.primary_event_id, spec.exit_signal)

    from project.strategy.templates.validation import validate_pit_invariants

    if not validate_pit_invariants(entries):
        raise ValueError(
            f"PIT violation in entry signal for spec '{spec.strategy_id}': "
            "index is not strictly monotone increasing. "
            "This indicates unsorted or lookahead-contaminated data."
        )
    if not validate_pit_invariants(exits):
        raise ValueError(
            f"PIT violation in exit signal for spec '{spec.strategy_id}': "
            "index is not strictly monotone increasing."
        )

    ent_arr = entries.to_numpy(dtype=np.bool_, copy=True)
    ext_arr = exits.to_numpy(dtype=np.bool_, copy=True)

    pos_arr = _compile_loop(ent_arr, ext_arr, float(spec.position_cap), int(spec.cooldown_bars))

    positions = pd.Series(pos_arr, index=idx)
    debug = pd.DataFrame({"entries": entries, "exits": exits, "positions": positions})
    return positions, debug
