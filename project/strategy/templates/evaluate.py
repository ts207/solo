from typing import Any, Dict, List

import pandas as pd

from project.strategy.templates.compiler import compile_positions
from project.strategy.templates.data_bundle import DataBundle
from project.strategy.templates.spec import StrategySpec

try:
    from project.engine.pnl import compute_pnl_ledger, compute_returns
except ImportError:
    pass  # We will rely on sys.path or the package root correctly mapping engine


def evaluate_candidates(
    specs: List[StrategySpec], bundle: DataBundle, engine_cfg: Dict[str, Any]
) -> pd.DataFrame:
    metrics = []

    # Pre-compute close-to-close returns once since they are spec-invariant.
    if "close" not in bundle.prices.columns:
        raise ValueError("DataBundle prices must contain a 'close' column.")

    close = bundle.prices["close"].astype(float)
    open_ = bundle.prices["open"].astype(float) if "open" in bundle.prices.columns else None

    cost_bps = engine_cfg.get("cost_bps", 5.0)
    execution_lag = int(engine_cfg.get("execution_lag_bars", 1))
    execution_mode = "next_open" if open_ is not None else "close"

    for spec in specs:
        pos, debug = compile_positions(spec, bundle)

        # Apply standard execution latency lag to prevent lookahead biases.
        if execution_lag > 0:
            pos = pos.shift(execution_lag).fillna(0).astype(int)

        ledger = compute_pnl_ledger(
            target_position=pos,
            close=close,
            open_=open_,
            execution_mode=execution_mode,
            cost_bps=cost_bps,
        )

        pnl = ledger["net_pnl"]

        pos_float = pos.astype(float)
        turnover = (pos_float - pos_float.shift(1).fillna(0.0)).abs().sum()
        trades = int(turnover / 2.0)

        total_pnl_raw = float(pnl.sum())
        net_expectancy_bps = (total_pnl_raw / trades * 10000.0) if trades > 0 else 0.0

        res = {
            "strategy_id": spec.strategy_id,
            "primary_event_id": spec.primary_event_id,
            "compat_event_family": spec.compat_event_family,
            "event_family": spec.compat_event_family,
            "entry_signal": spec.entry_signal,
            "exit_signal": spec.exit_signal,
            "trades": trades,
            "total_pnl": total_pnl_raw,
            "net_expectancy_bps": net_expectancy_bps,
            "mean_pnl": float(pnl.mean()),
            "std_pnl": float(pnl.std()),
        }
        res.update(spec.params)
        metrics.append(res)

    return pd.DataFrame(metrics)
