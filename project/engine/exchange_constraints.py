from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SymbolConstraints:
    tick_size: Optional[float]  # minimum price increment
    step_size: Optional[float]  # minimum quantity increment (lot size)
    min_notional: Optional[float]  # minimum order value in quote currency

    def round_qty(self, qty: float) -> float:
        if self.step_size is None or self.step_size <= 0.0:
            return qty
        precision = max(0, -int(math.floor(math.log10(self.step_size))))
        return round(math.floor(qty / self.step_size) * self.step_size, precision)

    def enforce_min_notional(self, qty: float, price: float) -> float:
        if self.min_notional is None or self.min_notional <= 0.0:
            return qty
        notional = abs(qty) * abs(price)
        return 0.0 if notional < self.min_notional else qty


import numpy as np
import pandas as pd


def apply_constraints(
    requested_qty: pd.Series,
    price: pd.Series,
    constraints: SymbolConstraints,
) -> pd.Series:
    """Vectorized calculation of step_size rounding and min_notional enforcement."""
    qty = requested_qty.copy()
    sign = np.sign(qty)
    abs_qty = qty.abs()

    # 1. Round to step size
    if constraints.step_size is not None and constraints.step_size > 0.0:
        precision = max(0, -int(math.floor(math.log10(constraints.step_size))))
        abs_qty = np.floor(abs_qty / constraints.step_size) * constraints.step_size
        abs_qty = abs_qty.round(precision)

    # 2. Enforce minimum notional
    if constraints.min_notional is not None and constraints.min_notional > 0.0:
        notional = abs_qty * price.abs()
        abs_qty = np.where(notional < constraints.min_notional, 0.0, abs_qty)

    return pd.Series(sign * abs_qty, index=qty.index)


def load_symbol_constraints(symbol: str, meta_dir) -> SymbolConstraints:
    """
    Load exchange filters from data/lake/raw/binance/meta/<symbol>.json.
    Returns unconstrained SymbolConstraints if file is absent.
    """
    import json
    from pathlib import Path

    path = Path(meta_dir) / f"{symbol}.json"
    if not path.exists():
        return SymbolConstraints(tick_size=None, step_size=None, min_notional=None)
    data = json.loads(path.read_text(encoding="utf-8"))
    filters = {f["filterType"]: f for f in data.get("filters", [])}
    tick = float(filters.get("PRICE_FILTER", {}).get("tickSize", 0)) or None
    step = float(filters.get("LOT_SIZE", {}).get("stepSize", 0)) or None
    notional = float(filters.get("MIN_NOTIONAL", {}).get("minNotional", 0)) or None
    return SymbolConstraints(tick_size=tick, step_size=step, min_notional=notional)
