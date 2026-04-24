"""
PnL and returns computation utilities.

Funding Convention:
The system assumes 'longs_pay_positive' (Bybit/Binance standard).
- Longs pay funding when rate > 0.
- Shorts receive funding when rate > 0.
Source: Bybit API Documentation - Funding Rate
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Standard funding convention for Bybit perps
FUNDING_CONVENTION = "longs_pay_positive"


def compute_returns(close: pd.Series) -> pd.Series:
    """
    Compute simple close-to-close returns.
    Uses manual division so gaps resulting in NaN returns propagate instead of being smoothed.
    """
    return (close / close.shift(1)) - 1.0


# Venue-specific funding schedules (UTC hours).
# Binance UM perpetuals: 3 events/day at 00:00, 08:00, 16:00.
# Bybit Linear 8-hour contracts: same as Binance.
# Bybit Linear 4-hour contracts: 6 events/day at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00.
# Callers must select the correct schedule for the instrument — passing the Binance
# schedule (default) to a Bybit 4-hour contract understates carry by 50%.
FUNDING_HOURS_BINANCE = (0, 8, 16)
FUNDING_HOURS_BYBIT_8H = (0, 8, 16)
FUNDING_HOURS_BYBIT_4H = (0, 4, 8, 12, 16, 20)


def compute_funding_pnl_event_aligned(
    pos: pd.Series,
    funding_rate: pd.Series,
    funding_hours: tuple[int, ...] = FUNDING_HOURS_BINANCE,
) -> pd.Series:
    """
    Apply funding only on bars whose timestamp falls on a funding event hour (UTC).

    ``funding_hours`` must match the actual funding schedule of the instrument.
    Use ``FUNDING_HOURS_BINANCE`` (0, 8, 16) for Binance UM perpetuals and
    Bybit 8-hour contracts, or ``FUNDING_HOURS_BYBIT_4H`` (0, 4, 8, 12, 16, 20)
    for Bybit 4-hour contracts. Using the wrong schedule silently understates or
    overstates the funding carry for affected positions.

    Position is the prior-bar position (signal held going into the event timestamp).
    """
    if not (hasattr(pos.index, "tz") and pos.index.tz is not None and str(pos.index.tz) == "UTC"):
        raise ValueError("funding PnL alignment requires a UTC-localized index")

    aligned_pos = pos.fillna(0.0).astype(float)
    prior_pos = aligned_pos.shift(1).fillna(0.0)

    rate_aligned = pd.to_numeric(funding_rate.reindex(pos.index), errors="coerce").fillna(0.0)

    is_funding_bar = pd.Series(False, index=pos.index)
    if hasattr(pos.index, "hour"):
        is_funding_bar = pd.Series(
            pos.index.hour.isin(funding_hours) & (pos.index.minute == 0),
            index=pos.index,
        )

    raw_funding = -prior_pos * rate_aligned
    return raw_funding.where(is_funding_bar, 0.0)


def _as_series(value: float | pd.Series | None, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        return pd.to_numeric(value.reindex(index), errors="coerce").fillna(0.0).astype(float)
    return pd.Series(0.0 if value is None else float(value), index=index, dtype=float)


def build_execution_state(
    target_position: pd.Series,
    close: pd.Series,
    open_: pd.Series | None = None,
    *,
    execution_mode: str = "close",
) -> pd.DataFrame:
    """
    Build explicit per-bar execution state from target positions and prices.

    Semantics
    ---------
    ``target_position[t]`` is the desired post-decision exposure after bar ``t``.
    ``executed_position[t]`` is the exposure actually active during bar ``t``.

    Therefore, irrespective of execution mode, the position active on bar ``t``
    is the previous target: ``executed_position[t] = target_position[t-1]``.

    The execution mode changes *how* gross PnL is measured on bars where the
    exposure changes:

    - ``close``: bar ``t`` always uses close-to-close return because the fill
      is assumed to have completed at the prior close.
    - ``next_open``: bar ``t`` is decomposed into a gap leg
      ``open[t] / close[t-1] - 1`` and an intrabar leg
      ``close[t] / open[t] - 1``. Entry/exit/flip bars only accrue the portion
      of the bar for which the corresponding position was actually live.
    """
    close_aligned = pd.to_numeric(close, errors="coerce").astype(float)
    idx = close_aligned.index
    target = pd.to_numeric(target_position.reindex(idx), errors="coerce").fillna(0.0).astype(float)
    executed = target.shift(1).fillna(0.0).astype(float)
    prior_executed = executed.shift(1).fillna(0.0).astype(float)
    turnover = (executed - prior_executed).abs().astype(float)

    exec_mode = str(execution_mode).strip().lower()
    if exec_mode not in {"close", "next_open"}:
        raise ValueError(f"Unsupported execution_mode={execution_mode!r}")

    bar_ret_cc = compute_returns(close_aligned)

    open_aligned = None
    gap_ret = pd.Series(np.nan, index=idx, dtype=float)
    intrabar_ret = pd.Series(np.nan, index=idx, dtype=float)
    fill_price = pd.Series(np.nan, index=idx, dtype=float)
    change_mask = turnover > 0.0

    if exec_mode == "next_open":
        if open_ is None:
            raise ValueError("open_ is required when execution_mode='next_open'")
        open_aligned = pd.to_numeric(open_.reindex(idx), errors="coerce").astype(float)
        prev_close = close_aligned.shift(1).replace(0.0, np.nan)
        safe_open = open_aligned.replace(0.0, np.nan)
        gap_ret = (open_aligned / prev_close - 1.0).replace([np.inf, -np.inf], np.nan)
        intrabar_ret = (close_aligned / safe_open - 1.0).replace([np.inf, -np.inf], np.nan)
        fill_price.loc[change_mask] = open_aligned.loc[change_mask]
    else:
        fill_source = close_aligned.shift(1)
        fill_price.loc[change_mask] = fill_source.loc[change_mask]

    state = pd.DataFrame(
        {
            "target_position": target,
            "executed_position": executed,
            "prior_executed_position": prior_executed,
            "turnover": turnover,
            "fill_mode": exec_mode,
            "fill_price": fill_price,
            "close": close_aligned,
            "open": open_aligned
            if open_aligned is not None
            else pd.Series(np.nan, index=idx, dtype=float),
            "bar_return_close_to_close": bar_ret_cc,
            "entry_return_next_open": gap_ret,
            "intrabar_return": intrabar_ret,
        },
        index=idx,
    )

    state["gross_pnl"] = compute_bar_gross_pnl(state)

    holding_return = pd.Series(np.nan, index=idx, dtype=float)
    if exec_mode == "close":
        holding_return[:] = bar_ret_cc.values
    else:
        no_change = executed.eq(prior_executed)
        entry = prior_executed.eq(0.0) & executed.ne(0.0)
        exit_ = prior_executed.ne(0.0) & executed.eq(0.0)
        holding_return.loc[no_change] = bar_ret_cc.loc[no_change]
        holding_return.loc[entry] = intrabar_ret.loc[entry]
        holding_return.loc[exit_] = gap_ret.loc[exit_]
    state["holding_return"] = holding_return
    state["mark_price"] = close_aligned
    return state


def compute_bar_gross_pnl(execution_state: pd.DataFrame) -> pd.Series:
    idx = execution_state.index
    executed = pd.to_numeric(execution_state["executed_position"], errors="coerce").fillna(0.0)
    prior_executed = pd.to_numeric(
        execution_state["prior_executed_position"], errors="coerce"
    ).fillna(0.0)
    bar_ret_cc = pd.to_numeric(execution_state["bar_return_close_to_close"], errors="coerce")
    exec_mode = (
        str(execution_state["fill_mode"].iloc[0]).strip().lower()
        if not execution_state.empty
        else "close"
    )

    if exec_mode == "close":
        gross = executed * bar_ret_cc
        return gross.replace([np.inf, -np.inf], np.nan)

    gap_ret = pd.to_numeric(execution_state["entry_return_next_open"], errors="coerce")
    intrabar_ret = pd.to_numeric(execution_state["intrabar_return"], errors="coerce")

    gross = pd.Series(0.0, index=idx, dtype=float)
    no_change = executed.eq(prior_executed)
    changed = ~no_change

    gross.loc[no_change] = prior_executed.loc[no_change] * bar_ret_cc.loc[no_change]
    gross.loc[changed] = (
        prior_executed.loc[changed] * gap_ret.loc[changed]
        + executed.loc[changed] * intrabar_ret.loc[changed]
    )
    return gross.replace([np.inf, -np.inf], np.nan)


def compute_transaction_cost(turnover: pd.Series, cost_bps: float | pd.Series) -> pd.Series:
    turnover_aligned = pd.to_numeric(turnover, errors="coerce").fillna(0.0).abs().astype(float)
    cost_bps_aligned = _as_series(cost_bps, turnover_aligned.index).clip(lower=0.0)
    return turnover_aligned * (cost_bps_aligned / 10000.0)


def compute_slippage_cost(
    turnover: pd.Series, slippage_bps: float | pd.Series | None = None
) -> pd.Series:
    turnover_aligned = pd.to_numeric(turnover, errors="coerce").fillna(0.0).abs().astype(float)
    slippage_bps_aligned = _as_series(slippage_bps, turnover_aligned.index).clip(lower=0.0)
    return turnover_aligned * (slippage_bps_aligned / 10000.0)


def compute_pnl_ledger(
    target_position: pd.Series,
    close: pd.Series,
    *,
    open_: pd.Series | None = None,
    execution_mode: str = "close",
    cost_bps: float | pd.Series = 0.0,
    slippage_bps: float | pd.Series | None = None,
    funding_rate: pd.Series | None = None,
    borrow_rate: pd.Series | None = None,
    capital_base: float | pd.Series = 1.0,
    use_event_aligned_funding: bool = True,
    funding_hours: tuple[int, ...] = FUNDING_HOURS_BINANCE,
) -> pd.DataFrame:
    """Compute an explicit per-bar execution and PnL ledger."""
    # Guard against funding overcount on sub-hourly frequencies
    if not use_event_aligned_funding and funding_rate is not None:
        idx = target_position.index
        if len(idx) > 1:
            # Check if bar duration is less than 1 hour
            delta = idx[1] - idx[0]
            if delta < pd.Timedelta(hours=1):
                raise ValueError(
                    f"Sub-hourly bars ({delta}) require use_event_aligned_funding=True "
                    "to prevent funding overcount."
                )

    state = build_execution_state(
        target_position=target_position,
        close=close,
        open_=open_,
        execution_mode=execution_mode,
    )

    transaction_cost = compute_transaction_cost(state["turnover"], cost_bps)
    slippage_cost = compute_slippage_cost(state["turnover"], slippage_bps)

    executed = pd.to_numeric(state["executed_position"], errors="coerce").fillna(0.0)
    funding_rate_aligned = _as_series(funding_rate, state.index)
    borrow_rate_aligned = _as_series(borrow_rate, state.index)

    if funding_rate is None:
        funding_pnl = pd.Series(0.0, index=state.index, dtype=float)
    elif use_event_aligned_funding:
        funding_pnl = compute_funding_pnl_event_aligned(
            executed, funding_rate_aligned, funding_hours=funding_hours
        )
    else:
        funding_pnl = -executed * funding_rate_aligned

    borrow_cost = executed.clip(upper=0.0).abs() * borrow_rate_aligned
    capital_base_aligned = _as_series(capital_base, state.index).replace(0.0, np.nan)

    ledger = state.copy()
    ledger["transaction_cost"] = transaction_cost
    ledger["slippage_cost"] = slippage_cost
    ledger["funding_pnl"] = funding_pnl
    ledger["borrow_cost"] = borrow_cost
    ledger["net_pnl"] = (
        ledger["gross_pnl"]
        - ledger["transaction_cost"]
        - ledger["slippage_cost"]
        + ledger["funding_pnl"]
        - ledger["borrow_cost"]
    )
    ledger["gross_exposure"] = ledger["executed_position"].abs()
    ledger["net_exposure"] = ledger["executed_position"]
    ledger["capital_base"] = capital_base_aligned.ffill().fillna(1.0)
    ledger["equity_return"] = (ledger["net_pnl"] / capital_base_aligned).replace(
        [np.inf, -np.inf], np.nan
    )

    numeric_zero_on_nan = [
        "gross_pnl",
        "transaction_cost",
        "slippage_cost",
        "funding_pnl",
        "borrow_cost",
        "net_pnl",
        "equity_return",
    ]
    nan_mask = ledger["bar_return_close_to_close"].isna()
    if execution_mode == "next_open":
        nan_mask = (
            nan_mask & ledger["entry_return_next_open"].isna() & ledger["intrabar_return"].isna()
        )
    if nan_mask.any():
        for col in numeric_zero_on_nan:
            ledger.loc[nan_mask, col] = 0.0

    return ledger


def compute_pnl(
    target_position: pd.Series,
    close: pd.Series,
    cost_bps: float | pd.Series = 0.0,
    *,
    open_: pd.Series | None = None,
    execution_mode: str = "close",
    funding_rate: pd.Series | None = None,
    borrow_rate: pd.Series | None = None,
    capital_base: float | pd.Series = 1.0,
) -> pd.Series:
    """Primary PnL API.  Delegates to :func:`compute_pnl_ledger`.

    Parameters
    ----------
    target_position:
        Desired end-of-bar exposure (``+1`` long, ``-1`` short, ``0`` flat).
    close:
        Per-bar close prices aligned to ``target_position``.
    cost_bps:
        Round-trip transaction cost in basis points (scalar or per-bar Series).
    open_:
        Per-bar open prices; required when ``execution_mode='next_open'``.
    execution_mode:
        ``'close'`` (default) or ``'next_open'``.
    funding_rate:
        Optional per-bar funding rate (event-aligned by default).
    borrow_rate:
        Optional per-bar borrow/short-fee rate.
    capital_base:
        Notional capital used to compute ``equity_return``.

    Returns
    -------
    pd.Series
        Net PnL per bar (``ledger['net_pnl']``).
    """
    ledger = compute_pnl_ledger(
        target_position=target_position,
        close=close,
        open_=open_,
        execution_mode=execution_mode,
        cost_bps=cost_bps,
        funding_rate=funding_rate,
        borrow_rate=borrow_rate,
        capital_base=capital_base,
    )
    return ledger["net_pnl"]
