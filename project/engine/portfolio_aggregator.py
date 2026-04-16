from __future__ import annotations

from typing import Dict, Iterable

import numpy as np
import pandas as pd


_CANONICAL_SUM_COLUMNS: tuple[str, ...] = (
    "gross_pnl",
    "net_pnl",
    "transaction_cost",
    "slippage_cost",
    "funding_pnl",
    "borrow_cost",
    "gross_exposure",
    "net_exposure",
    "turnover",
)


def _prepare_timestamp(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["timestamp"] = pd.to_datetime(prepared["timestamp"], utc=True)
    return prepared


def _aggregate_frame(
    frame: pd.DataFrame,
    *,
    extra_group_cols: Iterable[str] = (),
    capital_mode: str = "max",
) -> pd.DataFrame:
    if frame.empty:
        cols = ["timestamp", *extra_group_cols, *_CANONICAL_SUM_COLUMNS, "capital_base"]
        return pd.DataFrame(columns=cols)

    prepared = _prepare_timestamp(frame)
    group_cols = ["timestamp", *extra_group_cols]
    agg_map: dict[str, str] = {}
    for col in _CANONICAL_SUM_COLUMNS:
        if col in prepared.columns:
            agg_map[col] = "sum"
    if "capital_base" in prepared.columns:
        agg_map["capital_base"] = capital_mode

    grouped = prepared.groupby(group_cols, sort=True).agg(agg_map).reset_index()
    for col in _CANONICAL_SUM_COLUMNS:
        if col not in grouped.columns:
            grouped[col] = 0.0
    if "capital_base" not in grouped.columns:
        grouped["capital_base"] = 1.0
    return grouped.sort_values(group_cols).reset_index(drop=True)


def _build_equity_curve(
    net_pnl: pd.Series,
    capital_reference: pd.Series,
    *,
    initial_equity: float | None = None,
) -> tuple[pd.Series, pd.Series]:
    pnl = pd.to_numeric(net_pnl, errors="coerce").fillna(0.0).astype(float)
    capital = pd.to_numeric(capital_reference, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if initial_equity is None:
        if capital.notna().any():
            inferred = float(capital.ffill().bfill().iloc[0])
        else:
            inferred = 1.0
        if not np.isfinite(inferred) or inferred == 0.0:
            inferred = 1.0
        initial_equity = inferred

    equity = pd.Series(initial_equity, index=pnl.index, dtype=float) + pnl.cumsum()
    prior_equity = equity.shift(1).fillna(float(initial_equity))
    prior_equity = prior_equity.replace(0.0, np.nan)
    equity_return = (pnl / prior_equity).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return equity, equity_return


def aggregate_strategy_results(
    strategy_frames: Dict[str, pd.DataFrame],
    *,
    initial_equity: float | None = None,
) -> pd.DataFrame:
    per_strategy: dict[str, pd.DataFrame] = {}
    for strategy_name, frame in strategy_frames.items():
        if frame.empty:
            continue
        grouped = _aggregate_frame(frame, capital_mode="max")
        per_strategy[strategy_name] = grouped.set_index("timestamp")

    if not per_strategy:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "portfolio_gross_pnl",
                "portfolio_net_pnl",
                "portfolio_transaction_cost",
                "portfolio_slippage_cost",
                "portfolio_funding_pnl",
                "portfolio_borrow_cost",
                "portfolio_gross_exposure",
                "portfolio_net_exposure",
                "portfolio_turnover",
                "portfolio_capital_base",
                "portfolio_equity",
                "portfolio_equity_return",
            ]
        )

    aligned = pd.concat(per_strategy, axis=1).sort_index().fillna(0.0)

    def _sum_metric(metric: str) -> pd.Series:
        if metric not in aligned.columns.get_level_values(1):
            return pd.Series(0.0, index=aligned.index, dtype=float)
        return aligned.xs(metric, axis=1, level=1).sum(axis=1)

    capital_reference = _sum_metric("capital_base")
    portfolio_net_pnl = _sum_metric("net_pnl")
    portfolio_equity, portfolio_equity_return = _build_equity_curve(
        portfolio_net_pnl,
        capital_reference,
        initial_equity=initial_equity,
    )

    out = (
        pd.DataFrame(
            {
                "timestamp": aligned.index,
                "portfolio_gross_pnl": _sum_metric("gross_pnl").values,
                "portfolio_net_pnl": portfolio_net_pnl.values,
                "portfolio_transaction_cost": _sum_metric("transaction_cost").values,
                "portfolio_slippage_cost": _sum_metric("slippage_cost").values,
                "portfolio_funding_pnl": _sum_metric("funding_pnl").values,
                "portfolio_borrow_cost": _sum_metric("borrow_cost").values,
                "portfolio_gross_exposure": _sum_metric("gross_exposure").values,
                "portfolio_net_exposure": _sum_metric("net_exposure").values,
                "portfolio_turnover": _sum_metric("turnover").values,
                "portfolio_capital_base": capital_reference.values,
                "portfolio_equity": portfolio_equity.values,
                "portfolio_equity_return": portfolio_equity_return.values,
            }
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return out


def build_strategy_contributions(
    strategy_frames: Dict[str, pd.DataFrame],
    portfolio: pd.DataFrame,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    if portfolio.empty:
        return pd.DataFrame()
    prior_equity = portfolio[["timestamp", "portfolio_equity"]].copy()
    prior_equity["timestamp"] = pd.to_datetime(prior_equity["timestamp"], utc=True)
    prior_equity["prior_portfolio_equity"] = (
        prior_equity["portfolio_equity"]
        .shift(1)
        .fillna(prior_equity["portfolio_equity"].iloc[0] - portfolio["portfolio_net_pnl"].iloc[0])
    )
    prior_equity["prior_portfolio_equity"] = prior_equity["prior_portfolio_equity"].replace(
        0.0, np.nan
    )
    prior_equity = prior_equity[["timestamp", "prior_portfolio_equity"]]

    for strategy_name, frame in strategy_frames.items():
        if frame.empty:
            continue
        grouped = _aggregate_frame(frame, capital_mode="max")
        grouped["strategy"] = strategy_name
        parts.append(grouped)
    if not parts:
        return pd.DataFrame()
    contrib = pd.concat(parts, ignore_index=True)
    contrib = contrib.merge(prior_equity, on="timestamp", how="left")
    contrib["equity_return_contribution"] = (
        (contrib["net_pnl"] / contrib["prior_portfolio_equity"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )
    rename_map = {
        "gross_pnl": "strategy_gross_pnl",
        "net_pnl": "strategy_net_pnl",
        "transaction_cost": "strategy_transaction_cost",
        "slippage_cost": "strategy_slippage_cost",
        "funding_pnl": "strategy_funding_pnl",
        "borrow_cost": "strategy_borrow_cost",
        "gross_exposure": "strategy_gross_exposure",
        "net_exposure": "strategy_net_exposure",
        "turnover": "strategy_turnover",
        "capital_base": "strategy_capital_base",
    }
    contrib = contrib.rename(columns=rename_map)
    return contrib.sort_values(["timestamp", "strategy"]).reset_index(drop=True)


def build_symbol_contributions(
    strategy_frames: Dict[str, pd.DataFrame],
    portfolio: pd.DataFrame,
) -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame()
    prior_equity = portfolio[["timestamp", "portfolio_equity"]].copy()
    prior_equity["timestamp"] = pd.to_datetime(prior_equity["timestamp"], utc=True)
    prior_equity["prior_portfolio_equity"] = (
        prior_equity["portfolio_equity"]
        .shift(1)
        .fillna(prior_equity["portfolio_equity"].iloc[0] - portfolio["portfolio_net_pnl"].iloc[0])
    )
    prior_equity["prior_portfolio_equity"] = prior_equity["prior_portfolio_equity"].replace(
        0.0, np.nan
    )
    prior_equity = prior_equity[["timestamp", "prior_portfolio_equity"]]

    frames = [frame for frame in strategy_frames.values() if not frame.empty]
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    grouped = _aggregate_frame(combined, extra_group_cols=("symbol",), capital_mode="sum")
    grouped = grouped.merge(prior_equity, on="timestamp", how="left")
    grouped["equity_return_contribution"] = (
        (grouped["net_pnl"] / grouped["prior_portfolio_equity"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )
    rename_map = {
        "gross_pnl": "symbol_gross_pnl",
        "net_pnl": "symbol_net_pnl",
        "transaction_cost": "symbol_transaction_cost",
        "slippage_cost": "symbol_slippage_cost",
        "funding_pnl": "symbol_funding_pnl",
        "borrow_cost": "symbol_borrow_cost",
        "gross_exposure": "symbol_gross_exposure",
        "net_exposure": "symbol_net_exposure",
        "turnover": "symbol_turnover",
        "capital_base": "symbol_capital_base",
    }
    grouped = grouped.rename(columns=rename_map)
    return grouped.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def combine_strategy_symbols(symbol_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not symbol_frames:
        return pd.DataFrame()
    combined = pd.concat(symbol_frames, ignore_index=True)
    sort_cols = [col for col in ["timestamp", "symbol", "strategy"] if col in combined.columns]
    if not sort_cols:
        return combined.reset_index(drop=True)
    return combined.sort_values(sort_cols).reset_index(drop=True)
