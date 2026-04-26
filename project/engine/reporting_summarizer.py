from __future__ import annotations

import numpy as np
import pandas as pd


def summarize_pnl(series: pd.Series) -> dict[str, float]:
    if series.empty:
        return {"total_pnl": 0.0, "mean_pnl": 0.0, "std_pnl": 0.0}
    return {
        "total_pnl": float(series.sum()),
        "mean_pnl": float(series.mean()),
        "std_pnl": float(series.std()),
    }


def summarize_portfolio_ledger(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "total_pnl": 0.0,
            "mean_return": 0.0,
            "std_return": 0.0,
            "starting_equity": 0.0,
            "ending_equity": 0.0,
            "max_drawdown": 0.0,
            "max_gross_exposure": 0.0,
            "total_turnover": 0.0,
        }

    net_pnl = pd.to_numeric(frame.get("portfolio_net_pnl", 0.0), errors="coerce").fillna(0.0)
    equity_return = pd.to_numeric(
        frame.get("portfolio_equity_return", 0.0), errors="coerce"
    ).fillna(0.0)
    equity = pd.to_numeric(frame.get("portfolio_equity", 0.0), errors="coerce").fillna(0.0)
    gross_exposure = pd.to_numeric(
        frame.get("portfolio_gross_exposure", 0.0), errors="coerce"
    ).fillna(0.0)
    turnover = pd.to_numeric(frame.get("portfolio_turnover", 0.0), errors="coerce").fillna(0.0)

    starting_equity = float(equity.iloc[0] - net_pnl.iloc[0]) if len(equity) else 0.0
    equity_path = pd.concat(
        [pd.Series([starting_equity], dtype=float), equity.reset_index(drop=True)],
        ignore_index=True,
    )
    running_peak = equity_path.cummax().replace(0.0, np.nan)
    drawdown = ((equity_path / running_peak) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return {
        "total_pnl": float(net_pnl.sum()),
        "mean_return": float(equity_return.mean()),
        "std_return": float(equity_return.std()),
        "starting_equity": starting_equity,
        "ending_equity": float(equity.iloc[-1]) if len(equity) else 0.0,
        "max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "max_gross_exposure": float(gross_exposure.max()) if len(gross_exposure) else 0.0,
        "total_turnover": float(turnover.sum()),
    }


def entry_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    pos_col = next(
        (col for col in ("executed_position", "signal_position") if col in frame.columns),
        None,
    )
    if pos_col is None:
        return 0
    pos = frame.set_index("timestamp")[pos_col] if "timestamp" in frame.columns else frame[pos_col]
    prior = pos.shift(1).fillna(0)
    return int(((prior == 0) & (pos != 0)).sum())


def overlay_binding_stats(
    overlays: list[str],
    symbol: str,
    frame: pd.DataFrame,
    overlay_stats: dict[str, dict[str, int]] | None = None,
) -> dict[str, object]:
    entries = entry_count(frame)
    overlay_stats = overlay_stats or {}
    per_overlay = []
    for name in overlays:
        stats = overlay_stats.get(name, {})
        per_overlay.append(
            {
                "overlay": name,
                "symbol": symbol,
                "blocked_entries": int(stats.get("blocked_entries", 0)),
                "delayed_entries": int(stats.get("delayed_entries", 0)),
                "changed_bars": int(stats.get("changed_bars", 0)),
                "entry_count": entries,
            }
        )
    return {
        "symbol": symbol,
        "overlays": overlays,
        "binding_stats": per_overlay,
    }
