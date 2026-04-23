from __future__ import annotations

from typing import Any, Iterable

import numpy as np
import pandas as pd

from project.research.analyzers.base import (
    AnalyzerResult,
    BaseEventAnalyzer,
    ensure_timestamp,
    resolve_event_time_column,
    resolve_market_time_column,
    resolve_price_column,
)


class EdgeAnalyzer(BaseEventAnalyzer):
    name = "edge"

    def analyze(
        self, events: pd.DataFrame, *, market: pd.DataFrame | None = None, **kwargs: Any
    ) -> AnalyzerResult:
        frame = self.validate_events(events)
        market = self.validate_market(market)
        if frame.empty or market is None or market.empty:
            return AnalyzerResult(name=self.name, summary={"n_events": int(len(frame))}, tables={})

        horizons = list(kwargs.get("horizons", [1, 3, 5]))
        cost_bps = float(kwargs.get("cost_bps", 0.0))

        time_col = resolve_event_time_column(frame)
        market_time_col = resolve_market_time_column(market)
        price_col = resolve_price_column(market)
        frame = frame.copy()
        frame[time_col] = ensure_timestamp(frame[time_col])
        market = market.copy()
        market[market_time_col] = ensure_timestamp(market[market_time_col])
        market = (
            market.dropna(subset=[market_time_col, price_col])
            .sort_values(market_time_col)
            .reset_index(drop=True)
        )
        market = market.rename(columns={market_time_col: "_ts", price_col: "_px"})
        market["_idx"] = np.arange(len(market), dtype=int)

        joined = pd.merge_asof(
            frame.sort_values(time_col),
            market[["_ts", "_px", "_idx"]],
            left_on=time_col,
            right_on="_ts",
            direction="forward",
        )
        px = market["_px"]
        for horizon in horizons:
            col = f"fwd_{int(horizon)}_bps"
            joined[col] = np.nan
            for i, row in joined.iterrows():
                idx = row.get("_idx")
                if pd.isna(idx):
                    continue
                idx = int(idx)
                if idx + horizon < len(px):
                    joined.at[i, col] = (
                        float(px.iloc[idx + horizon]) / float(px.iloc[idx]) - 1.0
                    ) * 10000.0
            joined[f"net_{int(horizon)}_bps"] = joined[col] - cost_bps

        rows = []
        for horizon in horizons:
            gross_col = f"fwd_{int(horizon)}_bps"
            net_col = f"net_{int(horizon)}_bps"
            gross = pd.to_numeric(joined[gross_col], errors="coerce")
            net = pd.to_numeric(joined[net_col], errors="coerce")
            rows.append(
                {
                    "horizon_bars": int(horizon),
                    "n_obs": int(gross.notna().sum()),
                    "mean_bps": float(gross.mean()) if gross.notna().any() else None,
                    "median_bps": float(gross.median()) if gross.notna().any() else None,
                    "trimmed_mean_bps": float(
                        gross.clip(lower=gross.quantile(0.1), upper=gross.quantile(0.9)).mean()
                    )
                    if gross.notna().any()
                    else None,
                    "win_rate": float((gross > 0).mean()) if gross.notna().any() else None,
                    "net_mean_bps": float(net.mean()) if net.notna().any() else None,
                    "net_win_rate": float((net > 0).mean()) if net.notna().any() else None,
                }
            )
        horizon_table = pd.DataFrame(rows)
        summary = {
            "n_events": int(len(joined)),
            "cost_bps": cost_bps,
            "best_horizon_bars": int(
                horizon_table.sort_values("net_mean_bps", ascending=False).iloc[0]["horizon_bars"]
            )
            if not horizon_table.empty
            else None,
            "best_net_mean_bps": float(horizon_table["net_mean_bps"].max())
            if not horizon_table.empty
            else None,
        }
        return AnalyzerResult(
            name=self.name,
            summary=summary,
            tables={"edge_horizons": horizon_table, "edge_events": joined},
        )
