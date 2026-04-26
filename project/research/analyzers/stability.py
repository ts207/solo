from __future__ import annotations

from typing import Any

import pandas as pd

from project.research.analyzers.base import AnalyzerResult, BaseEventAnalyzer


class StabilityAnalyzer(BaseEventAnalyzer):
    name = "stability"

    def analyze(
        self, events: pd.DataFrame, *, market: pd.DataFrame | None = None, **kwargs: Any
    ) -> AnalyzerResult:
        frame = self.validate_events(events)
        if frame.empty:
            return AnalyzerResult(name=self.name, summary={"n_events": 0}, tables={})

        return_col = kwargs.get("return_col")
        if return_col is None:
            for candidate in ("net_3_bps", "fwd_3_bps", "net_1_bps", "fwd_1_bps", "return_bps"):
                if candidate in frame.columns:
                    return_col = candidate
                    break
        if return_col is None:
            return AnalyzerResult(
                name=self.name,
                summary={"n_events": len(frame), "return_col": None},
                tables={"stability_events": frame},
            )

        returns = pd.to_numeric(frame[return_col], errors="coerce")
        summary: dict[str, Any] = {
            "n_events": len(frame),
            "return_col": str(return_col),
            "overall_mean_bps": float(returns.mean()) if returns.notna().any() else None,
            "sign_consistency": float((returns > 0).mean()) if returns.notna().any() else None,
        }
        tables: dict[str, pd.DataFrame] = {"stability_events": frame.copy()}

        if "asset" in frame.columns:
            asset_table = (
                frame.assign(_ret=returns)
                .groupby("asset", dropna=False)["_ret"]
                .agg(["count", "mean", "median", "std"])
                .reset_index()
            )
            asset_table["sign_positive_rate"] = (
                frame.assign(_ret=returns)
                .groupby("asset", dropna=False)["_ret"]
                .apply(lambda s: float((s > 0).mean()))
                .values
            )
            tables["stability_by_asset"] = asset_table
            if not asset_table.empty:
                means = pd.to_numeric(asset_table["mean"], errors="coerce")
                summary["asset_mean_dispersion"] = (
                    float(means.std()) if means.notna().sum() > 1 else 0.0
                )

        if "regime" in frame.columns:
            regime_table = (
                frame.assign(_ret=returns)
                .groupby("regime", dropna=False)["_ret"]
                .agg(["count", "mean", "median", "std"])
                .reset_index()
            )
            tables["stability_by_regime"] = regime_table
            if not regime_table.empty:
                means = pd.to_numeric(regime_table["mean"], errors="coerce")
                summary["regime_mean_dispersion"] = (
                    float(means.std()) if means.notna().sum() > 1 else 0.0
                )

        if "split" in frame.columns:
            split_table = (
                frame.assign(_ret=returns)
                .groupby("split", dropna=False)["_ret"]
                .agg(["count", "mean", "median", "std"])
                .reset_index()
            )
            tables["stability_by_split"] = split_table
            if not split_table.empty:
                means = pd.to_numeric(split_table["mean"], errors="coerce")
                summary["split_mean_dispersion"] = (
                    float(means.std()) if means.notna().sum() > 1 else 0.0
                )

        return AnalyzerResult(name=self.name, summary=summary, tables=tables)
