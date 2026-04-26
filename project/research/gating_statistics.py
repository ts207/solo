from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.core.stats import bh_adjust, canonical_bh_group_key, newey_west_t_stat_for_mean


def _pick_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for col in candidates:
        if col in frame.columns:
            return col
    return None


def _group_value(row: pd.Series, column: str, default: str = "UNKNOWN") -> str:
    if column == "canonical_family":
        return str(row.get("research_family", row.get("canonical_family", default)))
    if column == "research_family":
        return str(row.get("research_family", row.get("canonical_family", default)))
    return str(row.get(column, default))


def apply_statistical_gates(
    candidates: pd.DataFrame,
    gate_spec: dict[str, Any],
) -> pd.DataFrame:
    """Apply basic statistical gating and BH-FDR correction to candidate rows."""
    out = candidates.copy()
    if out.empty:
        return out

    p_col = _pick_column(out, ("p_value", "p", "expectancy_p_value"))
    if p_col is None:
        return out

    alpha = float(gate_spec.get("alpha", gate_spec.get("q_threshold", 0.05)))
    pvals = pd.to_numeric(out[p_col], errors="coerce").fillna(1.0).to_numpy(dtype=float)
    out["bh_q_value"] = bh_adjust(pvals)
    # Gate on BH q-value only — the BH procedure is designed to replace the raw
    # threshold, not compound it.  ANDing both conditions inflates Type II errors.
    out["passes_statistical_gate"] = out["bh_q_value"] <= alpha
    if "group_key" not in out.columns:
        group_cols = tuple(gate_spec.get("group_columns", ()))
        if group_cols:
            out["group_key"] = [
                canonical_bh_group_key(
                    canonical_family=_group_value(row, group_cols[0]) if group_cols else "UNKNOWN",
                    canonical_event_type=_group_value(
                        row,
                        group_cols[1] if len(group_cols) > 1 else group_cols[0],
                    )
                    if group_cols
                    else "UNKNOWN",
                    template_verb=_group_value(row, group_cols[2], "verb")
                    if len(group_cols) > 2
                    else "verb",
                    horizon=_group_value(row, group_cols[3], str(gate_spec.get("horizon", "unknown")))
                    if len(group_cols) > 3
                    else str(gate_spec.get("horizon", "unknown")),
                )
                for _, row in out.iterrows()
            ]
    return out


def calculate_quality_scores(
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate a bounded composite quality score from available candidate metrics."""
    out = candidates.copy()
    if out.empty:
        return out

    score = pd.Series(0.5, index=out.index, dtype=float)
    metrics = []
    for col in ("profit_density_score", "selection_score", "expectancy_score", "stability_score"):
        if col in out.columns:
            metrics.append(pd.to_numeric(out[col], errors="coerce").fillna(0.0).clip(0.0, 1.0))
    if metrics:
        stacked = pd.concat(metrics, axis=1)
        score = stacked.mean(axis=1).clip(0.0, 1.0)
    else:
        ret_col = _pick_column(out, ("returns", "ret", "pnl", "expectancy_bps"))
        if ret_col is not None:
            series = pd.to_numeric(out[ret_col], errors="coerce").fillna(0.0)
            nw = newey_west_t_stat_for_mean(series)
            if np.isfinite(nw.t_stat):
                score = pd.Series(1.0 / (1.0 + np.exp(-np.clip(nw.t_stat, -8.0, 8.0))), index=out.index)
    out["quality_score"] = score.astype(float).clip(0.0, 1.0)
    return out
