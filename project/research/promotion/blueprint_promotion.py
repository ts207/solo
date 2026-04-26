from __future__ import annotations

import numpy as np
import pandas as pd

from project.eval.robustness import simulate_parameter_perturbation


def calculate_stressed_pnl(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {"train": 0.0, "validation": 0.0, "test": 0.0}
    gross = pd.to_numeric(frame.get("gross_pnl"), errors="coerce").fillna(
        pd.to_numeric(frame.get("pnl"), errors="coerce").fillna(0.0)
    )
    trading_cost = pd.to_numeric(frame.get("trading_cost"), errors="coerce").fillna(0.0)
    funding = pd.to_numeric(frame.get("funding_pnl"), errors="coerce").fillna(0.0)
    borrow = pd.to_numeric(frame.get("borrow_cost"), errors="coerce").fillna(0.0)
    stressed = gross - (2.0 * trading_cost) + funding - borrow
    out = {}
    for split in ("train", "validation", "test"):
        mask = frame["split_label"] == split
        out[split] = float(stressed[mask].sum())
    return out


def calculate_realized_cost_ratio(frame: pd.DataFrame) -> dict[str, dict[str, float]]:
    splits = ("train", "validation", "test")
    if frame.empty:
        return {s: {"realized_cost_ratio": 0.0} for s in splits}
    gross = pd.to_numeric(frame.get("gross_pnl"), errors="coerce").fillna(
        pd.to_numeric(frame.get("pnl"), errors="coerce").fillna(0.0)
    )
    trading_cost = pd.to_numeric(frame.get("trading_cost"), errors="coerce").fillna(0.0)
    out = {}
    eps = 1e-12
    for split in splits:
        mask = frame["split_label"] == split
        gross_abs = float(gross[mask].abs().sum())
        cost_sum = float(trading_cost[mask].sum())
        denom = max(gross_abs + cost_sum, eps)
        out[split] = {"realized_cost_ratio": float(cost_sum / denom)}
    return out


def get_loss_cluster_lengths(pnl_series: pd.Series) -> list[int]:
    values = pd.to_numeric(pnl_series, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    runs: list[int] = []
    run_len = 0
    for value in values:
        if value < 0.0:
            run_len += 1
        elif run_len > 0:
            runs.append(run_len)
            run_len = 0
    if run_len > 0:
        runs.append(run_len)
    return runs


def calculate_drawdown_metrics(frame: pd.DataFrame, split_label: str) -> dict[str, float]:
    sub = frame[frame["split_label"] == split_label].copy()
    if sub.empty:
        return {
            "max_loss_cluster_len": 0.0,
            "cluster_loss_concentration": 0.0,
            "tail_conditional_drawdown_95": 0.0,
        }

    pnl_ts = (
        pd.to_numeric(sub.get("pnl"), errors="coerce")
        .fillna(0.0)
        .groupby(sub["timestamp"], sort=True)
        .sum()
    )
    clusters = get_loss_cluster_lengths(pnl_ts)
    max_len = float(max(clusters)) if clusters else 0.0

    values = pnl_ts.to_numpy(dtype=float)
    loss_magnitudes = []
    start = None
    for idx, value in enumerate(values):
        if value < 0.0 and start is None:
            start = idx
        elif value >= 0.0 and start is not None:
            loss_magnitudes.append(float(np.abs(values[start:idx].sum())))
            start = None
    if start is not None:
        loss_magnitudes.append(float(np.abs(values[start:].sum())))

    concentration = 0.0
    if loss_magnitudes:
        sorted_losses = sorted(loss_magnitudes, reverse=True)
        k = max(1, int(np.ceil(len(sorted_losses) * 0.10)))
        concentration = float(sum(sorted_losses[:k]) / max(sum(sorted_losses), 1e-9))

    equity = (1.0 + pnl_ts.cumsum()).astype(float)
    peak = equity.cummax().replace(0.0, np.nan)
    drawdown = ((equity - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    tail_dd = float(drawdown.quantile(0.05)) if not drawdown.empty else 0.0

    return {
        "max_loss_cluster_len": max_len,
        "cluster_loss_concentration": concentration,
        "tail_conditional_drawdown_95": tail_dd,
    }


def fragility_gate(pnl_series: pd.Series, min_pass_rate: float = 0.60) -> bool:
    if pnl_series.empty:
        return False
    stats = simulate_parameter_perturbation(pnl_series, noise_std_dev=0.05, n_iterations=200)
    return float(stats.get("fraction_positive", 0.0)) >= float(min_pass_rate)
