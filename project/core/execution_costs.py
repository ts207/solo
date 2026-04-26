from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd

from project.core.config import load_configs


@dataclass(frozen=True)
class ResolvedExecutionCosts:
    config_paths: List[str]
    config: Dict[str, Any]
    fee_bps_per_side: float
    slippage_bps_per_fill: float
    round_trip_cost_bps: float
    slippage_model: str
    impact_coefficient_scaling: bool
    cost_bps: float
    execution_model: Dict[str, float]
    config_digest: str


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _default_config_paths(project_root: Path) -> List[Path]:
    candidate_dirs = [
        project_root / "configs",
        project_root / "project" / "configs",
    ]
    for cfg in candidate_dirs:
        if cfg.exists():
            return [cfg / "pipeline.yaml", cfg / "fees.yaml"]
    cfg = candidate_dirs[0]
    return [cfg / "pipeline.yaml", cfg / "fees.yaml"]


def _resolve_config_paths(project_root: Path, config_paths: Sequence[str] | None) -> List[Path]:
    paths = _default_config_paths(project_root)
    for raw in list(config_paths or []):
        path = Path(str(raw))
        if not path.is_absolute():
            path = project_root / path
        paths.append(path)
    return paths


def resolve_execution_costs(
    *,
    project_root: Path,
    config_paths: Sequence[str] | None,
    fees_bps: float | None,
    slippage_bps: float | None,
    cost_bps: float | None,
) -> ResolvedExecutionCosts:
    paths = _resolve_config_paths(project_root, config_paths)
    merged = load_configs([str(path) for path in paths])

    fee = float(fees_bps) if fees_bps is not None else float(merged.get("fee_bps_per_side", 4.0))
    slippage = (
        float(slippage_bps)
        if slippage_bps is not None
        else float(merged.get("slippage_bps_per_fill", 2.0))
    )
    slippage_model = str(merged.get("slippage_model", "fixed")).strip().lower()
    impact_scaling = bool(merged.get("impact_coefficient_scaling", False))
    cost = float(cost_bps) if cost_bps is not None else float(fee + slippage)
    round_trip_cost = float(2.0 * cost)

    execution_model_raw = merged.get("execution_model", {})
    execution_model = dict(execution_model_raw) if isinstance(execution_model_raw, dict) else {}
    execution_model.setdefault("base_fee_bps", float(fee))
    execution_model.setdefault("base_slippage_bps", float(slippage))
    execution_model.setdefault("slippage_model", slippage_model)
    execution_model.setdefault("impact_scaling", float(impact_scaling))

    payload = {
        "config_paths": [str(path) for path in paths],
        "fee_bps_per_side": float(fee),
        "slippage_bps_per_fill": float(slippage),
        "round_trip_cost_bps": float(round_trip_cost),
        "slippage_model": slippage_model,
        "impact_coefficient_scaling": impact_scaling,
        "cost_bps": float(cost),
        "execution_model": execution_model,
    }
    digest = _sha256_text(json.dumps(payload, sort_keys=True, default=str))
    return ResolvedExecutionCosts(
        config_paths=[str(path) for path in paths],
        config=merged,
        fee_bps_per_side=float(fee),
        slippage_bps_per_fill=float(slippage),
        round_trip_cost_bps=float(round_trip_cost),
        slippage_model=slippage_model,
        impact_coefficient_scaling=impact_scaling,
        cost_bps=float(cost),
        execution_model={str(k): float(v) for k, v in execution_model.items() if _is_floatable(v)},
        config_digest=digest,
    )


def estimate_transaction_cost_bps(
    frame: pd.DataFrame,
    turnover: pd.Series,
    config: Dict[str, float],
) -> pd.Series:
    """
    Estimate one-way per-turnover transaction cost in bps from spread, volatility,
    and liquidity proxies.

    The returned series is a *single-fill / per-side* cost. Ledger code multiplies
    this by realized turnover, so a full enter+exit trade pays the value twice via
    two separate one-way turnovers. Candidate-level evaluators that subtract cost
    once per completed trade should use ``2 * estimate_transaction_cost_bps(...)``
    or the explicit ``round_trip_cost_bps`` returned by ``resolve_execution_costs``.

    Supports both a static model and a dynamic model with spread, volatility, liquidity,
    and impact components. The function lives in `project.core` so portfolio, engine,
    and research code can share it without crossing architectural boundaries.
    """
    idx = turnover.index
    turnover = pd.to_numeric(turnover.reindex(idx), errors="coerce").fillna(0.0).abs()

    model_type = str(config.get("cost_model", "static")).strip().lower()
    if model_type in {"execution_simulator_v2", "fill_model_v2"}:
        return estimate_execution_model_v2_cost_bps(frame, turnover, config)

    min_tob_coverage = float(config.get("min_tob_coverage", 0.0))

    base_fee_bps = float(config.get("base_fee_bps", 0.0))
    base_slippage_bps = float(config.get("base_slippage_bps", 0.0))
    cap_bps = float(config.get("max_cost_bps_cap", 150.0))

    if model_type == "static":
        return pd.Series(base_fee_bps + base_slippage_bps, index=idx).clip(lower=0.0, upper=cap_bps)

    spread_weight = float(config.get("spread_weight", 0.5))
    volatility_weight = float(config.get("volatility_weight", 0.1))
    liquidity_weight = float(config.get("liquidity_weight", 0.1))
    impact_weight = float(config.get("impact_weight", 0.1))

    tob_coverage = (
        pd.to_numeric(
            frame.get("tob_coverage", pd.Series(0.0, index=idx)),
            errors="coerce",
        )
        .reindex(idx)
        .fillna(0.0)
    )
    spread = pd.to_numeric(
        frame.get("spread_bps", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)
    depth = pd.to_numeric(
        frame.get("depth_usd", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)

    use_dynamic = (tob_coverage >= min_tob_coverage) & spread.notna()

    atr = pd.to_numeric(frame.get("atr_14", pd.Series(np.nan, index=idx)), errors="coerce").reindex(
        idx
    )
    close = pd.to_numeric(
        frame.get("close", pd.Series(np.nan, index=idx)), errors="coerce"
    ).reindex(idx)
    high = pd.to_numeric(frame.get("high", pd.Series(np.nan, index=idx)), errors="coerce").reindex(
        idx
    )
    low = pd.to_numeric(frame.get("low", pd.Series(np.nan, index=idx)), errors="coerce").reindex(
        idx
    )
    quote_vol = pd.to_numeric(
        frame.get("quote_volume", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)

    range_bps = (((high - low) / close.replace(0.0, np.nan)) * 10000.0).replace(
        [np.inf, -np.inf], np.nan
    )
    atr_bps = ((atr / close.replace(0.0, np.nan)) * 10000.0).replace([np.inf, -np.inf], np.nan)
    vol_bps = atr_bps.fillna(range_bps).fillna(0.0).abs()

    available_liquidity = depth.fillna(quote_vol).replace(0.0, np.nan).fillna(1e6).clip(lower=1.0)
    participation_rate = (turnover / available_liquidity).clip(lower=0.0)
    impact_sqrt = np.sqrt(participation_rate)

    max_part = max(1e-4, float(config.get("max_participation_rate", 0.10)))
    participation_penalty = (
        np.exp(np.clip((participation_rate - max_part) / max_part, 0.0, 5.0)) - 1.0
    )

    liq_scale = (1.0 / available_liquidity).replace([np.inf, -np.inf], np.nan)
    liq_scale = liq_scale.fillna(liq_scale.median() if liq_scale.notna().any() else 0.0).clip(
        lower=0.0
    )
    if float(liq_scale.max()) > 0:
        liq_scale = liq_scale / float(liq_scale.max())

    dynamic_slippage = (
        (spread_weight * spread.fillna(base_slippage_bps))
        + (volatility_weight * vol_bps)
        + (liquidity_weight * (liq_scale * 10.0))
        + (impact_weight * (impact_sqrt * 10.0 + participation_penalty * 50.0))
    )

    cost_bps = base_fee_bps + np.where(use_dynamic, dynamic_slippage, base_slippage_bps)
    return pd.Series(cost_bps, index=idx).clip(lower=0.0, upper=cap_bps).astype(float)


def estimate_fill_probability_v2(
    *,
    order_size: float,
    liquidity_available: float,
    spread_bps: float,
    vol_regime_bps: float,
    urgency: str,
    profile: str,
) -> float:
    urgency_norm = str(urgency or "base").strip().lower()
    profile_norm = str(profile or "base").strip().lower()
    if urgency_norm == "aggressive":
        base_prob = 1.0
    elif urgency_norm == "passive":
        base_prob = 0.8
    elif urgency_norm == "delayed_aggressive":
        base_prob = 0.95
    else:
        base_prob = 0.9

    if profile_norm == "optimistic":
        profile_mult = 1.1
    elif profile_norm == "stressed":
        profile_mult = 0.6
    else:
        profile_mult = 1.0

    participation_rate = abs(float(order_size)) / max(1.0, float(liquidity_available))
    participation_impact = np.exp(-participation_rate * 5.0)
    vol_impact = (
        np.exp(-(float(vol_regime_bps) / 10_000.0) * 2.0) if urgency_norm == "passive" else 1.0
    )
    spread_impact = np.exp(-max(0.0, float(spread_bps) - 1.0) / 100.0)
    fill_prob = base_prob * profile_mult * participation_impact * vol_impact * spread_impact
    return float(np.clip(fill_prob, 0.0, 1.0))


def estimate_slippage_bps_v2(
    *,
    order_size: float,
    spread_bps: float,
    liquidity_available: float,
    vol_regime_bps: float,
    urgency: str,
    profile: str,
    passive_adverse_selection_bps: float = 0.2,
) -> float:
    urgency_norm = str(urgency or "base").strip().lower()
    profile_norm = str(profile or "base").strip().lower()
    if urgency_norm == "aggressive":
        base_slippage = float(spread_bps) * 0.5
    elif urgency_norm == "passive":
        base_slippage = float(passive_adverse_selection_bps)
    elif urgency_norm == "delayed_aggressive":
        base_slippage = float(spread_bps) * 0.7
    else:
        base_slippage = float(spread_bps) * 0.5

    if profile_norm == "optimistic":
        profile_mult = 0.8
        impact_sqrt_mult = 5.0
    elif profile_norm == "stressed":
        profile_mult = 1.5
        impact_sqrt_mult = 20.0
    else:
        profile_mult = 1.0
        impact_sqrt_mult = 10.0

    participation_rate = abs(float(order_size)) / max(1.0, float(liquidity_available))
    impact_bps = np.sqrt(participation_rate) * impact_sqrt_mult
    vol_impact_bps = float(vol_regime_bps) * 0.1
    total_slippage_bps = (base_slippage + impact_bps + vol_impact_bps) * profile_mult
    return float(np.clip(total_slippage_bps, 0.0, 1000.0))


def estimate_execution_model_v2_cost_bps(
    frame: pd.DataFrame,
    turnover: pd.Series,
    config: Dict[str, float],
) -> pd.Series:
    idx = turnover.index
    turnover_abs = pd.to_numeric(turnover.reindex(idx), errors="coerce").fillna(0.0).abs()
    spread = (
        pd.to_numeric(
            frame.get(
                "spread_bps", pd.Series(float(config.get("base_slippage_bps", 1.0)), index=idx)
            ),
            errors="coerce",
        )
        .reindex(idx)
        .fillna(float(config.get("base_slippage_bps", 1.0)))
    )
    depth = (
        pd.to_numeric(
            frame.get("depth_usd", frame.get("liquidity_available", pd.Series(1e6, index=idx))),
            errors="coerce",
        )
        .reindex(idx)
        .fillna(1e6)
        .clip(lower=1.0)
    )
    close = pd.to_numeric(
        frame.get("close", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)
    high = pd.to_numeric(
        frame.get("high", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)
    low = pd.to_numeric(
        frame.get("low", pd.Series(np.nan, index=idx)),
        errors="coerce",
    ).reindex(idx)
    range_bps = (((high - low) / close.replace(0.0, np.nan)) * 10_000.0).replace(
        [np.inf, -np.inf], np.nan
    )
    vol_regime = (
        pd.to_numeric(
            frame.get("vol_regime_bps", range_bps),
            errors="coerce",
        )
        .reindex(idx)
        .fillna(range_bps)
        .fillna(0.0)
        .abs()
    )

    fee_bps = float(config.get("base_fee_bps", config.get("fee_bps_per_side", 0.0)))
    urgency = str(config.get("urgency", config.get("order_urgency", "aggressive")))
    profile = str(config.get("fill_profile", config.get("profile", "base")))
    passive_bps = float(config.get("passive_adverse_selection_bps", 0.2))
    cap_bps = float(config.get("max_cost_bps_cap", 150.0))

    slippage = [
        estimate_slippage_bps_v2(
            order_size=float(turnover_abs.iloc[i]),
            spread_bps=float(spread.iloc[i]),
            liquidity_available=float(depth.iloc[i]),
            vol_regime_bps=float(vol_regime.iloc[i]),
            urgency=urgency,
            profile=profile,
            passive_adverse_selection_bps=passive_bps,
        )
        for i in range(len(idx))
    ]
    cost = pd.Series(np.array(slippage, dtype=float) + fee_bps, index=idx)
    return cost.clip(lower=0.0, upper=cap_bps).astype(float)


def _is_floatable(value: object) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False
