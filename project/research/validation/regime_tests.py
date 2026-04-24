from __future__ import annotations

import json
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd

from project.core.coercion import as_bool, safe_float
from project.core.exceptions import DataIntegrityError
from project.research.validation.schemas import StabilityResult


def _parse_mapping(value: Any) -> Dict[str, float]:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            num = safe_float(v, np.nan)
            if np.isfinite(num):
                out[str(k)] = float(num)
        return out
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise DataIntegrityError(f"Failed to parse stability mapping JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise DataIntegrityError("Stability mapping payload must be a JSON object")
        return _parse_mapping(parsed)
    return {}


def compute_regime_labels(
    df: pd.DataFrame, *, vol_col: str = "vol_regime", out_col: str = "regime"
) -> pd.DataFrame:
    out = df.copy()
    if out_col in out.columns and out[out_col].notna().any():
        return out
    if vol_col in out.columns:
        out[out_col] = out[vol_col].astype("object").fillna("unknown")
        return out
    for candidate in ("liquidity_state", "market_liquidity_state", "depth_state"):
        if candidate in out.columns:
            out[out_col] = out[candidate].astype("object").fillna("unknown")
            return out
    out[out_col] = "unknown"
    return out


def evaluate_by_regime(
    df: pd.DataFrame, *, value_col: str, regime_col: str = "regime"
) -> Dict[str, Any]:
    if df.empty or value_col not in df.columns or regime_col not in df.columns:
        return {
            "by_regime": {},
            "regime_flip_flag": False,
            "worst_regime_estimate": 0.0,
            "num_regimes": 0,
        }
    frame = df[[value_col, regime_col]].copy()
    frame[value_col] = pd.to_numeric(frame[value_col], errors="coerce")
    frame = frame.dropna(subset=[value_col])
    if frame.empty:
        return {
            "by_regime": {},
            "regime_flip_flag": False,
            "worst_regime_estimate": 0.0,
            "num_regimes": 0,
        }
    grouped = frame.groupby(regime_col, dropna=False)[value_col]
    means = grouped.mean()
    counts = grouped.size()
    by_regime = {
        str(regime): {"estimate": float(means.loc[regime]), "n_obs": int(counts.loc[regime])}
        for regime in means.index
    }
    non_zero = [np.sign(v["estimate"]) for v in by_regime.values() if abs(v["estimate"]) > 1e-12]
    flip = bool(any(s > 0 for s in non_zero) and any(s < 0 for s in non_zero))
    worst = float(min((v["estimate"] for v in by_regime.values()), default=0.0))
    return {
        "by_regime": by_regime,
        "regime_flip_flag": flip,
        "worst_regime_estimate": worst,
        "num_regimes": int(len(by_regime)),
    }


def evaluate_cross_symbol_stability(
    df: pd.DataFrame, *, value_col: str, symbol_col: str = "symbol"
) -> Dict[str, Any]:
    if df.empty or value_col not in df.columns or symbol_col not in df.columns:
        return {
            "cross_symbol_sign_consistency": 0.0,
            "worst_symbol_estimate": 0.0,
            "by_symbol": {},
            "n_symbols": 0,
        }
    frame = df[[value_col, symbol_col]].copy()
    frame[value_col] = pd.to_numeric(frame[value_col], errors="coerce")
    frame = frame.dropna(subset=[value_col])
    if frame.empty:
        return {
            "cross_symbol_sign_consistency": 0.0,
            "worst_symbol_estimate": 0.0,
            "by_symbol": {},
            "n_symbols": 0,
        }
    grouped = frame.groupby(symbol_col, dropna=False)[value_col]
    means = grouped.mean()
    overall = float(frame[value_col].mean())
    base_sign = np.sign(overall) if abs(overall) > 1e-12 else 0.0
    if base_sign == 0.0:
        consistency = 0.0
    else:
        consistency = float(
            np.mean([(np.sign(v) == base_sign) if abs(v) > 1e-12 else False for v in means.values])
        )
    by_symbol = {str(sym): float(val) for sym, val in means.items()}
    return {
        "cross_symbol_sign_consistency": consistency,
        "worst_symbol_estimate": float(min(by_symbol.values(), default=0.0)),
        "by_symbol": by_symbol,
        "n_symbols": int(len(by_symbol)),
    }


def rolling_stability_metrics(values: Iterable[float], *, window: int = 3) -> Dict[str, Any]:
    series = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if series.empty:
        return {"rolling_instability_score": 0.0, "rolling_means": []}
    if len(series) < window:
        score = (
            float(series.std(ddof=0) / max(abs(series.mean()), 1e-9)) if len(series) > 1 else 0.0
        )
        return {"rolling_instability_score": max(0.0, score), "rolling_means": series.tolist()}
    rolled = series.rolling(window=window, min_periods=window).mean().dropna()
    if rolled.empty:
        return {"rolling_instability_score": 0.0, "rolling_means": []}
    score = float(rolled.std(ddof=0) / max(abs(rolled.mean()), 1e-9)) if len(rolled) > 1 else 0.0
    return {"rolling_instability_score": max(0.0, score), "rolling_means": rolled.tolist()}


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, bool):
        return missing
    return False


def _stability_context(
    row: Dict[str, Any], field: str, source_artifact: str | None = None
) -> str:
    parts = [
        f"field={field}",
        f"candidate_id={row.get('candidate_id', '') or 'unknown'}",
        f"run_id={row.get('run_id', '') or 'unknown'}",
    ]
    if source_artifact:
        parts.append(f"source_artifact={source_artifact}")
    return " ".join(parts)


def _first_finite_row_float(
    row: Dict[str, Any],
    fields: tuple[str, ...],
    *,
    default: float,
    source_artifact: str | None = None,
) -> float:
    for field in fields:
        if field not in row:
            continue
        value = row.get(field)
        if _is_missing_value(value):
            safe_float(value, np.nan, context=_stability_context(row, field, source_artifact))
            continue
        numeric = safe_float(
            value,
            np.nan,
            context=_stability_context(row, field, source_artifact),
        )
        if numeric is not None and np.isfinite(numeric):
            return float(numeric)
    return float(default)


def build_stability_result_from_row(
    row: Dict[str, Any], *, source_artifact: str | None = None
) -> StabilityResult:
    effect = safe_float(
        row.get("effect_shrunk_state", row.get("expectancy", row.get("estimate", 0.0))),
        0.0,
        context=_stability_context(row, "effect", source_artifact),
    )
    std_return = abs(
        _first_finite_row_float(
            row,
            ("std_return", "stderr"),
            default=0.0,
            source_artifact=source_artifact,
        )
    )
    sign_consistency = safe_float(row.get("sign_consistency"), np.nan)
    if not np.isfinite(sign_consistency):
        t_stats = [
            safe_float(row.get(k), np.nan)
            for k in ("train_t_stat", "val_t_stat", "oos1_t_stat", "test_t_stat")
        ]
        t_stats = [t for t in t_stats if np.isfinite(t)]
        base_sign = np.sign(effect) if abs(effect) > 1e-12 else 0.0
        if t_stats and base_sign != 0.0:
            sign_consistency = float(
                np.mean([(np.sign(t) == base_sign) for t in t_stats if abs(t) > 1e-12])
            )
        else:
            sign_consistency = 0.0
    stability_score = safe_float(row.get("stability_score"), np.nan)
    if not np.isfinite(stability_score):
        stability_score = (
            float(sign_consistency * (abs(effect) / max(std_return, 1e-8)))
            if np.isfinite(std_return)
            else 0.0
        )

    regime_map = _parse_mapping(row.get("regime_mean_map", row.get("expectancy_by_regime_bps", {})))
    regime_counts = _parse_mapping(row.get("regime_counts", {}))
    regime_info = {
        "by_regime": {
            k: {"estimate": float(v), "n_obs": int(regime_counts.get(k, 0))}
            for k, v in regime_map.items()
        }
    }
    if regime_map:
        non_zero = [np.sign(v) for v in regime_map.values() if abs(v) > 1e-12]
        regime_flip_flag = bool(any(s > 0 for s in non_zero) and any(s < 0 for s in non_zero))
        worst_regime_estimate = float(min(regime_map.values()))
    else:
        regime_flip_flag = False
        worst_regime_estimate = 0.0
    symbol_map = _parse_mapping(row.get("symbol_expectancy_map", {}))
    if not symbol_map and "cross_symbol_sign_consistency" in row:
        cross_symbol_consistency = safe_float(row.get("cross_symbol_sign_consistency"), 0.0)
        worst_symbol_estimate = safe_float(row.get("worst_symbol_estimate"), 0.0)
    else:
        overall = effect
        base_sign = np.sign(overall) if abs(overall) > 1e-12 else 0.0
        cross_symbol_consistency = (
            float(
                np.mean([(np.sign(v) == base_sign) for v in symbol_map.values() if abs(v) > 1e-12])
            )
            if symbol_map and base_sign != 0.0
            else 0.0
        )
        worst_symbol_estimate = float(min(symbol_map.values(), default=0.0))
    path = [
        safe_float(row.get(k), np.nan)
        for k in ("mean_train_return", "mean_validation_return", "mean_test_return")
    ]
    path = [v for v in path if np.isfinite(v)]
    rolling = rolling_stability_metrics(path, window=2)
    return StabilityResult(
        sign_consistency=float(sign_consistency),
        stability_score=float(stability_score),
        regime_stability_pass=as_bool(
            row.get("gate_regime_stability", row.get("gate_stability", False))
        ),
        timeframe_consensus_pass=as_bool(row.get("gate_timeframe_consensus", True)),
        delay_robustness_pass=as_bool(row.get("gate_delay_robustness", True)),
        regime_flip_flag=bool(regime_flip_flag),
        cross_symbol_sign_consistency=float(cross_symbol_consistency),
        rolling_instability_score=float(rolling.get("rolling_instability_score", 0.0)),
        worst_regime_estimate=float(worst_regime_estimate),
        worst_symbol_estimate=float(worst_symbol_estimate),
        details={
            "path_effects": path,
            "rolling_means": rolling.get("rolling_means", []),
            **regime_info,
            "by_symbol": symbol_map,
        },
    )
