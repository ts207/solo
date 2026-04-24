from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.constants import parse_horizon_bars
from project.core.timeframes import normalize_timeframe, timeframe_to_minutes
from project.io.utils import ensure_dir, write_parquet
from project.research.gating import calculate_expectancy_stats
from project.research.phase2 import load_features as _load_features_impl


def summarize_subtype_diagnostics(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {}

    out: dict[str, object] = {}
    for event_type, group in df.groupby("event_type"):
        adverse = pd.to_numeric(group["adverse_move"], errors="coerce").dropna()
        favorable = pd.to_numeric(group["favorable_move"], errors="coerce").dropna()
        out[str(event_type)] = {
            "adverse_mean": float(adverse.mean()) if not adverse.empty else 0.0,
            "favorable_mean": float(favorable.mean()) if not favorable.empty else 0.0,
            "count": len(group),
        }
    return out


def optional_token(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    token = str(value).strip()
    if token.lower() in {"", "none", "null", "nan"}:
        return None
    return token


def bool_mask_from_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.fillna(0).astype(float) != 0.0
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin({"1", "true", "yes", "on"})


def horizon_to_bars(horizon: str) -> int:
    return parse_horizon_bars(horizon, default=12)


def load_phase2_features(
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
    data_root: Path | None = None,
) -> pd.DataFrame:
    root = Path(data_root) if data_root is not None else get_data_root()
    return _load_features_impl(
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        data_root=root,
    )


def bar_duration_minutes_from_timeframe(timeframe: str) -> int:
    return int(timeframe_to_minutes(normalize_timeframe(timeframe or "5m")))


def regime_ess_diagnostics(
    events_df: pd.DataFrame,
    min_ess_per_regime: float,
    min_regimes_required: int,
) -> dict[str, Any]:
    if events_df.empty or "vol_regime" not in events_df.columns:
        return {"gate_regime_ess": False, "pass_count": 0}
    counts = events_df["vol_regime"].value_counts()
    pass_count = (counts >= min_ess_per_regime).sum()
    return {
        "gate_regime_ess": bool(pass_count >= min_regimes_required),
        "pass_count": int(pass_count),
    }


def timeframe_expectancy_consensus(**kwargs: Any) -> dict[str, Any]:
    configured_timeframes = list(kwargs.get("configured_timeframes", []) or [])
    if not configured_timeframes:
        return {"gate_timeframe_consensus": False, "aligned_count": 0, "available_count": 0}

    events_df = kwargs.get("events_df", pd.DataFrame())
    base_sign = int(kwargs.get("base_sign", 0) or 0)
    min_consistency_ratio = float(kwargs.get("min_consistency_ratio", 0.3) or 0.3)
    min_timeframes_required = int(kwargs.get("min_timeframes_required", 1) or 1)

    aligned_count = 0
    available_count = 0
    for timeframe in configured_timeframes:
        features = load_phase2_features(
            run_id=str(kwargs.get("run_id", "")),
            symbol=str(kwargs.get("symbol", "")),
            timeframe=str(timeframe),
        )
        if features.empty:
            continue
        available_count += 1
        stats = calculate_expectancy_stats(
            events_df,
            features=features,
            horizon_bars_override=bar_duration_minutes_from_timeframe(str(timeframe)),
            args=kwargs.get("args"),
            side_policy=kwargs.get("side_policy", "both"),
            label_target=kwargs.get("label_target", "close_logret"),
            rule=kwargs.get("rule"),
        )
        sign = float(pd.to_numeric(stats.get("t_stat", 0.0), errors="coerce") or 0.0)
        if sign == 0.0 or base_sign == 0:
            continue
        if np.sign(sign) == np.sign(base_sign):
            aligned_count += 1

    required = (
        max(min_timeframes_required, int(np.ceil(available_count * min_consistency_ratio)))
        if available_count
        else 0
    )
    return {
        "gate_timeframe_consensus": bool(available_count and aligned_count >= required),
        "aligned_count": int(aligned_count),
        "available_count": int(available_count),
    }


def write_empty_phase2_outputs(out_dir: Path, run_id: str) -> None:
    del run_id
    ensure_dir(out_dir)
    (out_dir / "phase2_gate_summary.json").write_text("{}", encoding="utf-8")
    write_parquet(pd.DataFrame(), out_dir / "phase2_candidates.parquet")


def write_empty_phase2_outputs_with_diagnostics(
    reports_root: Path,
    generation_diagnostics: dict[str, object],
    spec_hashes: dict[str, str],
    template_config_hash: str,
    run_manifest_ontology_hash: str,
    current_ontology_hash: str,
    current_ontology_components: dict[str, str],
    operator_registry_version: str,
    cost_coordinate: dict[str, object],
    gate_profile: str,
    entry_lag_bars: int,
    summary_overrides: dict[str, object] | None = None,
    budget_diagnostics: dict[str, object] | None = None,
) -> None:
    ensure_dir(reports_root)

    diag_path = reports_root / "phase2_generation_diagnostics.json"
    diag_payload = {
        **generation_diagnostics,
        "spec_hashes": spec_hashes,
        "template_config_hash": template_config_hash,
        "run_manifest_ontology_hash": run_manifest_ontology_hash,
        "current_ontology_hash": current_ontology_hash,
        "current_ontology_components": current_ontology_components,
        "operator_registry_version": operator_registry_version,
        "cost_coordinate": cost_coordinate,
        "gate_profile": gate_profile,
        "entry_lag_bars": entry_lag_bars,
    }
    if budget_diagnostics:
        diag_payload["budget_diagnostics"] = budget_diagnostics
    diag_path.write_text(json.dumps(diag_payload, indent=2), encoding="utf-8")

    report_path = reports_root / "phase2_report.json"
    report_summary = {
        "total_tested": 0,
        "discoveries_statistical": 0,
        "survivors_phase2": 0,
    }
    if summary_overrides:
        report_summary.update(summary_overrides)
    report_payload = {
        "run_id": generation_diagnostics.get("run_id"),
        "event_type": generation_diagnostics.get("event_type"),
        "summary": report_summary,
        "cost_coordinate": cost_coordinate,
    }
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    empty_df = pd.DataFrame()
    write_parquet(empty_df, reports_root / "phase2_candidates.parquet")
    write_parquet(empty_df, reports_root / "phase2_candidates_raw.parquet")
    empty_df.to_csv(reports_root / "phase2_candidates.csv", index=False)
    write_parquet(empty_df, reports_root / "phase2_pvals.parquet")
    write_parquet(empty_df, reports_root / "phase2_fdr.parquet")

    summary_path = reports_root / "phase2_gate_summary.json"
    summary_payload = {
        "candidates_total": 0,
        "pass_all_gates": 0,
        "per_gate_pass_count": {},
        "per_gate_fail_count": {},
        "run_id": generation_diagnostics.get("run_id"),
        "event_type": generation_diagnostics.get("event_type"),
    }
    if summary_overrides:
        summary_payload.update(summary_overrides)
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
