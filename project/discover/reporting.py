from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from project.artifacts import data_root as resolve_data_root
from project.artifacts import phase2_candidates_path, phase2_diagnostics_path
from project.io.utils import read_table_auto


def _as_bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if df.empty or column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    raw = df[column]
    truthy = {"1", "true", "t", "yes", "y", "on", "pass"}
    return raw.map(
        lambda value: (
            bool(value)
            if isinstance(value, bool)
            else (str(value).strip().lower() in truthy if value is not None else False)
        )
    ).fillna(False).astype(bool)


def _as_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if df.empty or column not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _first_present(row: pd.Series, columns: Iterable[str]) -> Any:
    for column in columns:
        if column not in row.index:
            continue
        value = row.get(column)
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        if value not in (None, ""):
            return value
    return None


def _relpath(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def load_phase2_candidates(*, run_id: str, data_root: Path | None = None) -> tuple[pd.DataFrame, Path]:
    root = resolve_data_root(data_root)
    path = phase2_candidates_path(run_id, root)
    return read_table_auto(path), path


def load_phase2_diagnostics(*, run_id: str, data_root: Path | None = None) -> tuple[dict[str, Any], Path]:
    root = resolve_data_root(data_root)
    path = phase2_diagnostics_path(run_id, root)
    if not path.exists():
        return {}, path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return {}, path
    return payload if isinstance(payload, dict) else {}, path


def _score_column(df: pd.DataFrame) -> tuple[str | None, bool]:
    """
    Returns (column_name, lower_is_better).
    """
    for col in (
        "discovery_quality_score_v3",
        "discovery_quality_score",
        "after_cost_expectancy_bps",
        "stressed_after_cost_expectancy_bps",
        "abs_t_stat",
        "t_stat",
        "t_value",
        "q_value",
    ):
        if col in df.columns:
            return col, col == "q_value"
    return None, False


def _top_candidates(df: pd.DataFrame, *, limit: int = 10) -> dict[str, Any]:
    if df.empty:
        return {"count": 0, "score_column": None, "lower_is_better": False, "rows": []}

    selected = df.copy()
    if "gate_bridge_tradable" in selected.columns and bool(_as_bool_series(selected, "gate_bridge_tradable").any()):
        selected = selected.loc[_as_bool_series(selected, "gate_bridge_tradable")].copy()

    score_col, lower_is_better = _score_column(selected)
    if score_col is None:
        ranked = selected.reset_index(drop=True)
    else:
        scores = _as_numeric_series(selected, score_col)
        ranked = selected.assign(__score=scores).sort_values(
            "__score",
            ascending=bool(lower_is_better),
            na_position="last" if not lower_is_better else "first",
            kind="stable",
        )

    out_rows: list[dict[str, Any]] = []
    for _, row in ranked.head(int(limit)).iterrows():
        out_rows.append(
            {
                "candidate_id": str(row.get("candidate_id", "") or ""),
                "symbol": str(row.get("symbol", "") or ""),
                "event_type": str(_first_present(row, ["event_type", "event", "trigger_type"]) or ""),
                "template": str(
                    _first_present(
                        row,
                        ["template_id", "rule_template", "template_verb", "template", "template_family"],
                    )
                    or ""
                ),
                "direction": str(row.get("direction", "") or ""),
                "horizon": _first_present(row, ["horizon", "horizon_bars", "horizon_label"]),
                "after_cost_expectancy_bps": _first_present(
                    row,
                    ["after_cost_expectancy_bps", "stressed_after_cost_expectancy_bps", "after_cost_expectancy"],
                ),
                "sign_consistency": _first_present(row, ["sign_consistency", "stability_sign_consistency"]),
                "cost_survival_ratio": _first_present(row, ["cost_survival_ratio", "after_cost_survival_ratio"]),
                "control_pass_rate": _first_present(row, ["control_pass_rate"]),
                "gate_regime_stable": _first_present(
                    row, ["gate_c_regime_stable", "gate_regime_stability", "gate_regime_stable"]
                ),
                "gate_multiplicity": _first_present(row, ["gate_multiplicity"]),
                "gate_bridge_tradable": _first_present(row, ["gate_bridge_tradable"]),
                "score": (
                    None
                    if score_col is None
                    else (None if pd.isna(row.get(score_col)) else row.get(score_col))
                ),
            }
        )

    return {
        "count": int(len(selected)),
        "score_column": score_col,
        "lower_is_better": bool(lower_is_better),
        "rows": out_rows,
    }


def _placebo_failure_rate(df: pd.DataFrame) -> tuple[float | None, str]:
    control_rate = _as_numeric_series(df, "control_pass_rate")
    if control_rate.notna().any():
        return float(control_rate.dropna().mean()), "candidate.control_pass_rate"

    placebo_cols = [
        "pass_shift_placebo",
        "pass_random_entry_placebo",
        "pass_direction_reversal_placebo",
    ]
    present = [col for col in placebo_cols if col in df.columns]
    if not present:
        return None, "missing"

    passes = pd.Series(True, index=df.index, dtype=bool)
    for col in present:
        passes &= _as_bool_series(df, col)
    return float((~passes).mean()), "candidate.placebo_failure_rate"


def build_discover_summary(*, run_id: str, data_root: Path | None = None, top_k: int = 10) -> dict[str, Any]:
    root = resolve_data_root(data_root)

    candidates, candidates_path = load_phase2_candidates(run_id=run_id, data_root=root)
    diagnostics, diagnostics_path = load_phase2_diagnostics(run_id=run_id, data_root=root)
    gate_funnel = diagnostics.get("gate_funnel") if isinstance(diagnostics.get("gate_funnel"), dict) else {}

    final_mask = (
        _as_bool_series(candidates, "gate_bridge_tradable")
        if "gate_bridge_tradable" in candidates.columns
        else pd.Series(False, index=candidates.index, dtype=bool)
    )
    survivors = candidates.loc[final_mask].copy() if bool(final_mask.any()) else candidates.copy()

    after_cost = _as_numeric_series(survivors, "after_cost_expectancy_bps")
    sign_consistency = _as_numeric_series(survivors, "sign_consistency")
    cost_survival = _as_numeric_series(survivors, "cost_survival_ratio")

    placebo_rate, placebo_source = _placebo_failure_rate(survivors) if not survivors.empty else (None, "missing")
    regime_col = (
        "gate_c_regime_stable"
        if "gate_c_regime_stable" in survivors.columns
        else "gate_regime_stability"
        if "gate_regime_stability" in survivors.columns
        else "gate_regime_stable"
        if "gate_regime_stable" in survivors.columns
        else None
    )
    regime_pass_rate = (
        float(_as_bool_series(survivors, regime_col).mean()) if regime_col and not survivors.empty else None
    )

    return {
        "run_id": str(run_id),
        "data_root": str(root),
        "artifact_paths": {
            "phase2_candidates": _relpath(root, candidates_path),
            "phase2_diagnostics": _relpath(root, diagnostics_path),
        },
        "counts": {
            "candidates_total": int(len(candidates)),
            "candidates_final": int(len(survivors)),
        },
        "diagnostics": {
            "hypotheses_generated": int(diagnostics.get("hypotheses_generated", 0) or 0),
            "feasible_hypotheses": int(diagnostics.get("feasible_hypotheses", 0) or 0),
            "metrics_rows": int(diagnostics.get("metrics_rows", 0) or 0),
            "valid_metrics_rows": int(diagnostics.get("valid_metrics_rows", 0) or 0),
            "bridge_candidates_rows": int(diagnostics.get("bridge_candidates_rows", 0) or 0),
            "event_flag_rows": int(diagnostics.get("event_flag_rows", 0) or 0),
            "event_flag_columns_merged": int(diagnostics.get("event_flag_columns_merged", 0) or 0),
            "gate_funnel": gate_funnel,
        },
        "metrics": {
            "after_cost_expectancy_bps": {
                "median": None if after_cost.dropna().empty else float(after_cost.dropna().median()),
                "best": None if after_cost.dropna().empty else float(after_cost.dropna().max()),
            },
            "sign_consistency": {
                "median": None
                if sign_consistency.dropna().empty
                else float(sign_consistency.dropna().median()),
            },
            "cost_survival_ratio": {
                "median": None
                if cost_survival.dropna().empty
                else float(cost_survival.dropna().median()),
            },
            "placebo_failure_rate": {
                "value": placebo_rate,
                "source": placebo_source,
            },
            "regime_stability": {
                "pass_rate": regime_pass_rate,
                "source": regime_col or "missing",
            },
        },
        "top_candidates": _top_candidates(candidates, limit=int(top_k)),
    }


def format_discover_summary_text(summary: dict[str, Any]) -> str:
    run_id = str(summary.get("run_id", "") or "")
    data_root = str(summary.get("data_root", "") or "")
    artifacts = summary.get("artifact_paths", {}) if isinstance(summary.get("artifact_paths"), dict) else {}
    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    diagnostics = summary.get("diagnostics", {}) if isinstance(summary.get("diagnostics"), dict) else {}
    metrics = summary.get("metrics", {}) if isinstance(summary.get("metrics"), dict) else {}

    lines: list[str] = []
    lines.append(f"Discovery summary for run_id={run_id}")
    lines.append(f"data_root: {data_root}")
    if artifacts:
        lines.append("artifacts:")
        for key in ("phase2_candidates", "phase2_diagnostics"):
            if key in artifacts:
                lines.append(f"  - {key}: {artifacts[key]}")

    lines.append("counts:")
    lines.append(f"  - candidates_total: {int(counts.get('candidates_total', 0) or 0)}")
    lines.append(f"  - candidates_final: {int(counts.get('candidates_final', 0) or 0)}")

    gate_funnel = diagnostics.get("gate_funnel", {}) if isinstance(diagnostics.get("gate_funnel"), dict) else {}
    if gate_funnel:
        lines.append("gate_funnel:")
        for key, value in gate_funnel.items():
            try:
                lines.append(f"  - {key}: {int(value)}")
            except Exception:
                lines.append(f"  - {key}: {value}")

    if metrics:
        after_cost = (
            metrics.get("after_cost_expectancy_bps", {})
            if isinstance(metrics.get("after_cost_expectancy_bps"), dict)
            else {}
        )
        sign = metrics.get("sign_consistency", {}) if isinstance(metrics.get("sign_consistency"), dict) else {}
        survival = (
            metrics.get("cost_survival_ratio", {})
            if isinstance(metrics.get("cost_survival_ratio"), dict)
            else {}
        )
        placebo = (
            metrics.get("placebo_failure_rate", {})
            if isinstance(metrics.get("placebo_failure_rate"), dict)
            else {}
        )
        regime = (
            metrics.get("regime_stability", {})
            if isinstance(metrics.get("regime_stability"), dict)
            else {}
        )
        lines.append("quality:")
        lines.append(f"  - after_cost_expectancy_bps_median: {after_cost.get('median')}")
        lines.append(f"  - after_cost_expectancy_bps_best: {after_cost.get('best')}")
        lines.append(f"  - sign_consistency_median: {sign.get('median')}")
        lines.append(f"  - cost_survival_ratio_median: {survival.get('median')}")
        lines.append(f"  - placebo_failure_rate: {placebo.get('value')} ({placebo.get('source')})")
        lines.append(
            f"  - regime_stability_pass_rate: {regime.get('pass_rate')} ({regime.get('source')})"
        )

    top = summary.get("top_candidates", {}) if isinstance(summary.get("top_candidates"), dict) else {}
    top_rows = top.get("rows", []) if isinstance(top.get("rows"), list) else []
    if top_rows:
        score_col = top.get("score_column")
        lines.append(f"top_candidates (score={score_col}):")
        for idx, row in enumerate(top_rows[:10], start=1):
            if not isinstance(row, dict):
                continue
            lines.append(
                "  "
                + f"{idx}. {row.get('event_type','')} {row.get('template','')} {row.get('direction','')} h={row.get('horizon')} "
                + f"score={row.get('score')} after_cost_bps={row.get('after_cost_expectancy_bps')} "
                + f"sign={row.get('sign_consistency')} survival={row.get('cost_survival_ratio')} id={row.get('candidate_id')}"
            )

    return "\n".join(lines) + "\n"


def explain_empty_discovery(*, run_id: str, data_root: Path | None = None) -> dict[str, Any]:
    summary = build_discover_summary(run_id=run_id, data_root=data_root, top_k=0)

    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    candidates_total = int(counts.get("candidates_total", 0) or 0)
    diagnostics = summary.get("diagnostics", {}) if isinstance(summary.get("diagnostics"), dict) else {}
    gate_funnel = (
        diagnostics.get("gate_funnel", {})
        if isinstance(diagnostics.get("gate_funnel"), dict)
        else {}
    )

    if candidates_total > 0:
        return {
            **summary,
            "empty": False,
            "classification": "not_empty",
            "reason": f"phase2_candidates is non-empty ({candidates_total} rows)",
        }

    # Missing diagnostics is itself informative: run likely failed before phase2 emission.
    if not diagnostics or not isinstance(diagnostics, dict):
        return {
            **summary,
            "empty": True,
            "classification": "missing_diagnostics",
            "reason": "phase2_diagnostics missing or unreadable",
        }

    event_rows = int(diagnostics.get("event_flag_rows", 0) or 0)
    event_cols = int(diagnostics.get("event_flag_columns_merged", 0) or 0)
    generated = int(diagnostics.get("hypotheses_generated", 0) or 0)
    feasible = int(diagnostics.get("feasible_hypotheses", 0) or 0)
    valid_metrics = int(diagnostics.get("valid_metrics_rows", 0) or 0)

    classification = "unknown"
    reason = "Empty candidate set; inspect gate funnel + rejection reasons"

    if event_rows <= 0 or event_cols <= 0:
        classification = "no_qualifying_events"
        reason = "No qualifying event flags were produced (event_flag_rows/columns are zero)"
    elif generated <= 0:
        classification = "search_space_too_narrow"
        reason = "Search generated zero hypotheses (hypotheses_generated=0)"
    elif feasible <= 0 and generated > 0:
        classification = "hypotheses_rejected_pre_metrics"
        reason = "Hypotheses were generated but none were feasible (feasible_hypotheses=0)"
    elif valid_metrics <= 0:
        classification = "invalid_or_insufficient_metrics"
        reason = "No valid metrics rows survived basic validity checks (valid_metrics_rows=0)"
    elif gate_funnel:
        ordered = [
            ("pass_min_sample_size", "too_few_events"),
            ("pass_oos_validation", "oos_validation_failed"),
            ("pass_after_cost_positive", "expectancy_died_after_costs"),
            ("pass_after_cost_stressed_positive", "stressed_cost_expectancy_failed"),
            ("pass_multiplicity", "multiplicity_gate_failed"),
            ("pass_regime_stable", "regime_stability_failed"),
            ("phase2_final", "final_gate_failed"),
        ]
        for key, label in ordered:
            if key in gate_funnel and int(gate_funnel.get(key, 0) or 0) <= 0:
                classification = label
                reason = f"Gate funnel reached zero at {key}"
                break

    rejection_counts = diagnostics.get("rejection_reason_counts", {})
    if not isinstance(rejection_counts, dict):
        rejection_counts = {}
    top_rejections = sorted(
        ((str(k), int(v or 0)) for k, v in rejection_counts.items()),
        key=lambda item: item[1],
        reverse=True,
    )[:8]

    return {
        **summary,
        "empty": True,
        "classification": classification,
        "reason": reason,
        "top_rejection_reasons": top_rejections,
    }


def format_explain_empty_text(payload: dict[str, Any]) -> str:
    run_id = str(payload.get("run_id", "") or "")
    classification = str(payload.get("classification", "") or "")
    reason = str(payload.get("reason", "") or "")
    empty = bool(payload.get("empty", False))

    lines: list[str] = []
    if not empty:
        lines.append(f"Run {run_id} is not empty: {reason}")
        return "\n".join(lines) + "\n"

    lines.append(f"Empty discovery run: run_id={run_id}")
    lines.append(f"classification: {classification}")
    lines.append(f"reason: {reason}")

    gate_funnel = (
        payload.get("diagnostics", {}).get("gate_funnel", {})
        if isinstance(payload.get("diagnostics"), dict)
        else {}
    )
    if isinstance(gate_funnel, dict) and gate_funnel:
        lines.append("gate_funnel:")
        for key, value in gate_funnel.items():
            try:
                lines.append(f"  - {key}: {int(value)}")
            except Exception:
                lines.append(f"  - {key}: {value}")

    top_rejections = payload.get("top_rejection_reasons", [])
    if isinstance(top_rejections, list) and top_rejections:
        lines.append("top_rejection_reasons:")
        for key, value in top_rejections:
            lines.append(f"  - {key}: {value}")

    artifacts = payload.get("artifact_paths", {}) if isinstance(payload.get("artifact_paths"), dict) else {}
    if artifacts:
        lines.append("artifact_paths:")
        for name, path in artifacts.items():
            lines.append(f"  - {name}: {path}")

    return "\n".join(lines) + "\n"
