"""Phase-2 search gate funnel helpers.

This module keeps the search engine orchestration focused on I/O and run flow
while isolating the pure counting and funnel-reporting logic that is shared by
unit tests and diagnostics.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

_DEFAULT_PHASE2_MIN_T_STAT = 1.5
_DEFAULT_PHASE2_MIN_T_STAT_NET = 1.5


def _classify_metrics_counts(
    metrics: pd.DataFrame,
    *,
    min_n: int,
    min_t_stat: float,
) -> tuple[int, int, int]:
    if metrics.empty:
        return 0, 0, 0

    valid_mask = (
        metrics.get("valid", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
    )
    invalid_reason = (
        metrics.get("invalid_reason", pd.Series("", index=metrics.index)).fillna("").astype(str)
    )

    valid_metrics_rows = int(valid_mask.sum())
    rejected_by_min_n = int(((~valid_mask) & invalid_reason.eq("min_sample_size")).sum())
    rejected_invalid_metrics = max(
        0,
        len(metrics) - valid_metrics_rows - rejected_by_min_n,
    )
    return valid_metrics_rows, rejected_invalid_metrics, rejected_by_min_n


def _merge_rejection_reason_counts(
    base_counts: Mapping[str, Any],
    *,
    rejected_invalid_metrics: int,
    rejected_by_min_n: int,
    rejected_by_min_t_stat: int,
) -> dict[str, int]:
    counts = {str(reason): int(count) for reason, count in dict(base_counts).items()}
    if rejected_invalid_metrics:
        counts["invalid_metrics"] = counts.get("invalid_metrics", 0) + int(rejected_invalid_metrics)
    if rejected_by_min_n:
        counts["min_sample_size"] = counts.get("min_sample_size", 0) + int(rejected_by_min_n)
    if rejected_by_min_t_stat:
        counts["min_t_stat_net"] = counts.get("min_t_stat_net", 0) + int(rejected_by_min_t_stat)
    return counts


def _resolve_search_min_t_stat(
    *,
    explicit_min_t_stat: float | None,
    phase2_gates: Mapping[str, Any],
) -> float:
    if explicit_min_t_stat is not None:
        return float(explicit_min_t_stat)
    if bool(phase2_gates.get("use_net_gate", True)):
        raw = phase2_gates.get(
            "min_t_stat_net",
            phase2_gates.get("min_t_stat", _DEFAULT_PHASE2_MIN_T_STAT_NET),
        )
    else:
        raw = phase2_gates.get("min_t_stat", _DEFAULT_PHASE2_MIN_T_STAT)
    return float(raw)


def _bool_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[column].fillna(False).astype(bool)


def _numeric_series(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)


def _funnel_examples(
    frame: pd.DataFrame,
    reason_col: str = "gate_status",
    *,
    limit: int = 5,
) -> dict[str, list[str]]:
    examples: dict[str, list[str]] = {}
    if frame.empty or reason_col not in frame.columns:
        return examples
    id_col = "hypothesis_id" if "hypothesis_id" in frame.columns else "candidate_id"
    for _, row in frame.iterrows():
        reason = str(row.get(reason_col, "") or "unknown")
        bucket = examples.setdefault(reason, [])
        if len(bucket) < int(limit):
            bucket.append(str(row.get(id_col, "")))
    return examples


def _build_funnel_payload(
    *,
    run_id: str,
    program_id: str,
    gate_funnel: Mapping[str, Any],
    candidate_universe: pd.DataFrame,
    written_candidates: pd.DataFrame,
) -> dict[str, Any]:
    drops_by_reason: dict[str, int] = {}
    if not candidate_universe.empty and "gate_status" in candidate_universe.columns:
        drops_by_reason = {
            str(k): int(v)
            for k, v in (
                candidate_universe["gate_status"]
                .fillna("unknown")
                .astype(str)
                .value_counts()
                .sort_index()
                .items()
            )
            if str(k) != "passed"
        }
    return {
        "run_id": str(run_id),
        "program_id": str(program_id or ""),
        "generated": int(gate_funnel.get("generated", 0) or 0),
        "feasible": int(gate_funnel.get("feasible", 0) or 0),
        "t_gross_passed": int(gate_funnel.get("t_gross_passed", 0) or 0),
        "t_net_passed": int(
            gate_funnel.get("t_net_passed", gate_funnel.get("pass_min_t_stat_net", 0)) or 0
        ),
        "mean_net_passed": int(gate_funnel.get("mean_net_passed", 0) or 0),
        "q_passed": int(gate_funnel.get("q_passed", gate_funnel.get("pass_multiplicity", 0)) or 0),
        "robust_passed": int(
            gate_funnel.get("robust_passed", gate_funnel.get("pass_regime_stable", 0)) or 0
        ),
        "cost_survival_passed": int(
            gate_funnel.get(
                "cost_survival_passed",
                gate_funnel.get("pass_after_cost_stressed_positive", 0),
            )
            or 0
        ),
        "promoted_research": 0,
        "promoted_deploy": 0,
        "drops_by_reason": drops_by_reason,
        "examples_dropped": _funnel_examples(candidate_universe),
        "phase2_candidates_written": len(written_candidates),
    }


def _build_gate_funnel(
    *,
    hypotheses_generated: int,
    feasible_hypotheses: int,
    metrics: pd.DataFrame,
    candidate_universe: pd.DataFrame,
    written_candidates: pd.DataFrame,
    min_n: int,
    min_t_stat_net: float = 0.0,
) -> dict[str, int]:
    valid_mask = _bool_mask(metrics, "valid")
    # TICKET-012: keep n_values as a series when the n column is missing.
    if "n" in metrics.columns:
        n_values = pd.to_numeric(metrics["n"], errors="coerce").fillna(0)
    else:
        n_values = pd.Series(0.0, index=metrics.index)
    pass_min_n = valid_mask & (n_values >= int(min_n))
    t_gross = _numeric_series(metrics, "t_stat_gross")
    t_net = (
        _numeric_series(metrics, "t_stat_net")
        if "t_stat_net" in metrics.columns
        else _numeric_series(metrics, "t_stat")
    )
    mean_net = (
        _numeric_series(metrics, "mean_return_net_bps")
        if "mean_return_net_bps" in metrics.columns
        else _numeric_series(metrics, "cost_adjusted_return_bps")
    )
    threshold = abs(float(min_t_stat_net or 0.0))
    pass_t_gross = pass_min_n & (t_gross.abs() >= threshold)
    pass_t_net = pass_min_n & (t_net.abs() >= threshold)
    pass_mean_net = pass_t_net & (mean_net >= 0.0)

    funnel: dict[str, int] = {
        "generated": int(hypotheses_generated),
        "feasible": int(feasible_hypotheses),
        "metrics_emitted": len(metrics),
        "valid_metrics": int(valid_mask.sum()),
        "pass_min_sample_size": int(pass_min_n.sum()),
        "t_gross_passed": int(pass_t_gross.sum()),
        "t_net_passed": int(pass_t_net.sum()),
        "mean_net_passed": int(pass_mean_net.sum()),
        "bridge_candidate_universe": len(candidate_universe),
        "phase2_candidates_written": len(written_candidates),
    }

    stage_mask = pd.Series(True, index=written_candidates.index, dtype=bool)
    for label, column in (
        ("pass_oos_validation", "gate_oos_validation"),
        ("pass_after_cost_positive", "gate_after_cost_positive"),
        ("pass_after_cost_stressed_positive", "gate_after_cost_stressed_positive"),
        ("pass_cost_p95_survival", "gate_cost_p95_survival"),
        ("pass_multiplicity", "gate_multiplicity"),
        ("pass_regime_stable", "gate_c_regime_stable"),
        ("phase2_final", "gate_bridge_tradable"),
    ):
        stage_mask &= _bool_mask(written_candidates, column)
        funnel[label] = int(stage_mask.sum())
    funnel["q_passed"] = int(funnel.get("pass_multiplicity", 0))
    funnel["robust_passed"] = int(funnel.get("pass_regime_stable", 0))
    funnel["cost_survival_passed"] = int(funnel.get("pass_after_cost_stressed_positive", 0))
    return funnel


