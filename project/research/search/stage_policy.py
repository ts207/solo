"""
Stage policy helpers for Phase 4 hierarchical discovery.

Provides the logic to:
  - Compute a stage score from existing evidence columns (no new math).
  - Rank candidates within their parent group.
  - Advance survivors by top-k or threshold.
  - Produce explainable reason codes.

Design principle: all scoring inputs come from columns already produced
by the existing evaluator + fold + ledger + v2/v3 scoring stack.
This module only reads those columns — it never recomputes significance.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── Stage names ─────────────────────────────────────────────────────────────
STAGE_TRIGGER_VIABILITY = "trigger_viability"
STAGE_TEMPLATE_REFINEMENT = "template_refinement"
STAGE_EXECUTION_REFINEMENT = "execution_refinement"
STAGE_CONTEXT_REFINEMENT = "context_refinement"

# Minimum t-statistic gate threshold, matching the discovery pipeline minimum.
# Used to rescale t_norm so that t=_T_MIN_GATE maps to 0.0 (minimum viable)
# and t=3.0 maps to 1.0 (maximum), rather than the old 0.0 / 3.0 mapping that
# created a structural soft floor prematurely pruning near-threshold triggers
# before template or context conditioning could amplify the effect.
_T_MIN_GATE: float = 1.5

ALL_STAGES = [
    STAGE_TRIGGER_VIABILITY,
    STAGE_TEMPLATE_REFINEMENT,
    STAGE_EXECUTION_REFINEMENT,
    STAGE_CONTEXT_REFINEMENT,
]

# Reason codes emitted by stage gating
REASON_FAILED_TRIGGER = "failed_trigger_viability"
REASON_FAILED_TEMPLATE = "failed_template_refinement"
REASON_FAILED_EXECUTION = "failed_execution_refinement"
REASON_FAILED_CONTEXT = "failed_context_gain"
REASON_SMALL_SUPPORT = "support_too_small_after_refinement"
REASON_CONTEXT_OVERCONDITIONING = "context_overconditioning_penalty"
REASON_LEDGER_BURDEN = "ledger_burden_exceeded"


# ── Stage score ──────────────────────────────────────────────────────────────


def _safe_float(value, default: float = 0.0) -> float:
    try:
        v = float(value)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _compute_stage_score(row: dict | pd.Series, *, stage: str) -> float:
    """Derive a stage score from existing evidence fields.

    Reads columns already present after the evaluator + Phase 2/3 pipeline:
      t_stat, robustness_score, fold_stability_score / fold_pass_rate,
      ledger_multiplicity_penalty, discovery_quality_score_v3.

    The formula is a weighted sum of normalized existing fields — NOT new
    statistical machinery.  The score sits in approximately [-1, 2].

    For Stage D the caller should additionally compute context_gain_score
    and store it as a separate column.
    """
    # ── Evidence inputs from existing columns ────────────────────────────
    t_raw = _safe_float(row.get("t_stat", row.get("t_statistic", 0.0)), 0.0)
    # Normalize t to [0, 1] using a gate-relative scale: [_T_MIN_GATE, 3.0] → [0, 1].
    # This maps t=1.5 (minimum viable) to 0.0 and t=3.0 to 1.0.
    # The old division by 3.0 mapped t=1.5 to 0.5, creating a structural soft
    # floor that discarded near-threshold triggers before template conditioning
    # could strengthen them (the flat path evaluates all combinations;).
    t_norm = _clamp((abs(t_raw) - _T_MIN_GATE) / (3.0 - _T_MIN_GATE), 0.0, 1.0)

    rob = _clamp(_safe_float(row.get("robustness_score", 0.0), 0.0), 0.0, 1.0)

    # Fold stability — prefer fold_stability_score, then fold_pass_rate
    fold_stab_raw = row.get("fold_stability_score")
    if fold_stab_raw is None:
        fold_stab_raw = row.get("fold_pass_rate")
    fold_stab = _clamp(_safe_float(fold_stab_raw, 0.5), 0.0, 1.0)

    # Ledger burden — Phase 3 column; 0 if absent (no history = no penalty)
    ledger_pen = _clamp(
        _safe_float(row.get("ledger_multiplicity_penalty", 0.0), 0.0),
        0.0,
        3.0,
    )

    # ── Stage-specific weights ────────────────────────────────────────────
    if stage == STAGE_TRIGGER_VIABILITY:
        # Cheap probe — lean on t-stat and basic robustness
        score = 0.40 * t_norm + 0.30 * rob + 0.15 * fold_stab - 0.15 * (ledger_pen / 3.0)
    elif stage == STAGE_TEMPLATE_REFINEMENT:
        # Templates ranked within trigger; fold evidence more important
        score = 0.35 * t_norm + 0.25 * rob + 0.25 * fold_stab - 0.15 * (ledger_pen / 3.0)
    elif stage == STAGE_EXECUTION_REFINEMENT:
        # Direction/lag: cost-adjusted expectancy matters more
        cost_adj_raw = row.get("cost_adjusted_return_bps", row.get("mean_return_bps", 0.0))
        cost_adj = _clamp(_safe_float(cost_adj_raw, 0.0) / 20.0, -1.0, 1.0)
        score = (
            0.30 * t_norm
            + 0.20 * rob
            + 0.25 * fold_stab
            + 0.10 * cost_adj
            - 0.15 * (ledger_pen / 3.0)
        )
    elif stage == STAGE_CONTEXT_REFINEMENT:
        # Context: fold stability critical; ledger burden weighted higher
        score = 0.30 * t_norm + 0.25 * rob + 0.30 * fold_stab - 0.15 * (ledger_pen / 3.0)
    else:
        # Fallback / unknown stage
        score = 0.35 * t_norm + 0.25 * rob + 0.25 * fold_stab - 0.15 * (ledger_pen / 3.0)

    return _clamp(score, -1.0, 2.0)


# ── Ranking ──────────────────────────────────────────────────────────────────


def rank_stage_candidates(
    candidates: pd.DataFrame,
    *,
    parent_group_col: str,
    stage: str,
) -> pd.DataFrame:
    """Add ``stage_score`` and ``stage_rank_within_parent`` columns.

    Ranking is **within each parent group** — so the best template per
    trigger gets rank 1, not the globally best template.
    """
    if candidates is None or candidates.empty:
        out = candidates.copy() if candidates is not None else pd.DataFrame()
        out["stage_score"] = pd.Series(dtype=float)
        out["stage_rank_within_parent"] = pd.Series(dtype=int)
        out["search_stage"] = stage
        return out

    out = candidates.copy()
    out["search_stage"] = stage

    # Compute stage_score for every row
    scores = [_compute_stage_score(dict(row), stage=stage) for _, row in out.iterrows()]
    out["stage_score"] = scores

    # Rank within parent group (higher score = lower rank number = better)
    if parent_group_col in out.columns:
        out["stage_rank_within_parent"] = (
            out.groupby(parent_group_col, sort=False)["stage_score"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
    else:
        # No grouping possible — rank globally
        out["stage_rank_within_parent"] = (
            out["stage_score"].rank(method="first", ascending=False).astype(int)
        )

    return out


# ── Survivor advancement ─────────────────────────────────────────────────────


def advance_stage_survivors(
    ranked: pd.DataFrame,
    *,
    stage: str,
    top_k: Optional[int],
    min_stage_score: float,
    parent_group_col: str,
) -> pd.DataFrame:
    """Mark ``stage_pass=True`` for top-k per parent above threshold.

    Returns the *whole* ranked DataFrame with ``stage_pass`` and
    ``stage_reason_code`` columns populated.  Callers can then filter to
    ``stage_pass == True`` for the next stage.
    """
    out = ranked.copy()
    if "stage_score" not in out.columns:
        out["stage_score"] = 0.0
    if "stage_rank_within_parent" not in out.columns:
        out["stage_rank_within_parent"] = 1

    # Default all to fail
    out["stage_pass"] = False
    out["stage_reason_code"] = ""

    if out.empty:
        return out

    score_series = pd.to_numeric(out["stage_score"], errors="coerce").fillna(-999.0)
    rank_series = (
        pd.to_numeric(out["stage_rank_within_parent"], errors="coerce").fillna(9999).astype(int)
    )

    for idx in out.index:
        score = float(score_series.loc[idx])
        rank = int(rank_series.loc[idx])

        codes: list[str] = []

        # Score threshold gate
        if score < float(min_stage_score):
            stage_fail_code = {
                STAGE_TRIGGER_VIABILITY: REASON_FAILED_TRIGGER,
                STAGE_TEMPLATE_REFINEMENT: REASON_FAILED_TEMPLATE,
                STAGE_EXECUTION_REFINEMENT: REASON_FAILED_EXECUTION,
                STAGE_CONTEXT_REFINEMENT: REASON_FAILED_CONTEXT,
            }.get(stage, f"failed_{stage}")
            codes.append(stage_fail_code)
            out.at[idx, "stage_reason_code"] = "|".join(codes)
            continue

        # Top-k gate within parent
        if top_k is not None and rank > int(top_k):
            codes.append(f"outside_top_{top_k}")
            out.at[idx, "stage_reason_code"] = "|".join(codes)
            continue

        # Additional checks
        n_val = pd.to_numeric(out.at[idx, "n"] if "n" in out.columns else 0, errors="coerce") or 0
        if n_val < 10:
            codes.append(REASON_SMALL_SUPPORT)
            out.at[idx, "stage_reason_code"] = "|".join(codes)
            continue

        # Passed
        out.at[idx, "stage_pass"] = True
        out.at[idx, "stage_reason_code"] = "passed"

    return out


# ── Context gain ─────────────────────────────────────────────────────────────


def compute_context_gain(
    context_row: dict | pd.Series,
    baseline_row: dict | pd.Series,
) -> float:
    """Return the stage-score gain from adding context over the baseline.

    Positive = context helps, negative = context hurts.
    """
    ctx_score = _compute_stage_score(context_row, stage=STAGE_CONTEXT_REFINEMENT)
    base_score = _compute_stage_score(baseline_row, stage=STAGE_CONTEXT_REFINEMENT)
    return ctx_score - base_score


def annotate_context_gains(
    context_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    *,
    parent_id_col: str = "parent_candidate_id",
) -> pd.DataFrame:
    """Add ``context_gain_score`` and ``baseline_candidate_id`` to context candidates.

    Looks up the unconditional baseline row (context=None) for each parent
    candidate and computes the gain.
    """
    if context_df.empty:
        out = context_df.copy()
        out["context_gain_score"] = pd.Series(dtype=float)
        out["baseline_candidate_id"] = pd.Series(dtype=str)
        return out

    out = context_df.copy()
    out["context_gain_score"] = 0.0
    out["baseline_candidate_id"] = ""

    if baseline_df.empty or parent_id_col not in out.columns:
        return out

    # Build baseline lookup: parent_candidate_id → baseline row dict
    if "candidate_id" in baseline_df.columns:
        baseline_map = {str(row["candidate_id"]): dict(row) for _, row in baseline_df.iterrows()}
    else:
        return out

    for idx, row in out.iterrows():
        parent_id = str(row.get(parent_id_col, "")).strip()
        baseline_row = baseline_map.get(parent_id)
        if baseline_row is None:
            continue
        gain = compute_context_gain(dict(row), baseline_row)
        out.at[idx, "context_gain_score"] = float(gain)
        out.at[idx, "baseline_candidate_id"] = parent_id

    return out


# ── Reason code builder ───────────────────────────────────────────────────────


def build_stage_reason_codes(row: dict | pd.Series, *, stage: str) -> str:
    """Return a pipe-delimited string of reasons this row might be flagged.

    This is purely diagnostic — it does not gate anything.
    """
    codes: list[str] = []
    score = _safe_float(row.get("stage_score", 0.0), 0.0)

    if score < 0.0:
        codes.append("negative_stage_score")

    ledger_pen = _safe_float(row.get("ledger_multiplicity_penalty", 0.0), 0.0)
    if ledger_pen > 1.5:
        codes.append(REASON_LEDGER_BURDEN)

    n_val = _safe_float(row.get("n", row.get("sample_size", 0.0)), 0.0)
    if n_val < 10:
        codes.append(REASON_SMALL_SUPPORT)

    ctx = row.get("context") or row.get("context_slice") or {}
    if ctx and stage == STAGE_CONTEXT_REFINEMENT:
        if isinstance(ctx, dict) and len(ctx) > 1:
            codes.append(REASON_CONTEXT_OVERCONDITIONING)

    if not codes:
        codes.append("none")
    return "|".join(codes)


# ── Default hierarchical config ───────────────────────────────────────────────

DEFAULT_HIERARCHICAL_CONFIG: dict = {
    "mode": "hierarchical",
    "trigger_viability": {
        "enabled": True,
        "max_templates": 1,
        "max_horizons": 1,
        "max_entry_lags": 1,
        "allow_both_directions": True,
        "top_k_triggers": None,
        "min_stage_score": 0.0,
    },
    "template_refinement": {
        "enabled": True,
        "top_k_templates_per_trigger": 3,
        "min_stage_score": 0.0,
    },
    "execution_refinement": {
        "enabled": True,
        "top_k_shapes_per_template": 3,
        "min_stage_score": 0.0,
    },
    "context_refinement": {
        "enabled": True,
        "max_context_dims": 1,
        "top_k_contexts_per_candidate": 2,
        "require_unconditional_baseline": True,
        "min_context_gain": 0.0,
    },
}
