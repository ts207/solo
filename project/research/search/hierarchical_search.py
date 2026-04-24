"""
Phase 4 — Hierarchical Discovery Orchestrator.

Implements staged candidate refinement:
  Stage A  trigger_viability       — minimal probes per trigger
  Stage B  template_refinement     — expand templates for viable triggers
  Stage C  execution_refinement    — direction × lag for viable trigger-templates
  Stage D  context_refinement      — sparse context conditioning on viable shapes

Design principles:
  - Reuses the existing evaluation stack (run_distributed_search → bridge_adapter → scoring)
  - Does NOT introduce new statistical machinery
  - All stage decisions are made via stage_policy.py reading existing evidence columns
  - Flat mode path is completely untouched
  - Failures never raise; they produce empty DataFrames with logged warnings
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from project.io.utils import write_parquet
from project.research.search.stage_policy import (
    STAGE_CONTEXT_REFINEMENT,
    STAGE_EXECUTION_REFINEMENT,
    STAGE_TEMPLATE_REFINEMENT,
    STAGE_TRIGGER_VIABILITY,
    advance_stage_survivors,
    annotate_context_gains,
    rank_stage_candidates,
)

log = logging.getLogger(__name__)


# ── Result container ─────────────────────────────────────────────────────────


@dataclass
class HierarchicalSearchResult:
    """Outcome of a full hierarchical search for one symbol."""

    final_candidates: pd.DataFrame = field(default_factory=pd.DataFrame)
    """Final candidate table — compatible with downstream promotion."""

    stage_artifacts: dict[str, pd.DataFrame] = field(default_factory=dict)
    """Per-stage full candidate table (pass + fail rows) keyed by stage name."""

    stage_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Counts per stage: evaluated, survivors, pruned, reason_breakdown."""

    candidates_evaluated_total: int = 0
    """Total hypothesis evaluations performed (sum across all stages)."""

    flat_mode_equivalent_count: int = 0
    """Equivalent flat-mode hypothesis count (from flat generation audit)."""

    symbol: str = ""


# ── Internal helpers ─────────────────────────────────────────────────────────


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _evaluate_hypotheses(
    hypotheses,
    features: pd.DataFrame,
    *,
    chunk_size: int,
    min_n: int,
    use_context_quality: bool,
    folds,
    min_t_stat: float,
    symbol: str,
    bridge_gates: dict,
) -> pd.DataFrame:
    """Evaluate a hypothesis list and return bridge candidates.

    Reuses run_distributed_search → hypotheses_to_bridge_candidates.
    Raises on evaluation failures so hierarchical runs do not silently report
    missing candidates as a negative result.
    """
    if not hypotheses or features is None or features.empty:
        return pd.DataFrame()

    try:
        from project.research.search.distributed_runner import run_distributed_search
        from project.research.search.bridge_adapter import hypotheses_to_bridge_candidates

        metrics = run_distributed_search(
            hypotheses,
            features,
            chunk_size=chunk_size,
            min_sample_size=min_n,
            use_context_quality=use_context_quality,
            folds=folds,
        )
        if metrics is None or metrics.empty:
            return pd.DataFrame()

        candidates = hypotheses_to_bridge_candidates(
            metrics,
            symbol=symbol,
            min_t_stat=min_t_stat,
            min_n=min_n,
            bridge_min_t_stat=float(bridge_gates.get("search_bridge_min_t_stat", 2.0)),
            bridge_min_robustness_score=float(bridge_gates.get("search_bridge_min_robustness_score", 0.7)),
            bridge_min_regime_stability_score=float(
                bridge_gates.get("search_bridge_min_regime_stability_score", 0.6)
            ),
            bridge_min_stress_survival=float(bridge_gates.get("search_bridge_min_stress_survival", 0.5)),
            bridge_stress_cost_buffer_bps=float(
                bridge_gates.get("search_bridge_stress_cost_buffer_bps", 2.0)
            ),
            prefilter_min_n=True,
            prefilter_min_t_stat=False,  # Permissive for stage filtering
        )

        # Attach sidecar evidence if present
        if hasattr(metrics, "attrs") and "fold_breakdown" in metrics.attrs:
            candidates.attrs["fold_breakdown"] = metrics.attrs["fold_breakdown"]
        if hasattr(metrics, "attrs") and "candidate_event_timestamps" in metrics.attrs:
            candidates.attrs["candidate_event_timestamps"] = metrics.attrs["candidate_event_timestamps"]

        return candidates if candidates is not None else pd.DataFrame()

    except Exception as exc:
        log.error("Hierarchical evaluation failed: %s", exc, exc_info=True)
        raise RuntimeError("Hierarchical evaluation failed") from exc


def _apply_v2_scoring(
    candidates: pd.DataFrame, *, data_root: Path | None = None, run_id: str | None = None
) -> pd.DataFrame:
    """Apply v2/v3 discovery scoring if possible (non-fatal)."""
    if candidates.empty:
        return candidates
    try:
        import yaml
        from project import PROJECT_ROOT
        from project.research.services.candidate_discovery_scoring import (
            annotate_discovery_v2_scores,
            apply_ledger_multiplicity_correction,
        )

        config = {
            "default_turnover_penalty_thresh": 0.8,
            "default_coverage_thresh": 0.01,
            "min_acceptable_regime_support_ratio": 0.4,
        }
        config_path = PROJECT_ROOT.parent / "project" / "configs" / "discovery_scoring_v2.yaml"
        if config_path.exists():
            with config_path.open("r") as f:
                yaml_data = yaml.safe_load(f)
                if yaml_data and "v2_scoring" in yaml_data:
                    config.update(yaml_data["v2_scoring"])

        out = annotate_discovery_v2_scores(candidates, config)

        # Apply ledger correction (v3) if data_root/run_id are available
        if data_root and run_id:
            out = apply_ledger_multiplicity_correction(
                out, data_root=data_root, current_run_id=run_id
            )

        return out
    except Exception as exc:
        if data_root is not None and run_id is not None:
            log.error("V2 scoring failed in hierarchical stage: %s", exc, exc_info=True)
            raise RuntimeError("Hierarchical v2/ledger scoring failed") from exc
        log.warning("V2 scoring skipped in hierarchical stage: %s", exc)
        return candidates


def _stage_stats(
    candidates: pd.DataFrame,
    *,
    stage: str,
    evaluated_count: int,
) -> dict[str, Any]:
    """Build a stats dict for one stage."""
    if candidates.empty:
        return {
            "stage": stage,
            "evaluated": evaluated_count,
            "survivors": 0,
            "pruned": evaluated_count,
            "reason_breakdown": {},
        }
    pass_mask = (
        candidates.get("stage_pass", pd.Series(False, index=candidates.index))
        .fillna(False)
        .astype(bool)
    )
    survivors = int(pass_mask.sum())
    pruned = len(candidates) - survivors
    reason_col = candidates.get("stage_reason_code", pd.Series("", index=candidates.index)).fillna(
        ""
    )
    reason_breakdown: dict[str, int] = {}
    for reason in reason_col[~pass_mask]:
        for code in str(reason).split("|"):
            code = code.strip()
            if code:
                reason_breakdown[code] = reason_breakdown.get(code, 0) + 1
    return {
        "stage": stage,
        "evaluated": evaluated_count,
        "survivors": survivors,
        "pruned": pruned,
        "reason_breakdown": reason_breakdown,
    }


def _extract_trigger_events_from_candidates(candidates: pd.DataFrame) -> list[str]:
    """Pull surviving event_type values from a bridge candidate frame."""
    if candidates.empty:
        return []
    col = "canonical_event_type" if "canonical_event_type" in candidates.columns else "event_type"
    if col not in candidates.columns:
        return []
    return sorted(candidates[col].dropna().astype(str).str.strip().unique().tolist())


def _extract_trigger_templates_from_candidates(
    candidates: pd.DataFrame,
) -> list[tuple[str, str]]:
    """Pull (event_type, template_id) pairs from a bridge candidate frame."""
    if candidates.empty:
        return []
    event_col = (
        "canonical_event_type" if "canonical_event_type" in candidates.columns else "event_type"
    )
    tmpl_col = "rule_template" if "rule_template" in candidates.columns else "template_id"
    if event_col not in candidates.columns or tmpl_col not in candidates.columns:
        return []
    pairs = (
        candidates[[event_col, tmpl_col]]
        .dropna()
        .drop_duplicates()
        .apply(lambda r: (str(r[event_col]).strip(), str(r[tmpl_col]).strip()), axis=1)
        .tolist()
    )
    return sorted(set(pairs))


def _specs_from_survivor_candidates(
    candidates: pd.DataFrame,
    all_evaluated_specs,
) -> list:
    """Recover HypothesisSpec objects for passing candidates.

    Matches by hypothesis_id.
    """
    if candidates.empty or not all_evaluated_specs:
        return []
    pass_mask = (
        candidates.get("stage_pass", pd.Series(False, index=candidates.index))
        .fillna(False)
        .astype(bool)
    )
    survivor_ids = set(
        candidates.loc[pass_mask, "hypothesis_id"].dropna().astype(str).tolist()
        if "hypothesis_id" in candidates.columns
        else []
    )
    if not survivor_ids:
        return []
    return [s for s in all_evaluated_specs if s.hypothesis_id() in survivor_ids]


def _annotate_lineage(
    candidates: pd.DataFrame,
    *,
    stage: str,
    root_trigger_map: dict[str, str] | None = None,
    parent_candidate_id_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Add search_stage, root_trigger_id, parent_candidate_id to a candidate frame."""
    if candidates.empty:
        return candidates
    out = candidates.copy()
    out["search_stage"] = stage

    if "root_trigger_id" not in out.columns:
        event_col = (
            "canonical_event_type" if "canonical_event_type" in out.columns else "event_type"
        )
        if root_trigger_map and event_col in out.columns:
            out["root_trigger_id"] = (
                out[event_col]
                .map(root_trigger_map)
                .fillna(out[event_col] if event_col in out.columns else "")
            )
        elif event_col in out.columns:
            out["root_trigger_id"] = out[event_col].astype(str)
        else:
            out["root_trigger_id"] = ""

    if "parent_candidate_id" not in out.columns:
        out["parent_candidate_id"] = ""
    if parent_candidate_id_map and "root_trigger_id" in out.columns:
        out["parent_candidate_id"] = (
            out["root_trigger_id"].map(parent_candidate_id_map).fillna(out["parent_candidate_id"])
        )

    return out


# ── Stage runners ─────────────────────────────────────────────────────────────


def _run_stage_a(
    events: list[str],
    features: pd.DataFrame,
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    chunk_size: int,
    min_n: int,
    use_context_quality: bool,
    folds,
    min_t_stat: float,
    symbol: str,
    bridge_gates: dict,
    data_root: Path | None = None,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, list]:
    """Stage A — Trigger viability probes.

    Returns (annotated_candidates, probe_specs).
    """
    from project.research.search.generator import generate_trigger_probe_candidates

    stage_cfg = hierarchical_config.get("trigger_viability", {})
    top_k = stage_cfg.get("top_k_triggers")
    if top_k is not None:
        try:
            top_k = int(top_k)
        except (TypeError, ValueError):
            top_k = None
    min_stage_score = float(stage_cfg.get("min_stage_score", 0.0))

    probe_specs = generate_trigger_probe_candidates(
        events, search_spec_doc, hierarchical_config, features=features
    )
    log.info("Stage A: %d probe specs generated for %d events", len(probe_specs), len(events))

    if not probe_specs:
        return pd.DataFrame(), []

    candidates = _evaluate_hypotheses(
        probe_specs,
        features,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
    )
    candidates = _apply_v2_scoring(candidates, data_root=data_root, run_id=run_id)
    candidates = _annotate_lineage(candidates, stage=STAGE_TRIGGER_VIABILITY)

    if candidates.empty:
        return candidates, probe_specs

    # Rank and advance — grouped by root_trigger_id
    ranked = rank_stage_candidates(
        candidates,
        parent_group_col="root_trigger_id",
        stage=STAGE_TRIGGER_VIABILITY,
    )
    advanced = advance_stage_survivors(
        ranked,
        stage=STAGE_TRIGGER_VIABILITY,
        top_k=top_k,
        min_stage_score=min_stage_score,
        parent_group_col="root_trigger_id",
    )
    log.info(
        "Stage A: %d/%d triggers advanced",
        int(advanced["stage_pass"].sum()) if "stage_pass" in advanced.columns else 0,
        len(advanced),
    )
    return advanced, probe_specs


def _run_stage_b(
    stage_a_candidates: pd.DataFrame,
    features: pd.DataFrame,
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    chunk_size: int,
    min_n: int,
    use_context_quality: bool,
    folds,
    min_t_stat: float,
    symbol: str,
    bridge_gates: dict,
    data_root: Path | None = None,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, list]:
    """Stage B — Template refinement for surviving triggers."""
    from project.research.search.generator import generate_template_refinement_candidates

    if stage_a_candidates.empty:
        return pd.DataFrame(), []

    pass_mask = (
        stage_a_candidates.get("stage_pass", pd.Series(False, index=stage_a_candidates.index))
        .fillna(False)
        .astype(bool)
    )
    if not pass_mask.any():
        log.info("Stage B: no Stage A survivors — skipping template refinement")
        return pd.DataFrame(), []

    survivors_a = stage_a_candidates[pass_mask]
    surviving_events = _extract_trigger_events_from_candidates(survivors_a)
    log.info("Stage B: refining templates for %d surviving triggers", len(surviving_events))

    stage_cfg = hierarchical_config.get("template_refinement", {})
    top_k = int(stage_cfg.get("top_k_templates_per_trigger", 3))
    min_stage_score = float(stage_cfg.get("min_stage_score", 0.0))

    specs = generate_template_refinement_candidates(
        surviving_events, search_spec_doc, hierarchical_config, features=features
    )
    if not specs:
        return pd.DataFrame(), []

    candidates = _evaluate_hypotheses(
        specs,
        features,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
    )
    candidates = _apply_v2_scoring(candidates, data_root=data_root, run_id=run_id)

    # Build root_trigger_id map from event_type → existing Stage A candidate_id
    event_col = (
        "canonical_event_type" if "canonical_event_type" in survivors_a.columns else "event_type"
    )
    cid_col = "candidate_id" if "candidate_id" in survivors_a.columns else "hypothesis_id"
    parent_map: dict[str, str] = {}
    if event_col in survivors_a.columns and cid_col in survivors_a.columns:
        for _, r in survivors_a.iterrows():
            parent_map[str(r.get(event_col, "")).strip()] = str(r.get(cid_col, ""))

    candidates = _annotate_lineage(
        candidates,
        stage=STAGE_TEMPLATE_REFINEMENT,
        parent_candidate_id_map=parent_map,
    )

    if candidates.empty:
        return candidates, specs

    # Rank within trigger group
    ranked = rank_stage_candidates(
        candidates,
        parent_group_col="root_trigger_id",
        stage=STAGE_TEMPLATE_REFINEMENT,
    )
    advanced = advance_stage_survivors(
        ranked,
        stage=STAGE_TEMPLATE_REFINEMENT,
        top_k=top_k,
        min_stage_score=min_stage_score,
        parent_group_col="root_trigger_id",
    )
    log.info(
        "Stage B: %d/%d templates advanced",
        int(advanced["stage_pass"].sum()) if "stage_pass" in advanced.columns else 0,
        len(advanced),
    )
    return advanced, specs


def _run_stage_c(
    stage_b_candidates: pd.DataFrame,
    features: pd.DataFrame,
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    chunk_size: int,
    min_n: int,
    use_context_quality: bool,
    folds,
    min_t_stat: float,
    symbol: str,
    bridge_gates: dict,
    data_root: Path | None = None,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, list]:
    """Stage C — Execution shape (direction × lag) refinement."""
    from project.research.search.generator import generate_execution_refinement_candidates

    if stage_b_candidates.empty:
        return pd.DataFrame(), []

    pass_mask = (
        stage_b_candidates.get("stage_pass", pd.Series(False, index=stage_b_candidates.index))
        .fillna(False)
        .astype(bool)
    )
    if not pass_mask.any():
        log.info("Stage C: no Stage B survivors — skipping execution refinement")
        return pd.DataFrame(), []

    survivors_b = stage_b_candidates[pass_mask]
    trigger_templates = _extract_trigger_templates_from_candidates(survivors_b)
    log.info("Stage C: refining %d trigger-template pairs", len(trigger_templates))

    stage_cfg = hierarchical_config.get("execution_refinement", {})
    top_k = int(stage_cfg.get("top_k_shapes_per_template", 3))
    min_stage_score = float(stage_cfg.get("min_stage_score", 0.0))

    specs = generate_execution_refinement_candidates(
        trigger_templates, search_spec_doc, hierarchical_config, features=features
    )
    if not specs:
        return pd.DataFrame(), []

    candidates = _evaluate_hypotheses(
        specs,
        features,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
    )
    candidates = _apply_v2_scoring(candidates, data_root=data_root, run_id=run_id)

    # Parent map: (event_type, template) → Stage B candidate_id
    event_col = (
        "canonical_event_type" if "canonical_event_type" in survivors_b.columns else "event_type"
    )
    tmpl_col = "rule_template" if "rule_template" in survivors_b.columns else "template_id"
    cid_col = "candidate_id" if "candidate_id" in survivors_b.columns else "hypothesis_id"
    parent_map: dict[str, str] = {}
    if event_col in survivors_b.columns and tmpl_col in survivors_b.columns:
        for _, r in survivors_b.iterrows():
            key = f"{r.get(event_col, '')}::{r.get(tmpl_col, '')}"
            parent_map[key] = str(r.get(cid_col, ""))

    candidates = _annotate_lineage(
        candidates,
        stage=STAGE_EXECUTION_REFINEMENT,
    )
    # Wire parent_candidate_id for the (event, template) group
    if not candidates.empty:
        ev_col_c = (
            "canonical_event_type" if "canonical_event_type" in candidates.columns else "event_type"
        )
        tm_col_c = "rule_template" if "rule_template" in candidates.columns else "template_id"
        if ev_col_c in candidates.columns and tm_col_c in candidates.columns:
            candidates["parent_candidate_id"] = [
                parent_map.get(f"{r.get(ev_col_c, '')}::{r.get(tm_col_c, '')}", "")
                for _, r in candidates.iterrows()
            ]
        # Group key for ranking: trigger + template
        candidates["_stage_group"] = (
            candidates.get(ev_col_c, "").astype(str)
            + "::"
            + candidates.get(tm_col_c, "").astype(str)
        )

    if candidates.empty:
        return candidates, specs

    ranked = rank_stage_candidates(
        candidates,
        parent_group_col="_stage_group"
        if "_stage_group" in candidates.columns
        else "root_trigger_id",
        stage=STAGE_EXECUTION_REFINEMENT,
    )
    advanced = advance_stage_survivors(
        ranked,
        stage=STAGE_EXECUTION_REFINEMENT,
        top_k=top_k,
        min_stage_score=min_stage_score,
        parent_group_col="_stage_group" if "_stage_group" in ranked.columns else "root_trigger_id",
    )
    # Clean up temp column
    if "_stage_group" in advanced.columns:
        advanced = advanced.drop(columns=["_stage_group"])
    log.info(
        "Stage C: %d/%d execution shapes advanced",
        int(advanced["stage_pass"].sum()) if "stage_pass" in advanced.columns else 0,
        len(advanced),
    )
    return advanced, specs


def _run_stage_d(
    stage_c_candidates: pd.DataFrame,
    stage_c_specs: list,
    features: pd.DataFrame,
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    chunk_size: int,
    min_n: int,
    use_context_quality: bool,
    folds,
    min_t_stat: float,
    symbol: str,
    bridge_gates: dict,
    data_root: Path | None = None,
    run_id: str | None = None,
) -> pd.DataFrame:
    """Stage D — Sparse context refinement with unconditional baseline.

    Returns the final candidate DataFrame including both baselines and
    context variants.  Only context rows that improve over baseline survive.
    """
    from project.research.search.generator import generate_context_refinement_candidates

    if stage_c_candidates.empty:
        return pd.DataFrame()

    pass_mask = (
        stage_c_candidates.get("stage_pass", pd.Series(False, index=stage_c_candidates.index))
        .fillna(False)
        .astype(bool)
    )
    if not pass_mask.any():
        log.info("Stage D: no Stage C survivors — skipping context refinement")
        return pd.DataFrame()

    # Recover Stage C survivor specs
    survivors_c = stage_c_candidates[pass_mask]
    survivor_specs = _specs_from_survivor_candidates(survivors_c, stage_c_specs)
    if not survivor_specs:
        # Fall back to returning Stage C survivors as-is (no context)
        log.info("Stage D: couldn't recover specs — returning Stage C survivors as final")
        return survivors_c.copy()

    stage_cfg = hierarchical_config.get("context_refinement", {})
    min_context_gain = float(stage_cfg.get("min_context_gain", 0.0))

    baseline_specs, context_specs = generate_context_refinement_candidates(
        survivor_specs, search_spec_doc, hierarchical_config, features=features
    )
    all_d_specs = baseline_specs + context_specs

    if not all_d_specs:
        log.info("Stage D: no context candidates generated — returning Stage C survivors")
        return survivors_c.copy()

    all_candidates = _evaluate_hypotheses(
        all_d_specs,
        features,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
    )
    all_candidates = _apply_v2_scoring(all_candidates, data_root=data_root, run_id=run_id)
    all_candidates = _annotate_lineage(all_candidates, stage=STAGE_CONTEXT_REFINEMENT)

    if all_candidates.empty:
        return survivors_c.copy()

    # Split baseline vs context rows by presence of context dict
    has_ctx_col = "context" in all_candidates.columns
    if has_ctx_col:
        ctx_col = all_candidates["context"]
        baseline_mask = ctx_col.apply(
            lambda v: (not v) or (isinstance(v, dict) and len(v) == 0) or v in (None, "{}", "null")
        )
    else:
        # No context column — treat all as baselines
        baseline_mask = pd.Series(True, index=all_candidates.index)

    baseline_df = all_candidates[baseline_mask].copy() if baseline_mask.any() else pd.DataFrame()
    context_df = all_candidates[~baseline_mask].copy() if (~baseline_mask).any() else pd.DataFrame()

    # Annotate context gain relative to per-parent baseline
    if not context_df.empty and not baseline_df.empty:
        context_df = annotate_context_gains(
            context_df, baseline_df, parent_id_col="parent_candidate_id"
        )
        # Mark context_complexity_penalty (one-dim only, penalty=0 in v1)
        context_df["context_complexity_penalty"] = 0.0

    # Apply context advancement: keep context rows that outperform baseline
    if not context_df.empty and "context_gain_score" in context_df.columns:
        gain = pd.to_numeric(context_df["context_gain_score"], errors="coerce").fillna(-999.0)
        context_survivors = context_df[gain >= min_context_gain].copy()
        context_dropped = context_df[gain < min_context_gain].copy()
        if not context_dropped.empty:
            context_dropped["stage_pass"] = False
            context_dropped["stage_reason_code"] = "failed_context_gain"
        if not context_survivors.empty:
            context_survivors["stage_pass"] = True
            context_survivors["stage_reason_code"] = "passed"
    else:
        context_survivors = context_df.copy() if not context_df.empty else pd.DataFrame()
        if not context_survivors.empty:
            context_survivors["stage_pass"] = True
            context_survivors["stage_reason_code"] = "passed"

    # Baselines always survive Stage D
    if not baseline_df.empty:
        baseline_df["stage_pass"] = True
        baseline_df["stage_reason_code"] = "baseline"
        baseline_df["context_gain_score"] = 0.0
        baseline_df["baseline_candidate_id"] = baseline_df.get(
            "candidate_id", pd.Series("", index=baseline_df.index)
        ).astype(str)

    parts = [p for p in [baseline_df, context_survivors] if p is not None and not p.empty]
    if not parts:
        log.info("Stage D: no survivors after context gating — returning Stage C survivors")
        return survivors_c.copy()

    final = pd.concat(parts, ignore_index=True)
    log.info(
        "Stage D: %d final candidates (%d baselines + %d context survivors)",
        len(final),
        len(baseline_df) if not baseline_df.empty else 0,
        len(context_survivors) if not context_survivors.empty else 0,
    )
    return final


# ── Public orchestrator ───────────────────────────────────────────────────────


def run_hierarchical_search(
    *,
    run_id: str,
    symbol: str,
    events: list[str],
    features: pd.DataFrame,
    search_spec_doc: dict,
    hierarchical_config: dict,
    chunk_size: int = 500,
    min_n: int = 30,
    min_t_stat: float = 1.5,
    use_context_quality: bool = True,
    folds=None,
    bridge_gates: Optional[dict] = None,
    data_root: Optional[Path] = None,
    out_dir: Optional[Path] = None,
) -> HierarchicalSearchResult:
    """Orchestrate Stage A → B → C → D for one symbol.

    Args:
        events:       List of event IDs to probe (from expanded search spec).
        features:     Wide feature DataFrame for the symbol.
        bridge_gates: Dict of bridge gate thresholds (from gates spec).
        data_root:    Used for ledger writes.
        out_dir:      If provided, stage artifact parquets are written here.

    Returns:
        HierarchicalSearchResult with final_candidates and stage artifacts.
    """
    if bridge_gates is None:
        bridge_gates = {}

    result = HierarchicalSearchResult(symbol=symbol)
    total_evaluated = 0

    # ── Stage A ───────────────────────────────────────────────────────────
    stage_a_cfg = hierarchical_config.get("trigger_viability", {})
    if not stage_a_cfg.get("enabled", True):
        log.info("Hierarchical Stage A disabled — aborting hierarchical search")
        return result

    stage_a_df, probe_specs = _run_stage_a(
        events,
        features,
        search_spec_doc,
        hierarchical_config,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
        data_root=data_root,
        run_id=run_id,
    )
    total_evaluated += len(probe_specs)
    result.stage_artifacts[STAGE_TRIGGER_VIABILITY] = stage_a_df
    result.stage_stats[STAGE_TRIGGER_VIABILITY] = _stage_stats(
        stage_a_df, stage=STAGE_TRIGGER_VIABILITY, evaluated_count=len(probe_specs)
    )

    if out_dir:
        try:
            write_parquet(stage_a_df, out_dir / f"phase2_trigger_probes__{symbol}.parquet")
        except Exception as exc:
            log.warning("Stage A artifact write failed: %s", exc)

    # ── Stage B ───────────────────────────────────────────────────────────
    stage_b_cfg = hierarchical_config.get("template_refinement", {})
    if not stage_b_cfg.get("enabled", True):
        # Flat-pass Stage A survivors directly
        surviving = (
            stage_a_df[stage_a_df.get("stage_pass", pd.Series(False)).fillna(False).astype(bool)]
            if not stage_a_df.empty
            else pd.DataFrame()
        )
        result.final_candidates = surviving
        return result

    stage_b_df, stage_b_specs = _run_stage_b(
        stage_a_df,
        features,
        search_spec_doc,
        hierarchical_config,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
        data_root=data_root,
        run_id=run_id,
    )
    total_evaluated += len(stage_b_specs)
    result.stage_artifacts[STAGE_TEMPLATE_REFINEMENT] = stage_b_df
    result.stage_stats[STAGE_TEMPLATE_REFINEMENT] = _stage_stats(
        stage_b_df, stage=STAGE_TEMPLATE_REFINEMENT, evaluated_count=len(stage_b_specs)
    )

    if out_dir:
        try:
            write_parquet(stage_b_df, out_dir / f"phase2_template_refinement__{symbol}.parquet")
        except Exception as exc:
            log.warning("Stage B artifact write failed: %s", exc)

    # ── Stage C ───────────────────────────────────────────────────────────
    stage_c_cfg = hierarchical_config.get("execution_refinement", {})
    if not stage_c_cfg.get("enabled", True):
        surviving = (
            stage_b_df[stage_b_df.get("stage_pass", pd.Series(False)).fillna(False).astype(bool)]
            if not stage_b_df.empty
            else pd.DataFrame()
        )
        result.final_candidates = surviving
        return result

    stage_c_df, stage_c_specs = _run_stage_c(
        stage_b_df,
        features,
        search_spec_doc,
        hierarchical_config,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
        data_root=data_root,
        run_id=run_id,
    )
    total_evaluated += len(stage_c_specs)
    result.stage_artifacts[STAGE_EXECUTION_REFINEMENT] = stage_c_df
    result.stage_stats[STAGE_EXECUTION_REFINEMENT] = _stage_stats(
        stage_c_df, stage=STAGE_EXECUTION_REFINEMENT, evaluated_count=len(stage_c_specs)
    )

    if out_dir:
        try:
            write_parquet(stage_c_df, out_dir / f"phase2_execution_refinement__{symbol}.parquet")
        except Exception as exc:
            log.warning("Stage C artifact write failed: %s", exc)

    # ── Stage D ───────────────────────────────────────────────────────────
    stage_d_cfg = hierarchical_config.get("context_refinement", {})
    if not stage_d_cfg.get("enabled", True):
        surviving = (
            stage_c_df[stage_c_df.get("stage_pass", pd.Series(False)).fillna(False).astype(bool)]
            if not stage_c_df.empty
            else pd.DataFrame()
        )
        result.final_candidates = surviving
        result.candidates_evaluated_total = total_evaluated
        return result

    final_candidates = _run_stage_d(
        stage_c_df,
        stage_c_specs,
        features,
        search_spec_doc,
        hierarchical_config,
        chunk_size=chunk_size,
        min_n=min_n,
        use_context_quality=use_context_quality,
        folds=folds,
        min_t_stat=min_t_stat,
        symbol=symbol,
        bridge_gates=bridge_gates,
        data_root=data_root,
        run_id=run_id,
    )
    result.stage_artifacts[STAGE_CONTEXT_REFINEMENT] = final_candidates
    result.stage_stats[STAGE_CONTEXT_REFINEMENT] = _stage_stats(
        final_candidates,
        stage=STAGE_CONTEXT_REFINEMENT,
        evaluated_count=len(stage_c_specs),  # Stage D evaluates same specs
    )

    if out_dir:
        try:
            write_parquet(
                final_candidates, out_dir / f"phase2_context_refinement__{symbol}.parquet"
            )
        except Exception as exc:
            log.warning("Stage D artifact write failed: %s", exc)

    result.final_candidates = final_candidates
    result.candidates_evaluated_total = total_evaluated

    log.info(
        "Hierarchical search complete for %s: %d total evaluated → %d final candidates",
        symbol,
        total_evaluated,
        len(final_candidates),
    )
    return result
