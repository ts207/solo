from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.io.utils import atomic_write_json
from project.research.action_policy import build_action_policy_queues
from project.research.knowledge.memory import (
    build_failures_snapshot,
    build_tested_regions_snapshot,
    compute_context_statistics,
    compute_event_statistics,
    compute_region_statistics,
    compute_template_statistics,
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)
from project.research.knowledge.reflection import build_run_reflection
from project.research.knowledge.schemas import canonical_json
from project.research.search_intelligence import update_search_intelligence
from project.research.services.campaign_memory_rollup_service import write_campaign_memory_rollup
from project.specs.manifest import finalize_manifest, load_run_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def _merge_by_keys(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if existing.empty:
        return incoming.copy()
    if incoming.empty:
        return existing.copy()
    out = pd.concat([existing, incoming], ignore_index=True)
    present_keys = [key for key in keys if key in out.columns]
    if present_keys:
        out = out.drop_duplicates(subset=present_keys, keep="last").reset_index(drop=True)
    return out


_MISSING_STAGE_TOKENS = frozenset({"", "none", "null", "nan"})


def _has_valid_stage(value: Any) -> bool:
    return str(value).strip().lower() not in _MISSING_STAGE_TOKENS


def _sanitize_failures(failures: pd.DataFrame) -> pd.DataFrame:
    if failures.empty:
        return failures
    cleaned = failures.copy()
    if "stage" in cleaned.columns:
        cleaned = cleaned[cleaned["stage"].apply(_has_valid_stage)]
    return cleaned.reset_index(drop=True)


def _active_failures(failures: pd.DataFrame) -> pd.DataFrame:
    if failures.empty:
        return failures
    active = _sanitize_failures(failures)
    if "superseded_by_run_id" in active.columns:
        active = active[
            active["superseded_by_run_id"].astype(str).str.strip() == ""
        ]
    return active.reset_index(drop=True)


def mark_failures_superseded(
    failures: pd.DataFrame,
    *,
    current_run_id: str,
    stage: str,
    program_id: str,
) -> pd.DataFrame:
    """Mark open failures for *stage* and *program_id* as superseded.

    Phase 2.4: When a repair run completes successfully for a previously
    failing stage, existing failure records for that stage are marked with
    the current run ID so the controller's repair-check logic skips them.

    Only rows where ``superseded_by_run_id`` is empty (not already resolved)
    are updated.  Rows for other stages or programs are left untouched.

    Parameters
    ----------
    failures:
        The merged failures DataFrame (all programs).
    current_run_id:
        The run that resolved the failure (written into superseded_by_run_id).
    stage:
        The pipeline stage that previously failed and has now recovered.
    program_id:
        Scope the update to this program only.

    Returns
    -------
    pd.DataFrame
        Updated failures DataFrame with superseded_by_run_id populated for
        matching rows.
    """
    if failures.empty:
        return failures
    if "superseded_by_run_id" not in failures.columns:
        failures = failures.copy()
        failures["superseded_by_run_id"] = ""

    mask = (
        (failures["stage"].astype(str) == stage)
        & (failures["program_id"].astype(str) == program_id)
        & (failures["superseded_by_run_id"].astype(str).str.strip() == "")
    )
    if mask.any():
        failures = failures.copy()
        failures.loc[mask, "superseded_by_run_id"] = current_run_id
        _LOG.info(
            "Superseded %d failure record(s) for stage=%s program=%s with run_id=%s",
            int(mask.sum()),
            stage,
            program_id,
            current_run_id,
        )
    return failures


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    atomic_write_json(
        path,
        payload,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _scope_already_tested(tested_regions: pd.DataFrame, proposed_scope: Dict[str, Any]) -> bool:
    if tested_regions.empty:
        return False

    work = tested_regions.copy()
    checks = [
        ("trigger_type", str(proposed_scope.get("trigger_type", "EVENT")).strip().upper()),
        ("event_type", str(proposed_scope.get("event_type", "")).strip()),
        ("template_id", str(proposed_scope.get("template_id", "")).strip()),
        ("direction", str(proposed_scope.get("direction", "")).strip()),
        ("horizon", str(proposed_scope.get("horizon", "")).strip()),
    ]
    for column, value in checks:
        if value and column in work.columns:
            work = work[work[column].astype(str).str.strip() == value]
        if work.empty:
            return False

    entry_lag = proposed_scope.get("entry_lag")
    if entry_lag not in (None, "") and "entry_lag" in work.columns:
        work = work[pd.to_numeric(work["entry_lag"], errors="coerce").fillna(0).astype(int) == int(entry_lag)]
        if work.empty:
            return False

    contexts = proposed_scope.get("contexts", {})
    if "context_json" in work.columns:
        target_context = canonical_json(contexts if isinstance(contexts, dict) else {})
        work = work[
            work["context_json"].fillna("{}").map(canonical_json) == target_context
        ]
        if work.empty:
            return False

    return not work.empty


def _gate_rank(val: Any) -> int:
    """Standardized ranking for gate statuses."""
    val = str(val).strip().lower()
    if val in ("pass", "true", "1", "1.0"):
        return 2
    if val in ("fail", "false", "0", "0.0"):
        return 1
    return 0


def _build_belief_state(
    *,
    tested_regions: pd.DataFrame,
    failures: pd.DataFrame,
    reflection: Dict[str, Any],
    promising_top_k: int,
    avoid_top_k: int,
    repair_top_k: int,
) -> Dict[str, Any]:
    promising_regions = []
    recommended_next_action = str(reflection.get("recommended_next_action", "")).strip()
    statistical_outcome = str(reflection.get("statistical_outcome", "")).strip()
    suppress_promising_regions = (
        recommended_next_action in {"hold", "repair_pipeline", "kill"}
        or statistical_outcome == "no_signal"
    )
    if not suppress_promising_regions and not tested_regions.empty:
        ranked = tested_regions.copy()
        if "gate_promo_statistical" in ranked.columns:
            ranked["_gate_rank"] = ranked["gate_promo_statistical"].apply(_gate_rank)
        else:
            ranked["_gate_rank"] = 0

        ranked = ranked.sort_values(
            ["_gate_rank", "after_cost_expectancy", "q_value"],
            ascending=[False, False, True],
        ).head(int(promising_top_k))
        promising_regions = ranked[
            [
                c
                for c in ["event_type", "template_id", "direction", "horizon", "region_key"]
                if c in ranked.columns
            ]
        ].to_dict(orient="records")
        for idx, region in enumerate(promising_regions):
            source = ranked.iloc[idx]
            for key in [
                "trigger_type",
                "trigger_key",
                "trigger_payload_json",
                "entry_lag",
                "context_json",
                "state_id",
                "from_state",
                "to_state",
                "feature",
                "operator",
                "threshold",
            ]:
                if key in ranked.columns:
                    region[key] = source.get(key)

    avoid_regions = []
    if not tested_regions.empty and "primary_fail_gate" in tested_regions.columns:
        # Phase 1.3 — probabilistic avoidance: only block on high-confidence,
        # non-mechanical failures with adequate sample size.
        # A low-confidence or mechanical failure should route to repair, not closure.
        avoid_candidates = tested_regions[
            tested_regions["primary_fail_gate"].astype(str) != ""
        ].copy()
        if "failure_confidence" in avoid_candidates.columns:
            avoid_candidates = avoid_candidates[
                (avoid_candidates["failure_confidence"].fillna(0.0) > 0.7)
                & (avoid_candidates["failure_cause_class"].isin(
                    ["market", "cost", "overfitting"]
                ) if "failure_cause_class" in avoid_candidates.columns else True)
                & (avoid_candidates["failure_sample_size"].fillna(0).astype(int) >= 30
                   if "failure_sample_size" in avoid_candidates.columns else True)
            ]
        rejected = avoid_candidates.head(int(avoid_top_k))
        avoid_regions = rejected[
            [
                c
                for c in ["event_type", "template_id", "primary_fail_gate", "region_key",
                          "failure_confidence", "failure_cause_class", "failure_sample_size"]
                if c in rejected.columns
            ]
        ].to_dict(orient="records")

    # Phase 2-B: Aggregate avoidance from event_statistics
    try:
        # Resolve data_root for memory table read; default to standard if missing
        program_id = reflection.get("program_id", "")
        if program_id:
            event_stats = read_memory_table(program_id, "event_statistics")
            if not event_stats.empty:
                OVERFIT_GATES = frozenset([
                    "gate_promo_multiplicity_diagnostics",
                    "gate_promo_negative_control_missing",
                    "gate_promo_negative_control_fail",
                ])
                for row in event_stats.to_dict(orient="records"):
                    gate = str(row.get("dominant_fail_gate", "")).strip()
                    n_eval = int(row.get("times_evaluated", 0) or 0)
                    n_promoted = int(row.get("times_promoted", 0) or 0)
                    if n_eval >= 10 and gate in OVERFIT_GATES:
                        avoid_regions.append({
                            "region_key": f"event_aggregate:{row['event_type']}",
                            "event_type": str(row["event_type"]),
                            "trigger_type": "EVENT",
                            "reason": f"aggregate overfitting signal: {gate} ({n_eval} evals, {n_promoted} promoted)",
                            "confidence": 0.7,
                            "failure_cause_class": "overfitting",
                        })
    except Exception as exc:
        _LOG.warning("Failed to propagate aggregate avoidance signals: %s", exc)

    open_failures = _active_failures(failures)
    open_repairs = []
    if not open_failures.empty:
        open_repairs = (
            open_failures[
                [c for c in ["stage", "failure_class", "failure_detail"] if c in failures.columns]
            ]
            .head(int(repair_top_k))
            .to_dict(orient="records")
        )

    return {
        "current_focus": str(reflection.get("recommended_next_action", "")),
        "avoid_regions": avoid_regions,
        "promising_regions": promising_regions,
        "open_repairs": open_repairs,
        "last_reflection_run_id": str(reflection.get("run_id", "")),
    }


def _build_next_actions(
    *,
    reflection: Dict[str, Any],
    tested_regions: pd.DataFrame,
    failures: pd.DataFrame,
    regime_conditional_candidates: pd.DataFrame,
    exploit_top_k: int,
    repair_top_k: int,
) -> Dict[str, Any]:
    """Build the next_actions queue from memory artefacts.

    Phase 4.2: regime_conditional_candidates are injected into explore_adjacent
    with context conditioning derived from the best-performing regime slice,
    so the controller's follow-up run targets the specific regime.
    """
    policy_actions = build_action_policy_queues(
        tested_regions,
        exploit_top_k=int(exploit_top_k),
        retest_top_k=int(exploit_top_k),
        hold_top_k=int(exploit_top_k),
    )
    exploit = list(policy_actions.get("exploit", []))
    retest = list(policy_actions.get("retest", []))
    hold = list(policy_actions.get("hold", []))

    recommended_next_action = str(reflection.get("recommended_next_action", "")).strip()
    allow_exploit = recommended_next_action in {
        "exploit_promising_region",
        "explore_adjacent_region",
    }
    if allow_exploit and not exploit and not tested_regions.empty:
        ranked_df = tested_regions.copy()
        if "gate_promo_statistical" in ranked_df.columns:
            ranked_df["_gate_rank"] = ranked_df["gate_promo_statistical"].apply(_gate_rank)
        else:
            ranked_df["_gate_rank"] = 0

        exploit = [
            {
                "reason": "best observed region by statistical and expectancy filters",
                "priority": "medium",
                "proposed_scope": row,
            }
            for row in (
                ranked_df.sort_values(
                    ["_gate_rank", "after_cost_expectancy", "q_value"],
                    ascending=[False, False, True],
                )
                .head(int(exploit_top_k))[
                    [
                        c
                        for c in [
                            "event_type",
                            "trigger_type",
                            "trigger_key",
                            "trigger_payload_json",
                            "template_id",
                            "direction",
                            "horizon",
                            "entry_lag",
                            "context_json",
                            "state_id",
                            "region_key",
                            "feature",
                            "operator",
                            "threshold",
                            "from_state",
                            "to_state",
                        ]
                        if c in tested_regions.columns
                    ]
                ]
                .to_dict(orient="records")
            )
        ]

    active_failures = _active_failures(failures)
    repair = []
    if not active_failures.empty:
        repair = active_failures.head(int(repair_top_k))[
            [c for c in ["stage", "failure_class", "failure_detail"] if c in failures.columns]
        ].to_dict(orient="records")

    recommended_experiment = {}
    try:
        recommended_experiment = json.loads(
            str(reflection.get("recommended_next_experiment", "{}"))
        )
    except json.JSONDecodeError:
        recommended_experiment = {}

    # Phase 4.2 — Regime-conditional explore_adjacent entries.
    # Each entry carries a proposed_scope with context pinned to the best-
    # performing regime so the controller generates context-conditioned runs.
    explore_adjacent: list[Dict[str, Any]] = []

    if not regime_conditional_candidates.empty:
        ranked = regime_conditional_candidates.copy()
        if "priority_score" in ranked.columns:
            ranked["priority_score"] = pd.to_numeric(ranked["priority_score"], errors="coerce").fillna(0.0)
            ranked = ranked.sort_values(
                ["priority_score", "best_regime_t_stat", "best_regime_mean_return_bps"],
                ascending=[False, False, False],
            )
        seen_scopes: set[str] = set()
        for row in ranked.head(max(1, int(exploit_top_k))).to_dict(orient="records"):
            pinned_contexts = _parse_best_regime_contexts(row.get("best_regime"))
            merged_contexts = _merge_contexts(
                _coerce_contexts(row.get("context_json")),
                pinned_contexts,
            )
            proposed_scope = {
                "event_type": str(row.get("event_type", "")).strip(),
                "trigger_type": str(row.get("trigger_type", "EVENT")).strip().upper() or "EVENT",
                "template_id": str(row.get("template_id", "")).strip(),
                "direction": str(row.get("direction", "")).strip(),
                "horizon": str(row.get("horizon", "")).strip(),
                "entry_lag": int(row.get("entry_lag", row.get("entry_lag_bars", 0)) or 0),
            }
            if merged_contexts:
                proposed_scope["contexts"] = merged_contexts
            best_regime = str(row.get("best_regime", "")).strip()
            if best_regime:
                proposed_scope["best_regime"] = best_regime
            source_hypothesis_id = str(row.get("hypothesis_id", "")).strip()
            if source_hypothesis_id:
                proposed_scope["source_hypothesis_id"] = source_hypothesis_id
            if not proposed_scope["event_type"] or not proposed_scope["template_id"]:
                continue
            if _scope_already_tested(tested_regions, proposed_scope):
                continue
            scope_key = canonical_json(proposed_scope)
            if scope_key in seen_scopes:
                continue
            seen_scopes.add(scope_key)
            explore_adjacent.append(
                {
                    "reason": "strong regime slice despite weak aggregate result",
                    "priority": "medium",
                    "proposed_scope": proposed_scope,
                }
            )

    if recommended_experiment and any(
        str(recommended_experiment.get(key, "")).strip()
        for key in ("event_type", "template_id", "primary_fail_gate")
    ):
        if not _scope_already_tested(tested_regions, recommended_experiment):
            explore_adjacent.append(
                {
                    "reason": str(reflection.get("recommended_next_action", "")),
                    "priority": "medium",
                    "proposed_scope": recommended_experiment,
                }
            )

    return {
        "repair": [
            {
                "reason": "mechanical failure detected",
                "priority": "high",
                "proposed_scope": row,
            }
            for row in repair
        ],
        "exploit": exploit,
        "retest": retest,
        "explore_adjacent": explore_adjacent,
        "hold": hold,
    }
def _load_regime_conditional_candidates(*, run_id: str, data_root: Path) -> pd.DataFrame:
    """Phase 4.2 — Load regime_conditional_candidates.parquet for this run.

    Written by _write_regime_conditional_candidates() in phase2_search_engine.py
    or run_hypothesis_search.py. Checks the canonical flat phase-2 output path
    plus the hypothesis-search output path so either current producer's artifact
    is found without reviving nested legacy layouts.
    Returns an empty DataFrame when the artifact is absent.
    Raises when the artifact exists but is unreadable/corrupted.
    """
    candidates = [
        data_root / "reports" / "phase2" / run_id / "regime_conditional_candidates.parquet",
        data_root / "reports" / "hypothesis_search" / run_id / "regime_conditional_candidates.parquet",
    ]
    for path in candidates:
        if path.exists():
            try:
                return pd.read_parquet(path)
            except Exception as exc:
                _LOG.warning("Failed to read regime conditional candidates from %s", path, exc_info=True)
                raise DataIntegrityError(
                    f"Failed to read regime conditional candidates from {path}: {exc}"
                ) from exc
    return pd.DataFrame()


def _coerce_contexts(value: Any) -> Dict[str, list]:
    """Coerce a context_json value (str or dict) to {family: [label]} dict."""
    payload = value
    if isinstance(payload, str):
        text = payload.strip()
        if not text or text == "{}":
            return {}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, list] = {}
    for key, raw in payload.items():
        family = str(key).strip()
        label = str(raw).strip()
        if family and label:
            out[family] = [label]
    return out


def _parse_best_regime_contexts(best_regime: Any) -> Dict[str, list]:
    """Convert a best_regime label string to a context conditioning dict.

    Maps token substrings from regime labels produced by regime_evaluator.py
    to the context dimension / label pairs used by the campaign controller.
    """
    regime = str(best_regime or "").strip().lower()
    if not regime:
        return {}

    token_map = {
        "high_vol":    ("vol_regime",     "high"),
        "low_vol":     ("vol_regime",     "low"),
        "funding_pos": ("carry_state",    "funding_pos"),
        "funding_neg": ("carry_state",    "funding_neg"),
        "trend":       ("ms_trend_state", "trend"),
        "chop":        ("ms_trend_state", "chop"),
        "tight":       ("ms_spread_state", "tight"),
        "wide":        ("ms_spread_state", "wide"),
    }
    contexts: Dict[str, list] = {}
    for token, (family, label) in token_map.items():
        if token in regime:
            contexts[family] = [label]
    return contexts


def _merge_contexts(
    base: Dict[str, list], override: Dict[str, list]
) -> Dict[str, list]:
    """Merge two context dicts; override wins on key conflicts."""
    merged = dict(base)
    merged.update(override)
    return merged


def update_campaign_memory(
    *,
    run_id: str,
    program_id: str,
    data_root: Path,
    registry_root: Path,
    promising_top_k: int,
    avoid_top_k: int,
    repair_top_k: int,
    exploit_top_k: int,
    frontier_untested_top_k: int,
    frontier_repair_top_k: int,
    exhausted_failure_threshold: int,
) -> Dict[str, Any]:
    paths = ensure_memory_store(program_id, data_root=data_root)

    incoming_tested = build_tested_regions_snapshot(
        run_id=run_id, program_id=program_id, data_root=data_root
    )
    incoming_failures = build_failures_snapshot(
        run_id=run_id, program_id=program_id, data_root=data_root
    )
    reflection_row = build_run_reflection(run_id=run_id, program_id=program_id, data_root=data_root)
    reflection_df = pd.DataFrame([reflection_row])

    # Phase 1.3 — propagate reflection confidence into tested_regions so the
    # campaign controller can use confidence-weighted avoidance instead of
    # binary region-key blocking.
    if not incoming_tested.empty and "confidence" in reflection_row:
        run_confidence = float(reflection_row.get("confidence") or 0.0)
        incoming_tested = incoming_tested.copy()
        incoming_tested["failure_confidence"] = run_confidence

    tested_regions = _merge_by_keys(
        read_memory_table(program_id, "tested_regions", data_root=data_root),
        incoming_tested,
        ["run_id", "candidate_id", "region_key"],
    )
    failures = _merge_by_keys(
        read_memory_table(program_id, "failures", data_root=data_root),
        incoming_failures,
        ["run_id", "stage", "failure_class", "artifact_path"],
    )
    failures = _sanitize_failures(failures)

    # Phase 2.4 — Supersession tracking.
    # If the current run produced no failures for stages that were previously
    # failing, mark those old failure records as superseded so the controller's
    # repair queue no longer proposes them.
    existing_failures = _sanitize_failures(
        read_memory_table(program_id, "failures", data_root=data_root)
    )
    if not existing_failures.empty and "stage" in existing_failures.columns:
        stages_that_failed_before = set(
            existing_failures[
                existing_failures["superseded_by_run_id"].astype(str).str.strip() == ""
            ]["stage"].astype(str).unique()
        )
        new_failure_stages = set(incoming_failures["stage"].astype(str).unique()) if not incoming_failures.empty else set()
        recovered_stages = stages_that_failed_before - new_failure_stages
        for recovered_stage in sorted(recovered_stages):
            failures = mark_failures_superseded(
                failures,
                current_run_id=run_id,
                stage=recovered_stage,
                program_id=program_id,
            )
    reflections = _merge_by_keys(
        read_memory_table(program_id, "reflections", data_root=data_root),
        reflection_df,
        ["run_id"],
    )

    write_memory_table(program_id, "tested_regions", tested_regions, data_root=data_root)
    write_memory_table(program_id, "failures", failures, data_root=data_root)
    write_memory_table(program_id, "reflections", reflections, data_root=data_root)
    write_memory_table(
        program_id,
        "region_statistics",
        compute_region_statistics(tested_regions),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "event_statistics",
        compute_event_statistics(tested_regions),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "template_statistics",
        compute_template_statistics(tested_regions),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "context_statistics",
        compute_context_statistics(tested_regions),
        data_root=data_root,
    )

    _write_json(
        paths.belief_state,
        _build_belief_state(
            tested_regions=tested_regions,
            failures=failures,
            reflection=reflection_row,
            promising_top_k=promising_top_k,
            avoid_top_k=avoid_top_k,
            repair_top_k=repair_top_k,
        ),
    )
    _write_json(
        paths.next_actions,
        _build_next_actions(
            reflection=reflection_row,
            tested_regions=tested_regions,
            failures=failures,
            regime_conditional_candidates=_load_regime_conditional_candidates(
                run_id=run_id, data_root=data_root
            ),
            exploit_top_k=exploit_top_k,
            repair_top_k=repair_top_k,
        ),
    )

    compatibility = update_search_intelligence(
        data_root,
        registry_root,
        program_id,
        summary_top_k=max(int(promising_top_k), int(exploit_top_k)),
        frontier_untested_top_k=int(frontier_untested_top_k),
        frontier_repair_top_k=int(frontier_repair_top_k),
        exhausted_failure_threshold=int(exhausted_failure_threshold),
    )
    rollup_path = write_campaign_memory_rollup(
        program_id=program_id,
        data_root=data_root,
    )
    return {
        "tested_regions_rows": int(len(incoming_tested)),
        "failures_rows": int(len(incoming_failures)),
        "reflection_written": True,
        "compatibility_summary_status": compatibility["summary"].get("status", "ok"),
        "memory_root": str(paths.root),
        "campaign_memory_rollup_path": str(rollup_path),
        "promising_top_k": int(promising_top_k),
        "repair_top_k": int(repair_top_k),
        "frontier_untested_top_k": int(frontier_untested_top_k),
        "exhausted_failure_threshold": int(exhausted_failure_threshold),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update campaign memory from run artifacts.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--program_id", default="")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--promising_top_k", type=int, default=5)
    parser.add_argument("--avoid_top_k", type=int, default=5)
    parser.add_argument("--repair_top_k", type=int, default=5)
    parser.add_argument("--exploit_top_k", type=int, default=3)
    parser.add_argument("--frontier_untested_top_k", type=int, default=3)
    parser.add_argument("--frontier_repair_top_k", type=int, default=2)
    parser.add_argument("--exhausted_failure_threshold", type=int, default=3)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    data_root = Path(args.data_root) if args.data_root else get_data_root()
    manifest_path = data_root / "runs" / str(args.run_id) / "run_manifest.json"
    if manifest_path.exists():
        try:
            run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            run_manifest = {}
    else:
        run_manifest = load_run_manifest(str(args.run_id))
    program_id = str(args.program_id or run_manifest.get("program_id", "")).strip()
    if not program_id:
        _LOG.info(
            "Skipping campaign memory update for run %s because no program_id was provided.",
            args.run_id,
        )
        return 0

    manifest = start_manifest("update_campaign_memory", str(args.run_id), vars(args), [], [])
    try:
        diagnostics = update_campaign_memory(
            run_id=str(args.run_id),
            program_id=program_id,
            data_root=data_root,
            registry_root=Path(args.registry_root),
            promising_top_k=int(args.promising_top_k),
            avoid_top_k=int(args.avoid_top_k),
            repair_top_k=int(args.repair_top_k),
            exploit_top_k=int(args.exploit_top_k),
            frontier_untested_top_k=int(args.frontier_untested_top_k),
            frontier_repair_top_k=int(args.frontier_repair_top_k),
            exhausted_failure_threshold=int(args.exhausted_failure_threshold),
        )
        paths = ensure_memory_store(program_id, data_root=data_root)
        manifest["outputs"] = [
            {
                "path": str(paths.tested_regions),
                "artifact_type": "experiment.memory.tested_regions",
            },
            {"path": str(paths.reflections), "artifact_type": "experiment.memory.reflections"},
            {"path": str(paths.failures), "artifact_type": "experiment.memory.failures"},
            {
                "path": str(diagnostics.get("campaign_memory_rollup_path", "")),
                "artifact_type": "experiment.memory.rollup",
            },
        ]
        finalize_manifest(
            manifest,
            status="success",
            stats=diagnostics,
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, status="failed", error=str(exc))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
