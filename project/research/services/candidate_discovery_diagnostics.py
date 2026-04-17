from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def sample_quality_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "candidates_total": 0,
            "zero_validation_rows": 0,
            "zero_test_rows": 0,
            "zero_eval_rows": 0,
            "median_validation_n_obs": 0.0,
            "median_test_n_obs": 0.0,
            "median_n_obs": 0.0,
        }
    validation = pd.to_numeric(
        df["validation_n_obs"]
        if "validation_n_obs" in df.columns
        else pd.Series(0, index=df.index),
        errors="coerce",
    ).fillna(0)
    test = pd.to_numeric(
        df["test_n_obs"] if "test_n_obs" in df.columns else pd.Series(0, index=df.index),
        errors="coerce",
    ).fillna(0)
    n_obs = pd.to_numeric(
        df["n_obs"] if "n_obs" in df.columns else pd.Series(0, index=df.index), errors="coerce"
    ).fillna(0)
    return {
        "candidates_total": int(len(df)),
        "zero_validation_rows": int((validation <= 0).sum()),
        "zero_test_rows": int((test <= 0).sum()),
        "zero_eval_rows": int(((validation <= 0) & (test <= 0)).sum()),
        "median_validation_n_obs": float(validation.median()) if not validation.empty else 0.0,
        "median_test_n_obs": float(test.median()) if not test.empty else 0.0,
        "median_n_obs": float(n_obs.median()) if not n_obs.empty else 0.0,
    }


def survivor_quality_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "survivors_total": 0,
            "median_q_value": 1.0,
            "median_q_value_by": 1.0,
            "median_estimate_bps": 0.0,
            "median_cost_bps": 0.0,
            "median_discovery_quality_score": 0.0,
            "v2_demotion_reasons": {},
            "families_with_survivors": 0,
        }
    survivors = df[
        pd.to_numeric(df.get("is_discovery", False), errors="coerce").fillna(0).astype(bool)
    ].copy()
    if survivors.empty:
        return {
            "survivors_total": 0,
            "median_q_value": 1.0,
            "median_q_value_by": 1.0,
            "median_estimate_bps": 0.0,
            "median_cost_bps": 0.0,
            "median_discovery_quality_score": 0.0,
            "v2_demotion_reasons": {},
            "families_with_survivors": 0,
        }
    # Family concentration
    family_counts = {}
    if "event_family" in survivors.columns or "family_id" in survivors.columns:
        fam_col = "event_family" if "event_family" in survivors.columns else "family_id"
        family_counts = {
            str(k): int(v) for k, v in survivors[fam_col].value_counts().head(10).to_dict().items()
        }

    return {
        "survivors_total": int(len(survivors)),
        "median_q_value": float(
            pd.to_numeric(
                survivors["q_value"]
                if "q_value" in survivors.columns
                else pd.Series(1.0, index=survivors.index),
                errors="coerce",
            )
            .fillna(1.0)
            .median()
        ),
        "median_q_value_by": float(
            pd.to_numeric(
                survivors["q_value_by"]
                if "q_value_by" in survivors.columns
                else pd.Series(1.0, index=survivors.index),
                errors="coerce",
            )
            .fillna(1.0)
            .median()
        ),
        "median_estimate_bps": float(
            pd.to_numeric(
                survivors["estimate_bps"]
                if "estimate_bps" in survivors.columns
                else pd.Series(0.0, index=survivors.index),
                errors="coerce",
            )
            .fillna(0.0)
            .median()
        ),
        "median_cost_bps": float(
            pd.to_numeric(
                survivors["resolved_cost_bps"]
                if "resolved_cost_bps" in survivors.columns
                else pd.Series(0.0, index=survivors.index),
                errors="coerce",
            )
            .fillna(0.0)
            .median()
        ),
        "median_discovery_quality_score": float(
            pd.to_numeric(
                survivors.get("discovery_quality_score", np.nan), errors="coerce"
            ).median()
        )
        if "discovery_quality_score" in survivors.columns
        else 0.0,
        "v2_demotion_reasons": (
            {
                str(k): int(v)
                for k, v in survivors["rank_primary_reason"].value_counts().to_dict().items()
            }
            if "rank_primary_reason" in survivors.columns
            else {}
        ),
        "families_with_survivors": int(survivors["family_id"].nunique())
        if "family_id" in survivors.columns
        else 0,
        "family_concentration": family_counts,
    }


def build_false_discovery_diagnostics(combined: pd.DataFrame) -> dict[str, Any]:
    gate_rejections = (
        pd.to_numeric(
            combined.get("rejected_by_sample_quality", pd.Series(False, index=combined.index)),
            errors="coerce",
        )
        .fillna(0)
        .astype(bool)
    )
    survivors_before_gate = (
        pd.to_numeric(
            combined.get("is_discovery_pre_sample_quality", pd.Series(False, index=combined.index)),
            errors="coerce",
        )
        .fillna(0)
        .astype(bool)
    )
    fail_reason_counts = (
        combined.loc[gate_rejections, "sample_quality_fail_reason"]
        .astype(str)
        .value_counts()
        .to_dict()
        if "sample_quality_fail_reason" in combined.columns and bool(gate_rejections.any())
        else {}
    )
    if combined.empty:
        return {
            "global": {
                "candidates_total": 0,
                "symbols_total": 0,
                "survivors_total": 0,
                "families_total": 0,
            },
            "sample_quality": sample_quality_summary(combined),
            "sample_quality_gate": {
                "survivors_before_gate": 0,
                "survivors_after_gate": 0,
                "rejected_by_sample_quality_gate": 0,
                "fail_reason_counts": {},
            },
            "survivor_quality": survivor_quality_summary(combined),
            "ledger_diagnostics": build_ledger_diagnostics(combined),
            "by_symbol": {},
        }

    by_symbol: dict[str, Any] = {}
    for symbol, sym_df in combined.groupby("symbol", sort=True):
        by_symbol[str(symbol)] = {
            "sample_quality": sample_quality_summary(sym_df),
            "sample_quality_gate": {
                "survivors_before_gate": int(
                    pd.to_numeric(
                        sym_df.get(
                            "is_discovery_pre_sample_quality", pd.Series(False, index=sym_df.index)
                        ),
                        errors="coerce",
                    )
                    .fillna(0)
                    .astype(bool)
                    .sum()
                ),
                "survivors_after_gate": int(
                    pd.to_numeric(sym_df.get("is_discovery", False), errors="coerce")
                    .fillna(0)
                    .astype(bool)
                    .sum()
                ),
                "rejected_by_sample_quality_gate": int(
                    pd.to_numeric(
                        sym_df.get(
                            "rejected_by_sample_quality", pd.Series(False, index=sym_df.index)
                        ),
                        errors="coerce",
                    )
                    .fillna(0)
                    .astype(bool)
                    .sum()
                ),
                "fail_reason_counts": (
                    sym_df.loc[
                        pd.to_numeric(
                            sym_df.get(
                                "rejected_by_sample_quality", pd.Series(False, index=sym_df.index)
                            ),
                            errors="coerce",
                        )
                        .fillna(0)
                        .astype(bool),
                        "sample_quality_fail_reason",
                    ]
                    .astype(str)
                    .value_counts()
                    .to_dict()
                    if "sample_quality_fail_reason" in sym_df.columns
                    else {}
                ),
            },
            "survivor_quality": survivor_quality_summary(sym_df),
        }

    return {
        "global": {
            "candidates_total": int(len(combined)),
            "symbols_total": int(combined["symbol"].nunique())
            if "symbol" in combined.columns
            else 0,
            "survivors_total": int(
                pd.to_numeric(combined.get("is_discovery", False), errors="coerce")
                .fillna(0)
                .astype(bool)
                .sum()
            ),
            "families_total": int(combined["family_id"].nunique())
            if "family_id" in combined.columns
            else 0,
        },
        "sample_quality": sample_quality_summary(combined),
        "sample_quality_gate": {
            "survivors_before_gate": int(survivors_before_gate.sum()),
            "survivors_after_gate": int(
                pd.to_numeric(combined.get("is_discovery", False), errors="coerce")
                .fillna(0)
                .astype(bool)
                .sum()
            ),
            "rejected_by_sample_quality_gate": int(gate_rejections.sum()),
            "fail_reason_counts": {
                str(key): int(value) for key, value in fail_reason_counts.items()
            },
        },
        "survivor_quality": survivor_quality_summary(combined),
        "v2_scoring_diagnostics": build_v2_scoring_diagnostics(combined),
        "ledger_diagnostics": build_ledger_diagnostics(combined),
        "by_symbol": by_symbol,
    }


def build_v2_scoring_diagnostics(df: pd.DataFrame) -> dict[str, Any]:
    """Summary of V2 quality score components and rank movers."""
    if df.empty or "discovery_quality_score" not in df.columns:
        return {
            "v2_scoring_enabled": False,
            "rank_movers_v1_v2": [],
            "rank_movers_v2_v3": [],
            "penalty_counts": {},
        }

    # Helper to compute rank movers
    def get_movers(df_sub, col_old, col_new):
        if col_old not in df_sub.columns or col_new not in df_sub.columns:
            return []
        old_scores = pd.to_numeric(df_sub[col_old], errors="coerce").fillna(-999)
        new_scores = pd.to_numeric(df_sub[col_new], errors="coerce").fillna(-999)
        old_rank = old_scores.rank(ascending=False, method="min").astype(int)
        new_rank = new_scores.rank(ascending=False, method="min").astype(int)
        delta = old_rank - new_rank
        movers_idx = delta.nlargest(10).index
        return [
            {
                "candidate_id": str(df_sub.loc[i].get("candidate_id", i)),
                "old_rank": int(old_rank.loc[i]),
                "new_rank": int(new_rank.loc[i]),
                "delta": int(delta.loc[i]),
            }
            for i in movers_idx
            if delta.loc[i] > 0
        ]

    # V1 vs V2 (t-stat vs discovery_quality_score)
    movers_v1_v2 = []
    if "t_stat" in df.columns:
        # Use abs(t_stat) as V1 equivalent
        df_tmp = df.copy()
        df_tmp["abs_t_stat"] = df_tmp["t_stat"].abs()
        movers_v1_v2 = get_movers(df_tmp, "abs_t_stat", "discovery_quality_score")

    # V2 vs V3 (discovery_quality_score vs discovery_quality_score_v3)
    movers_v2_v3 = get_movers(df, "discovery_quality_score", "discovery_quality_score_v3")

    # Penalty counts (from reason codes)
    penalty_counts = {}
    if "demotion_reason_codes" in df.columns:
        all_codes = []
        for codes in df["demotion_reason_codes"].fillna("").astype(str):
            all_codes.extend([c.strip() for c in codes.split("|") if c.strip()])
        penalty_counts = {
            str(k): int(v) for k, v in pd.Series(all_codes).value_counts().to_dict().items()
        }

    return {
        "v2_scoring_enabled": True,
        "rank_movers_v1_v2": movers_v1_v2,
        "rank_movers_v2_v3": movers_v2_v3,
        "penalty_counts": penalty_counts,
    }


def apply_sample_quality_gates(
    candidates_df: pd.DataFrame,
    *,
    min_validation_n_obs: int,
    min_test_n_obs: int,
    min_total_n_obs: int,
) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    out = candidates_df.copy()
    validation = pd.to_numeric(out.get("validation_n_obs", 0), errors="coerce").fillna(0)
    test = pd.to_numeric(out.get("test_n_obs", 0), errors="coerce").fillna(0)
    total = pd.to_numeric(out.get("n_obs", 0), errors="coerce").fillna(0)
    multiplicity_survivor = (
        pd.to_numeric(out.get("is_discovery", False), errors="coerce").fillna(0).astype(bool)
    )

    gate_validation = validation >= int(min_validation_n_obs)
    gate_test = test >= int(min_test_n_obs)
    gate_total = total >= int(min_total_n_obs)
    gate_sample_quality = gate_validation & gate_test & gate_total

    fail_reason = np.where(
        ~gate_validation,
        "min_validation_n_obs",
        np.where(~gate_test, "min_test_n_obs", np.where(~gate_total, "min_total_n_obs", "")),
    )

    out["gate_min_validation_n_obs"] = gate_validation.astype(bool)
    out["gate_min_test_n_obs"] = gate_test.astype(bool)
    out["gate_min_total_n_obs"] = gate_total.astype(bool)
    out["gate_sample_quality"] = gate_sample_quality.astype(bool)
    out["sample_quality_fail_reason"] = pd.Series(fail_reason, index=out.index).astype(str)
    out["is_discovery_pre_sample_quality"] = multiplicity_survivor.astype(bool)
    out["rejected_by_sample_quality"] = (multiplicity_survivor & ~gate_sample_quality).astype(bool)
    out["is_discovery"] = (multiplicity_survivor & gate_sample_quality).astype(bool)
    return out


# ---------------------------------------------------------------------------
# Phase 3 — Ledger diagnostics
# ---------------------------------------------------------------------------


def build_ledger_diagnostics(combined: pd.DataFrame) -> dict[str, Any]:
    """Build a diagnostics dict describing ledger-driven rank changes.

    Safe to call when ledger columns are absent (returns a minimal dict).
    """
    if combined is None or combined.empty:
        return {
            "ledger_adjustment_enabled": False,
            "lineages_covered": 0,
            "crowded_lineages": [],
            "top_burdened_candidates": [],
            "rank_demotions": [],
            "demotion_reason_counts": {},
            "lineages_with_repeated_failure": [],
            "ledger_coverage_rate": 0.0,
        }

    has_ledger = "ledger_multiplicity_penalty" in combined.columns
    has_v3 = "discovery_quality_score_v3" in combined.columns
    has_v2 = "discovery_quality_score" in combined.columns
    has_lineage = "concept_lineage_key" in combined.columns

    if not has_ledger:
        return {
            "ledger_adjustment_enabled": False,
            "lineages_covered": 0,
            "crowded_lineages": [],
            "top_burdened_candidates": [],
            "rank_demotions": [],
            "demotion_reason_counts": {},
            "lineages_with_repeated_failure": [],
            "ledger_coverage_rate": 0.0,
        }

    # Coverage: fraction of candidates with a non-null lineage key
    if has_lineage:
        lineage_col = combined["concept_lineage_key"].fillna("").astype(str)
        lineages_covered = int((lineage_col != "").sum())
        coverage_rate = float(lineages_covered / max(len(combined), 1))
        unique_lineages = lineage_col[lineage_col != ""].unique().tolist()
    else:
        lineages_covered = 0
        coverage_rate = 0.0
        unique_lineages = []

    # Crowded lineages (high prior test count)
    crowded: list[str] = []
    repeated_failure: list[str] = []
    if has_lineage and "ledger_prior_test_count" in combined.columns:
        lineage_burden = (
            combined.groupby("concept_lineage_key")["ledger_prior_test_count"].max().reset_index()
        )
        crowded = lineage_burden[lineage_burden["ledger_prior_test_count"] >= 20][
            "concept_lineage_key"
        ].tolist()

    if (
        has_lineage
        and "ledger_empirical_success_rate" in combined.columns
        and "ledger_prior_test_count" in combined.columns
    ):
        fail_df = (
            combined.groupby("concept_lineage_key")
            .agg(
                rate=("ledger_empirical_success_rate", "min"),
                tests=("ledger_prior_test_count", "max"),
            )
            .reset_index()
        )
        repeated_failure = fail_df[(fail_df["rate"] < 0.10) & (fail_df["tests"] >= 5)][
            "concept_lineage_key"
        ].tolist()

    # Top burdened candidates (highest penalty)
    penalty_col = pd.to_numeric(
        combined.get("ledger_multiplicity_penalty", 0), errors="coerce"
    ).fillna(0)
    top_n = min(10, int((penalty_col > 0).sum()))
    top_burdened: list[dict] = []
    if top_n > 0:
        top_idx = penalty_col.nlargest(top_n).index
        for idx in top_idx:
            row = combined.loc[idx]
            top_burdened.append(
                {
                    "candidate_id": str(row.get("candidate_id", "")).strip(),
                    "concept_lineage_key": str(row.get("concept_lineage_key", "")),
                    "ledger_multiplicity_penalty": float(
                        row.get("ledger_multiplicity_penalty", 0.0)
                    ),
                    "ledger_prior_test_count": int(row.get("ledger_prior_test_count", 0)),
                    "ledger_empirical_success_rate": float(
                        row.get("ledger_empirical_success_rate", 0.0)
                    ),
                }
            )

    # Rank demotions: candidates whose v3 rank is much worse than v2 rank
    rank_demotions: list[dict] = []
    if has_v2 and has_v3:
        score_v2 = pd.to_numeric(combined.get("discovery_quality_score", np.nan), errors="coerce")
        score_v3 = pd.to_numeric(
            combined.get("discovery_quality_score_v3", np.nan), errors="coerce"
        )
        valid_both = score_v2.notna() & score_v3.notna()
        if valid_both.any():
            ranked_v2 = score_v2[valid_both].rank(ascending=False, method="first").astype(int)
            ranked_v3 = score_v3[valid_both].rank(ascending=False, method="first").astype(int)
            demotion_delta = ranked_v3 - ranked_v2
            # Flag candidates demoted by 5+ positions
            big_demotions = demotion_delta[demotion_delta >= 5].nlargest(10)
            for idx, delta in big_demotions.items():
                row = combined.loc[idx]
                reason = str(row.get("demotion_reason_codes", "")).strip()
                ledger_codes = [
                    r
                    for r in reason.split("|")
                    if r.startswith(
                        ("crowded", "repeated", "low_empirical", "high_recent", "ledger")
                    )
                ]
                if not ledger_codes:
                    continue
                rank_demotions.append(
                    {
                        "candidate_id": str(row.get("candidate_id", "")).strip(),
                        "v2_rank": int(ranked_v2.loc[idx]),
                        "v3_rank": int(ranked_v3.loc[idx]),
                        "demotion_delta": int(delta),
                        "demotion_reason": "|".join(ledger_codes),
                    }
                )

    # Demotion reason code counts (ledger codes only)
    demotion_counts: dict[str, int] = {}
    if "demotion_reason_codes" in combined.columns:
        all_codes: list[str] = []
        for codes_str in combined["demotion_reason_codes"].fillna("").astype(str):
            for code in codes_str.split("|"):
                code = code.strip()
                if code and code in {
                    "crowded_lineage",
                    "repeated_family_failure",
                    "low_empirical_family_success",
                    "high_recent_test_density",
                    "ledger_penalty_applied",
                }:
                    all_codes.append(code)
        for code in all_codes:
            demotion_counts[code] = demotion_counts.get(code, 0) + 1

    return {
        "ledger_adjustment_enabled": True,
        "lineages_covered": lineages_covered,
        "unique_lineage_count": len(unique_lineages),
        "crowded_lineages": crowded[:20],
        "top_burdened_candidates": top_burdened,
        "rank_demotions": rank_demotions,
        "demotion_reason_counts": demotion_counts,
        "lineages_with_repeated_failure": repeated_failure[:20],
        "ledger_coverage_rate": round(coverage_rate, 4),
    }


# ---------------------------------------------------------------------------
# Phase 4 — Hierarchical stage diagnostics
# ---------------------------------------------------------------------------


def build_hierarchical_stage_diagnostics(
    stage_artifacts: "dict[str, pd.DataFrame]",
    *,
    flat_mode_equivalent_count: int = 0,
) -> "dict[str, Any]":
    """Build a diagnostics dict summarising hierarchical stage progression.

    Args:
        stage_artifacts: Dict keyed by stage name (e.g. "trigger_viability")
                         mapping to the full-stage candidate DataFrame (pass+fail rows).
        flat_mode_equivalent_count: Estimated number of hypotheses that flat
                                    mode would have evaluated.

    Returns a dict suitable for inclusion in the main diagnostics JSON report.
    """
    STAGE_ORDER = [
        "trigger_viability",
        "template_refinement",
        "execution_refinement",
        "context_refinement",
    ]
    REASON_CODES = {
        "failed_trigger_viability",
        "failed_template_refinement",
        "failed_execution_refinement",
        "failed_context_gain",
        "support_too_small_after_refinement",
        "context_overconditioning_penalty",
        "ledger_burden_exceeded",
    }

    if not stage_artifacts:
        return {
            "search_mode": "hierarchical",
            "stages": {},
            "flat_mode_equivalent_count": flat_mode_equivalent_count,
            "hierarchical_evaluated_count": 0,
            "pruning_efficiency": 0.0,
            "pruned_by_stage": {},
        }

    stage_diags: dict[str, dict[str, Any]] = {}
    total_evaluated = 0
    pruned_by_stage: dict[str, int] = {}

    for stage in STAGE_ORDER:
        df = stage_artifacts.get(stage)
        if df is None or df.empty:
            stage_diags[stage] = {
                "candidates_evaluated": 0,
                "survivors": 0,
                "drop_rate": 0.0,
                "top_triggers": [],
                "dropped_triggers": [],
                "drop_reasons": {},
            }
            pruned_by_stage[stage] = 0
            continue

        evaluated = len(df)
        total_evaluated += evaluated

        pass_col = df.get("stage_pass", pd.Series(False, index=df.index)).fillna(False).astype(bool)
        survivors = int(pass_col.sum())
        pruned = evaluated - survivors
        drop_rate = round(pruned / max(evaluated, 1), 4)
        pruned_by_stage[stage] = pruned

        # Top and dropped triggers
        event_col = "canonical_event_type" if "canonical_event_type" in df.columns else "event_type"
        top_triggers: list[str] = []
        dropped_triggers: list[str] = []
        if event_col in df.columns:
            top_triggers = df.loc[pass_col, event_col].dropna().astype(str).unique().tolist()[:10]
            dropped_triggers = (
                df.loc[~pass_col, event_col].dropna().astype(str).unique().tolist()[:10]
            )

        # Reason code breakdown (only Phase 4 codes)
        reason_col = df.get("stage_reason_code", pd.Series("", index=df.index)).fillna("")
        drop_reasons: dict[str, int] = {}
        for raw_reason in reason_col[~pass_col]:
            for code in str(raw_reason).split("|"):
                code = code.strip()
                if code and code in REASON_CODES:
                    drop_reasons[code] = drop_reasons.get(code, 0) + 1

        stage_body: dict[str, Any] = {
            "candidates_evaluated": evaluated,
            "survivors": survivors,
            "pruned": pruned,
            "drop_rate": drop_rate,
            "top_triggers": top_triggers,
            "dropped_triggers": dropped_triggers,
            "drop_reasons": drop_reasons,
        }

        # Context stage extras
        if stage == "context_refinement" and "context_gain_score" in df.columns:
            ctx_mask = df.get("context", pd.Series("", index=df.index)).apply(
                lambda v: bool(v) and isinstance(v, dict) and len(v) > 0
            )
            baseline_mask = ~ctx_mask
            stage_body["survivors_with_context"] = int((pass_col & ctx_mask).sum())
            stage_body["baseline_survivors"] = int((pass_col & baseline_mask).sum())
            stage_body["context_gains"] = [
                {
                    "candidate_id": str(row.get("candidate_id", "")),
                    "context_gain_score": float(row.get("context_gain_score", 0.0)),
                    "search_stage": str(row.get("search_stage", "context_refinement")),
                }
                for _, row in df[pass_col & ctx_mask].head(10).iterrows()
            ]

        stage_diags[stage] = stage_body

    hierarchical_evaluated_count = total_evaluated
    pruning_efficiency = (
        round(
            1.0 - (hierarchical_evaluated_count / max(flat_mode_equivalent_count, 1)),
            4,
        )
        if flat_mode_equivalent_count > 0
        else 0.0
    )

    return {
        "search_mode": "hierarchical",
        "stages": stage_diags,
        "flat_mode_equivalent_count": flat_mode_equivalent_count,
        "hierarchical_evaluated_count": hierarchical_evaluated_count,
        "pruning_efficiency": pruning_efficiency,
        "pruned_by_stage": pruned_by_stage,
    }


# ---------------------------------------------------------------------------
# Phase 5 — Diversification diagnostics
# ---------------------------------------------------------------------------


def build_diversification_diagnostics(
    candidates: "pd.DataFrame",
    shortlist: "pd.DataFrame | None" = None,
    diversification_config: "dict | None" = None,
) -> "dict[str, Any]":
    """Build a diagnostics dict explaining the Phase 5 diversification output.

    Args:
        candidates:             Full annotated candidate table (post Phase 5).
        shortlist:              Diversified shortlist DataFrame (may be empty/None).
        diversification_config: ``discovery_selection`` config block.

    Returns a dict suitable for inclusion in the main diagnostics JSON report.
    """
    config = diversification_config or {}
    shortlist_cfg = config.get("shortlist", {})
    shortlist_enabled = bool(shortlist_cfg.get("enabled", False))
    shortlist_size = int(shortlist_cfg.get("size", 20))

    if candidates is None or candidates.empty:
        return {
            "diversification_mode": str(config.get("mode", "greedy")),
            "shortlist_enabled": shortlist_enabled,
            "shortlist_size": shortlist_size,
            "total_candidates": 0,
            "overlap_cluster_count": 0,
            "duplicate_like_count": 0,
            "shortlist_actual_size": 0,
            "trigger_family_concentration": {},
            "lineage_concentration": {},
            "top_crowded_clusters": [],
            "strongest_excluded": [],
            "shortlist_vs_raw_top_n": {},
        }

    # Cluster counts
    cluster_col = candidates.get(
        "overlap_cluster_id", pd.Series("c0000", index=candidates.index)
    ).fillna("c0000")
    n_clusters = int(cluster_col.nunique())

    dup_col = candidates.get("is_duplicate_like", pd.Series(False, index=candidates.index))
    n_dup = int(dup_col.fillna(False).astype(bool).sum())

    # Trigger family concentration
    event_col = (
        "event_family"
        if "event_family" in candidates.columns
        else "canonical_event_type"
        if "canonical_event_type" in candidates.columns
        else "event_type"
    )
    trig_counts: dict[str, int] = {}
    if event_col in candidates.columns:
        for v in candidates[event_col].fillna("unknown").astype(str):
            trig_counts[v.upper()] = trig_counts.get(v.upper(), 0) + 1
    top_trig = dict(sorted(trig_counts.items(), key=lambda kv: -kv[1])[:10])

    # Lineage concentration
    lineage_col = candidates.get(
        "concept_lineage_key", pd.Series("", index=candidates.index)
    ).fillna("")
    lin_counts: dict[str, int] = {}
    for key in lineage_col.astype(str):
        if key:
            lin_counts[key] = lin_counts.get(key, 0) + 1
    top_lin = dict(sorted(lin_counts.items(), key=lambda kv: -kv[1])[:5])

    # Top crowded clusters
    cluster_size_col = candidates.get("cluster_size", pd.Series(1, index=candidates.index)).fillna(
        1
    )
    quality_col_name = _best_quality_col_diag(candidates)

    cluster_summaries: list[dict] = []
    seen_clusters: set[str] = set()
    for _, row in candidates[cluster_size_col > 1].iterrows():
        clu = str(cluster_col.loc[row.name] if row.name in cluster_col.index else "c0000")
        if clu in seen_clusters:
            continue
        seen_clusters.add(clu)
        clu_members = candidates[cluster_col == clu]
        top_q = (
            pd.to_numeric(clu_members.get(quality_col_name, 0), errors="coerce").fillna(0.0).max()
        )
        top_member = str(
            clu_members.loc[clu_members.get(quality_col_name, pd.Series(0)).idxmax(), :].get(
                "candidate_id", ""
            )
            if not clu_members.empty
            else ""
        )
        cluster_summaries.append(
            {
                "cluster_id": clu,
                "size": int(clu_members["cluster_size"].iloc[0])
                if "cluster_size" in clu_members.columns
                else len(clu_members),
                "density": float(clu_members["cluster_density"].iloc[0])
                if "cluster_density" in clu_members.columns
                else 0.0,
                "top_member": top_member,
                "top_quality": round(float(top_q), 4),
            }
        )
    top_crowded = sorted(cluster_summaries, key=lambda d: -d["size"])[:10]

    # Strongest excluded (selected_into_diversified_shortlist == False, sorted by quality)
    shortlist_actual_size = 0
    strongest_excluded: list[dict] = []
    if shortlist_enabled and "selected_into_diversified_shortlist" in candidates.columns:
        selected_mask = candidates["selected_into_diversified_shortlist"].fillna(False).astype(bool)
        excluded = candidates[~selected_mask].copy()
        shortlist_actual_size = int(selected_mask.sum())
        exc_quality = pd.to_numeric(excluded.get(quality_col_name, 0), errors="coerce").fillna(0.0)
        exc_sorted = excluded.assign(_q=exc_quality).sort_values("_q", ascending=False).head(10)
        reason_col = candidates.get(
            "selection_reason", pd.Series("", index=candidates.index)
        ).fillna("")
        for _, r in exc_sorted.iterrows():
            strongest_excluded.append(
                {
                    "candidate_id": str(r.get("candidate_id", "")),
                    "quality_score": round(float(r.get("_q", 0.0)), 4),
                    "cluster_id": str(cluster_col.get(r.name, "")),
                    "exclusion_reason": str(reason_col.get(r.name, "not_selected")),
                }
            )
    elif shortlist is not None and not shortlist.empty:
        shortlist_actual_size = len(shortlist)

    # Shortlist vs raw top-N comparison
    shortlist_vs_top_n: dict[str, Any] = {}
    shortlist_df = shortlist or pd.DataFrame()
    if not shortlist_df.empty and not candidates.empty:
        top_n = candidates.head(int(shortlist_size)).copy()
        top_n_q = pd.to_numeric(top_n.get(quality_col_name, 0), errors="coerce").fillna(0.0)
        sl_q = pd.to_numeric(shortlist_df.get(quality_col_name, 0), errors="coerce").fillna(0.0)
        top_n_clusters = top_n.get(
            "overlap_cluster_id", pd.Series("c0", index=top_n.index)
        ).nunique()
        sl_clusters = shortlist_df.get(
            "overlap_cluster_id", pd.Series("c0", index=shortlist_df.index)
        ).nunique()
        shortlist_vs_top_n = {
            "raw_top_n_quality_mean": round(float(top_n_q.mean()), 4) if not top_n_q.empty else 0.0,
            "shortlist_quality_mean": round(float(sl_q.mean()), 4) if not sl_q.empty else 0.0,
            "raw_top_n_cluster_count": int(top_n_clusters),
            "shortlist_cluster_count": int(sl_clusters),
        }

    return {
        "diversification_mode": str(config.get("mode", "greedy")),
        "shortlist_enabled": shortlist_enabled,
        "shortlist_size": shortlist_size,
        "shortlist_actual_size": shortlist_actual_size,
        "total_candidates": len(candidates),
        "overlap_cluster_count": n_clusters,
        "duplicate_like_count": n_dup,
        "trigger_family_concentration": top_trig,
        "lineage_concentration": top_lin,
        "top_crowded_clusters": top_crowded,
        "strongest_excluded": strongest_excluded,
        "shortlist_vs_raw_top_n": shortlist_vs_top_n,
    }


def _best_quality_col_diag(df: "pd.DataFrame") -> str:
    for col in ("discovery_quality_score_v3", "discovery_quality_score", "t_stat"):
        if col in df.columns:
            return col
    return "t_stat"
