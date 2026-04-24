from __future__ import annotations

import logging
from typing import Any, List

import numpy as np
import pandas as pd

from project.research.knowledge.concept_ledger import build_concept_lineage_key
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.search.evaluator_utils import trigger_mask
from project.research.trigger_discovery.candidate_generation import TriggerProposal
from project.research.trigger_discovery.registry_comparison import compute_registry_overlaps

log = logging.getLogger(__name__)

def score_trigger_candidates(
    proposals: List[TriggerProposal],
    features: pd.DataFrame,
    folds: list[Any] = None,
    cost_bps: float = 2.0
) -> pd.DataFrame:
    """
    Score the proposals using the rigorous evaluator toolset plus novelty proxies.
    Returns a dataframe describing each candidate trigger.
    """
    if not proposals or features.empty:
        return pd.DataFrame()

    specs = [p.spec for p in proposals]
    metrics_df = evaluate_hypothesis_batch(specs, features, folds=folds, cost_bps=cost_bps)

    if metrics_df.empty:
        return pd.DataFrame()

    scored_rows = []

    # Track evaluated spec -> metrics
    metrics_map = {row["hypothesis_id"]: row for _, row in metrics_df.iterrows()}

    for p in proposals:
        h_id = p.spec.hypothesis_id()
        m = metrics_map.get(h_id)
        if m is None or not m.get("valid", False):
            continue

        # Registry overlap
        mask = trigger_mask(p.spec, features)
        overlaps = compute_registry_overlaps(mask, features)

        row = {
            "candidate_trigger_id": p.candidate_trigger_id,
            "source_lane": p.source_lane,
            "detector_family": p.detector_family,
            "parameterization": p.parameterization,
            "dominant_features": p.dominant_features,
            "suggested_trigger_name": p.suggested_trigger_name,

            # Evidence
            "support_count": int(m.get("n", 0)),
            "after_cost_expectancy_bps": float(m.get("after_cost_expectancy_bps", 0.0)),
            "t_stat": float(m.get("t_stat", 0.0)),
            "hit_rate": float(m.get("hit_rate", 0.0)),
            "robustness_score": float(m.get("robustness_score", 0.0)),

            # Fold evidence
            "fold_count": int(m.get("fold_count", 0)),
            "fold_valid_count": int(m.get("fold_valid_count", 0)),
            "fold_sign_consistency": float(m.get("fold_sign_consistency", 0.0)),
            "fold_median_t_stat": float(m.get("fold_median_t_stat", 0.0)),
            "fold_stability_score": float(m.get("fold_sign_consistency", 0.0)), # Proxy

            # Novelty vs existing registry
            **overlaps
        }

        # Concept lineage (to penalize known structures if needed)
        # Standardize lineage keys to emulate edge lines
        dummy_row = {
            "event_type": p.detector_family.upper(),
            "rule_template": p.spec.template_id,
            "direction": p.spec.direction,
            "timeframe": "5m",
            "horizon_bars": p.spec.horizon.replace("b", "")
        }
        lk = build_concept_lineage_key(dummy_row)
        row["concept_lineage_key"] = lk

        scored_rows.append(row)

    df = pd.DataFrame(scored_rows)

    if df.empty:
        return df

    # We apply lineage penalty but use a fake ledger payload because we only care
    # about internal ledger multiplicity inside this sweep space for scaling
    try:
        # Instead of deep ledger dependency, compute internal crowding penalty
        # simple: penalize families with high counts in this proposal batch
        family_counts = df["detector_family"].value_counts()
        df["lineage_burden_penalty"] = df["detector_family"].apply(
            lambda dev: float(min(1.0, np.log1p(family_counts.get(dev, 1)) * 0.1))
        )
    except Exception:
        df["lineage_burden_penalty"] = 0.0

    # Trigger Candidate Quality Score formulation
    # Emphasize distinctiveness, interpretability, and robust stats
    norm_t = (df["t_stat"] - df["t_stat"].mean()) / (df["t_stat"].std() + 1e-9)
    norm_t = norm_t.clip(-2, 2)

    df["trigger_candidate_quality_score"] = (
        norm_t * 0.4
        + df["fold_stability_score"] * 0.3
        + df["novelty_vs_registry_score"] * 0.4
        - df["lineage_burden_penalty"] * 0.2
    )

    if "registry_redundancy_flag" in df.columns:
        df.loc[df["registry_redundancy_flag"], "trigger_candidate_quality_score"] -= 1.0

    df["review_status"] = "proposed"
    df["warnings"] = ""

    # Append warnings
    df.loc[df["support_count"] < 30, "warnings"] += "Low support count. "
    df.loc[df["fold_stability_score"] < 0.5, "warnings"] += "Unstable across holdout folds. "
    df.loc[df["registry_redundancy_flag"], "warnings"] += "Highly redundant with existing canonical triggers. "

    return df.sort_values(by="trigger_candidate_quality_score", ascending=False)
