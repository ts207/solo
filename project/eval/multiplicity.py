from project.io.utils import write_parquet, read_parquet
import numpy as np
import pandas as pd
import hashlib
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

from project.core.stats import bh_adjust
from project.research.validation.multiple_testing import (
    adjust_pvalues_by,
)


def _normalize_direction_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        if not np.isfinite(numeric):
            return ""
        if numeric > 0:
            return "long"
        if numeric < 0:
            return "short"
        return "flat"
    text = str(value).strip().lower()
    if text in {"", "nan", "none"}:
        return ""
    if text in {"1", "+1", "1.0", "+1.0", "long", "buy", "up", "bull", "bullish"}:
        return "long"
    if text in {"-1", "-1.0", "short", "sell", "down", "bear", "bearish"}:
        return "short"
    if text in {"0", "0.0", "flat", "neutral", "both"}:
        return "flat"
    return text


def _normalize_hypothesis_log_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "direction" in out.columns:
        out["direction"] = out["direction"].apply(_normalize_direction_value)
    return out


def benjamini_hochberg(
    p_values: List[float], alpha: float = 0.05
) -> Tuple[List[bool], List[float]]:
    """Apply Benjamini-Hochberg procedure for FDR control.

    This delegates to the shared research validation adjustment backend so
    eval/reporting paths share the same q-value implementation.
    """
    q_values = bh_adjust(np.array(p_values))
    if len(q_values) == 0:
        return [], []
    reject = np.asarray(q_values, dtype=float) <= float(alpha)
    return reject.tolist(), np.asarray(q_values, dtype=float).tolist()


def benjamini_yekutieli(
    p_values: List[float], alpha: float = 0.05
) -> Tuple[List[bool], List[float]]:
    """Apply Benjamini-Yekutieli procedure for FDR control under dependency.

    This delegates to the research validation adjustment backend to avoid
    divergence between evaluation and research multiplicity math.
    """
    q_values = adjust_pvalues_by(p_values)
    if len(q_values) == 0:
        return [], []
    reject = np.asarray(q_values, dtype=float) <= float(alpha)
    return reject.tolist(), np.asarray(q_values, dtype=float).tolist()


def _load_mechanism_group_map() -> Dict[str, str]:
    """Return a mapping of event_type -> primary mechanism tag (first tag in list).

    Loads from the live events registry so the FDR grouping stays in sync with
    tag assignments in events.yaml.  Falls back to an empty dict on any IO error
    so callers degrade gracefully rather than raising.
    """
    try:
        import yaml as _yaml
        from pathlib import Path as _Path

        candidates = [
            _Path(__file__).resolve().parents[2] / "project" / "configs" / "registries" / "events.yaml",
            _Path(__file__).resolve().parents[1] / "configs" / "registries" / "events.yaml",
        ]
        registry_path = next((p for p in candidates if p.exists()), None)
        if registry_path is None:
            return {}

        with open(registry_path) as fh:
            data = _yaml.safe_load(fh)

        mapping: Dict[str, str] = {}
        for event_type, cfg in data.get("events", {}).items():
            tags = cfg.get("tags") or []
            if tags:
                mapping[str(event_type)] = str(tags[0])
        return mapping
    except Exception:
        return {}


def compute_multiplicity_metrics(df: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    """Compute multiple q-value variants: Global, Mechanism, Family, Cluster-adjusted, BY.

    Layer hierarchy
    ---------------
    1. Global     - all hypotheses pooled (most conservative)
    2. Mechanism  - grouped by primary mechanism tag from events.yaml; operative FDR gate
    3. Family     - family-specific grouping retained for audit and diagnostics
    4. Cluster    - behavior-signature hash groups (finest grain)
    5. BY         - Benjamini-Yekutieli diagnostic under arbitrary dependence
    """
    if df.empty:
        return df
    out = df.copy()

    # 1. Global Q (BH)
    _, out["q_value_global"] = benjamini_hochberg(out["p_value"].fillna(1.0).tolist(), alpha)

    # 2. Mechanism Q (BH per primary mechanism tag) -- operative FDR gate
    mechanism_map = _load_mechanism_group_map()
    if "mechanism_group" not in out.columns and "event_type" in out.columns:
        out["mechanism_group"] = out["event_type"].map(mechanism_map).fillna("untagged")
    if "mechanism_group" in out.columns:
        out["q_value_mechanism"] = 1.0
        for mgrp, group in out.groupby("mechanism_group"):
            _, qvals = benjamini_hochberg(group["p_value"].fillna(1.0).tolist(), alpha)
            out.loc[group.index, "q_value_mechanism"] = qvals

    # 3. Family Q (BH per family_id) -- legacy audit column, no longer drives promotion
    if "family_id" in out.columns:
        out["q_value_family"] = 1.0
        for fid, group in out.groupby("family_id"):
            _, qvals = benjamini_hochberg(group["p_value"].fillna(1.0).tolist(), alpha)
            out.loc[group.index, "q_value_family"] = qvals

    # 4. Cluster-adjusted Q (BH per cluster_id)
    if "cluster_id" in out.columns:
        out["q_value_cluster"] = 1.0
        for cid, group in out.groupby("cluster_id"):
            _, qvals = benjamini_hochberg(group["p_value"].fillna(1.0).tolist(), alpha)
            out.loc[group.index, "q_value_cluster"] = qvals

    # 5. BY Diagnostic Q
    _, out["q_value_by"] = benjamini_yekutieli(out["p_value"].fillna(1.0).tolist(), alpha)

    return out


def get_program_hypothesis_log_path(program_id: str, data_root: Path) -> Path:
    return data_root / "research" / "programs" / program_id / "hypothesis_log.parquet"


def update_program_hypothesis_log(
    program_id: str, data_root: Path, new_hypotheses: pd.DataFrame
) -> pd.DataFrame:
    """M2: Maintain per-research-program tested-hypothesis log."""
    log_path = get_program_hypothesis_log_path(program_id, data_root)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    existing_path = log_path if log_path.exists() else log_path.with_suffix(".csv")
    normalized_new = _normalize_hypothesis_log_frame(new_hypotheses)
    if existing_path.exists():
        existing_log = _normalize_hypothesis_log_frame(read_parquet(existing_path))
        combined = pd.concat([existing_log, normalized_new], ignore_index=True).drop_duplicates(
            subset=["hypothesis_id"]
        )
    else:
        combined = normalized_new

    write_parquet(combined, log_path)
    return combined


def apply_program_multiplicity_control(
    candidates: pd.DataFrame, program_id: str, data_root: Path, alpha: float = 0.05
) -> pd.DataFrame:
    """M2: Apply multiplicity to confirmatory universe (Program-level)."""
    log_path = get_program_hypothesis_log_path(program_id, data_root)
    existing_path = log_path if log_path.exists() else log_path.with_suffix(".csv")
    if not existing_path.exists():
        return compute_multiplicity_metrics(candidates, alpha)

    preserved_candidate_metrics = {}
    for col in ("q_value", "q_value_family", "q_value_cluster", "q_value_by"):
        if col in candidates.columns:
            preserved_candidate_metrics[col] = pd.to_numeric(candidates[col], errors="coerce")

    full_universe = read_parquet(existing_path)
    # We need p-values for all hypotheses in the universe to do global program-level control.
    # If some runs didn't produce p-values for all hypotheses, we assume 1.0 (conservative).

    universe_pvals = full_universe["p_value"].fillna(1.0).tolist()
    _, universe_qvals = benjamini_hochberg(universe_pvals, alpha)
    full_universe["q_value_program"] = universe_qvals

    # Map back to candidates
    q_map = full_universe.set_index("hypothesis_id")["q_value_program"].to_dict()
    candidates["q_value_program"] = candidates["hypothesis_id"].map(q_map).fillna(1.0)

    out = compute_multiplicity_metrics(candidates, alpha)
    for col, preserved in preserved_candidate_metrics.items():
        if col not in out.columns:
            out[col] = preserved
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
        keep_mask = preserved.notna()
        out.loc[keep_mask, col] = preserved.loc[keep_mask]
    return out


def formalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    """M1: Formalize family, cluster, and mechanism group assignment rules."""
    out = df.copy()

    # Mechanism Group = Primary mechanism tag from events.yaml
    if "mechanism_group" not in out.columns:
        mechanism_map = _load_mechanism_group_map()
        if "event_type" in out.columns:
            out["mechanism_group"] = out["event_type"].map(mechanism_map).fillna("untagged")

    if "family_id" not in out.columns:
        # Family = Event Type + Horizon
        out["family_id"] = out.apply(
            lambda r: f"fam_{r.get('event_type')}_{r.get('horizon')}", axis=1
        )

    if "cluster_id" not in out.columns:
        # Cluster = Behavior Signature (bounded correlation proxy)
        out["cluster_id"] = out.get("behavior_signature_hash", out["family_id"])

    return out


def apply_multiplicity_control(candidates: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    """Compatibility shim for tests."""
    if "p_value" not in candidates.columns:
        raise ValueError("DataFrame must contain 'p_value' column")
    _, qvals = benjamini_hochberg(candidates["p_value"].fillna(1.0).tolist(), alpha)
    candidates["pass_fdr"] = [q <= alpha for q in qvals]
    candidates["q_value_bh"] = qvals
    return candidates


def report_discoveries(candidates: pd.DataFrame, alpha: float = 0.05) -> Dict[str, Any]:
    """Compatibility shim for tests."""
    total = len(candidates)
    passed = candidates["pass_fdr"].sum() if "pass_fdr" in candidates.columns else 0
    return {
        "total_hypotheses": int(total),
        "fdr_target": alpha,
        "discoveries": int(passed),
        "discovery_rate": float(passed / total) if total > 0 else 0.0,
    }
