"""
Canonical search-burden contract.

Defines the fields and normalization logic for search-burden accounting
across discovery → promotion → evidence/export.

Every candidate and promotion artifact should explain the search universe
it emerged from. These fields are additive to multiplicity accounting
(Workstream A) and do not replace it.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

SEARCH_BURDEN_FIELDS: List[str] = [
    "search_proposals_attempted",
    "search_candidates_generated",
    "search_candidates_scored",
    "search_candidates_eligible",
    "search_parameterizations_attempted",
    "search_mutations_attempted",
    "search_directions_tested",
    "search_confirmations_attempted",
    "search_trigger_variants_attempted",
    "search_family_count",
    "search_lineage_count",
    "search_scope_version",
    "search_burden_estimated",
]

SEARCH_BURDEN_NUMERIC_FIELDS: List[str] = [
    "search_proposals_attempted",
    "search_candidates_generated",
    "search_candidates_scored",
    "search_candidates_eligible",
    "search_parameterizations_attempted",
    "search_mutations_attempted",
    "search_directions_tested",
    "search_confirmations_attempted",
    "search_trigger_variants_attempted",
    "search_family_count",
    "search_lineage_count",
]

DEFAULT_SEARCH_BURDEN_VERSION = "phase1_v1"


def default_search_burden_dict(
    *,
    estimated: bool = False,
    scope_version: str = DEFAULT_SEARCH_BURDEN_VERSION,
) -> Dict[str, Any]:
    """Return a complete search-burden dict with all fields defaulted."""
    return {
        "search_proposals_attempted": 0,
        "search_candidates_generated": 0,
        "search_candidates_scored": 0,
        "search_candidates_eligible": 0,
        "search_parameterizations_attempted": 0,
        "search_mutations_attempted": 0,
        "search_directions_tested": 0,
        "search_confirmations_attempted": 0,
        "search_trigger_variants_attempted": 0,
        "search_family_count": 0,
        "search_lineage_count": 0,
        "search_scope_version": scope_version,
        "search_burden_estimated": estimated,
    }


def normalize_search_burden_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a DataFrame to include all canonical search-burden fields.
    
    Missing numeric fields are filled with 0.
    Missing string/bool fields are filled with defaults.
    Existing values are preserved.
    """
    if df.empty:
        out = df.copy()
        for col in SEARCH_BURDEN_FIELDS:
            if col not in out.columns:
                if col in SEARCH_BURDEN_NUMERIC_FIELDS:
                    out[col] = 0
                elif col == "search_scope_version":
                    out[col] = DEFAULT_SEARCH_BURDEN_VERSION
                elif col == "search_burden_estimated":
                    out[col] = False
        return out
    
    out = df.copy()
    
    for col in SEARCH_BURDEN_NUMERIC_FIELDS:
        if col not in out.columns:
            out[col] = 0
        else:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
    
    if "search_scope_version" not in out.columns:
        out["search_scope_version"] = DEFAULT_SEARCH_BURDEN_VERSION
    
    if "search_burden_estimated" not in out.columns:
        out["search_burden_estimated"] = False
    else:
        out["search_burden_estimated"] = out["search_burden_estimated"].fillna(False).astype(bool)
    
    return out


def merge_search_burden_columns(
    df: pd.DataFrame,
    defaults: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Merge search-burden columns into a DataFrame.
    
    If defaults are provided, missing fields are filled from defaults.
    Otherwise, canonical defaults are used.
    
    This is safe to call multiple times (idempotent).
    """
    if defaults is None:
        defaults = default_search_burden_dict()
    
    if df.empty:
        out = df.copy()
        for col, val in defaults.items():
            if col not in out.columns:
                out[col] = val
        return out
    
    out = df.copy()
    
    for col, val in defaults.items():
        if col not in out.columns:
            out[col] = val
        elif col in SEARCH_BURDEN_NUMERIC_FIELDS:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
        elif col == "search_burden_estimated":
            out[col] = out[col].fillna(False).astype(bool)
    
    return out


def build_search_burden_summary(
    *,
    proposals_attempted: int = 0,
    candidates_generated: int = 0,
    candidates_scored: int = 0,
    candidates_eligible: int = 0,
    parameterizations_attempted: int = 0,
    mutations_attempted: int = 0,
    directions_tested: int = 0,
    confirmations_attempted: int = 0,
    trigger_variants_attempted: int = 0,
    family_count: int = 0,
    lineage_count: int = 0,
    estimated: bool = False,
    scope_version: str = DEFAULT_SEARCH_BURDEN_VERSION,
    crowded_families: Optional[List[str]] = None,
    crowded_lineages: Optional[List[str]] = None,
    repeated_failure_lineages: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build a complete search-burden summary dict for artifact emission.
    
    Args:
        proposals_attempted: Number of search proposals attempted
        candidates_generated: Number of candidates generated
        candidates_scored: Number of candidates scored
        candidates_eligible: Number of candidates eligible
        parameterizations_attempted: Number of parameterizations attempted
        mutations_attempted: Number of mutations attempted
        directions_tested: Number of directions tested
        confirmations_attempted: Number of confirmations attempted
        trigger_variants_attempted: Number of trigger variants attempted
        family_count: Number of unique families explored
        lineage_count: Number of unique lineages explored
        estimated: Whether counts are estimated vs exact
        scope_version: Version string for this contract
        crowded_families: Families with many candidates
        crowded_lineages: Lineages with many candidates
        repeated_failure_lineages: Lineages that repeatedly failed
    
    Returns:
        Dict with canonical search-burden fields plus optional diagnostics
    """
    summary = {
        "search_proposals_attempted": proposals_attempted,
        "search_candidates_generated": candidates_generated,
        "search_candidates_scored": candidates_scored,
        "search_candidates_eligible": candidates_eligible,
        "search_parameterizations_attempted": parameterizations_attempted,
        "search_mutations_attempted": mutations_attempted,
        "search_directions_tested": directions_tested,
        "search_confirmations_attempted": confirmations_attempted,
        "search_trigger_variants_attempted": trigger_variants_attempted,
        "search_family_count": family_count,
        "search_lineage_count": lineage_count,
        "search_scope_version": scope_version,
        "search_burden_estimated": estimated,
    }
    
    if crowded_families is not None:
        summary["crowded_families"] = crowded_families
    if crowded_lineages is not None:
        summary["crowded_lineages"] = crowded_lineages
    if repeated_failure_lineages is not None:
        summary["repeated_failure_lineages"] = repeated_failure_lineages
    
    return summary


def write_search_burden_summary(
    summary: Dict[str, Any],
    out_dir,
) -> Dict[str, str]:
    """
    Write search-burden summary as JSON and Markdown artifacts.
    
    Args:
        summary: Search-burden summary dict
        out_dir: Output directory path
    
    Returns:
        Dict with paths to written artifacts
    """
    import json
    from pathlib import Path
    
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    json_path = out_path / "search_burden_summary.json"
    md_path = out_path / "search_burden_summary.md"
    
    json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    
    md_lines = [
        "# Search Burden Summary",
        "",
        "## Totals",
        f"- search_proposals_attempted: `{summary.get('search_proposals_attempted', 0)}`",
        f"- search_candidates_generated: `{summary.get('search_candidates_generated', 0)}`",
        f"- search_candidates_scored: `{summary.get('search_candidates_scored', 0)}`",
        f"- search_candidates_eligible: `{summary.get('search_candidates_eligible', 0)}`",
        f"- search_parameterizations_attempted: `{summary.get('search_parameterizations_attempted', 0)}`",
        f"- search_mutations_attempted: `{summary.get('search_mutations_attempted', 0)}`",
        f"- search_directions_tested: `{summary.get('search_directions_tested', 0)}`",
        f"- search_confirmations_attempted: `{summary.get('search_confirmations_attempted', 0)}`",
        f"- search_trigger_variants_attempted: `{summary.get('search_trigger_variants_attempted', 0)}`",
        "",
        "## Scope",
        f"- search_family_count: `{summary.get('search_family_count', 0)}`",
        f"- search_lineage_count: `{summary.get('search_lineage_count', 0)}`",
        f"- search_scope_version: `{summary.get('search_scope_version', 'unknown')}`",
        f"- search_burden_estimated: `{summary.get('search_burden_estimated', False)}`",
    ]
    
    crowded_families = summary.get("crowded_families")
    if crowded_families:
        md_lines.extend(["", "## Crowded Families"])
        for fam in crowded_families:
            md_lines.append(f"- `{fam}`")
    
    crowded_lineages = summary.get("crowded_lineages")
    if crowded_lineages:
        md_lines.extend(["", "## Crowded Lineages"])
        for lin in crowded_lineages:
            md_lines.append(f"- `{lin}`")
    
    repeated_failures = summary.get("repeated_failure_lineages")
    if repeated_failures:
        md_lines.extend(["", "## Repeated-Failure Lineages"])
        for lin in repeated_failures:
            md_lines.append(f"- `{lin}`")
    
    md_lines.append("")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
    }


def load_search_burden_summary(out_dir) -> Optional[Dict[str, Any]]:
    """
    Load search-burden summary from output directory.
    
    Returns None if not found.
    """
    import json
    from pathlib import Path
    
    out_path = Path(out_dir)
    json_path = out_path / "search_burden_summary.json"
    
    if not json_path.exists():
        return None
    
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return None


__all__ = [
    "SEARCH_BURDEN_FIELDS",
    "SEARCH_BURDEN_NUMERIC_FIELDS",
    "DEFAULT_SEARCH_BURDEN_VERSION",
    "default_search_burden_dict",
    "normalize_search_burden_frame",
    "merge_search_burden_columns",
    "build_search_burden_summary",
    "write_search_burden_summary",
    "load_search_burden_summary",
]
