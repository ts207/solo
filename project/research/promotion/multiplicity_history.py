"""
Historical scope loader for cross-campaign multiplicity.

Loads prior tested candidates from promotion artifacts to construct
the full scope pool for multiplicity accounting.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from project.io.utils import read_parquet

_LOG = logging.getLogger(__name__)


def _deduplicate_historical_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate historical candidates by strongest available key."""
    if df.empty:
        return df

    # Priority: candidate_id > run_id + hypothesis_id
    if "candidate_id" in df.columns and df["candidate_id"].notna().any():
        # Keep first occurrence by candidate_id
        return df.drop_duplicates(subset=["candidate_id"], keep="first")

    if "run_id" in df.columns and "hypothesis_id" in df.columns:
        return df.drop_duplicates(subset=["run_id", "hypothesis_id"], keep="first")

    # Fallback: keep all (no good key)
    return df


def _load_promotion_audit_artifacts(
    data_root: Path,
    program_id: str,
    campaign_id: str | None,
    current_run_id: str,
) -> pd.DataFrame:
    """Load candidates from prior promotion audit artifacts."""
    rows: list[dict] = []

    reports_dir = data_root / "reports" / "promotions"
    if not reports_dir.exists():
        return pd.DataFrame()

    # Find all promotion audit subdirectories
    for run_dir in reports_dir.iterdir():
        if not run_dir.is_dir():
            continue
        run_id = run_dir.name
        if run_id == current_run_id:
            continue  # Exclude current run

        audit_path = run_dir / "promotion_audit.parquet"
        if not audit_path.exists():
            audit_path = run_dir / "promotion_audit.json"

        if not audit_path.exists():
            continue

        try:
            if audit_path.suffix == ".parquet":
                df = read_parquet(audit_path)
            else:
                df = pd.read_json(audit_path, orient="records")

            # Filter by program_id if column exists
            if "program_id" in df.columns:
                df = df[df["program_id"] == program_id]

            if campaign_id and "campaign_id" in df.columns:
                df = df[df["campaign_id"] == campaign_id]

            if not df.empty:
                df["_source_artifact"] = str(audit_path)
                rows.extend(df.to_dict(orient="records"))
        except Exception as exc:
            _LOG.warning("Failed to load historical audit %s: %s", audit_path, exc)

    if not rows:
        return pd.DataFrame()

    return _deduplicate_historical_candidates(pd.DataFrame(rows))


def _load_promoted_candidates_artifacts(
    data_root: Path,
    program_id: str,
    campaign_id: str | None,
    current_run_id: str,
) -> pd.DataFrame:
    """Load candidates from prior promoted_candidates artifacts."""
    rows: list[dict] = []

    reports_dir = data_root / "reports" / "promotions"
    if not reports_dir.exists():
        return pd.DataFrame()

    for run_dir in reports_dir.iterdir():
        if not run_dir.is_dir():
            continue
        run_id = run_dir.name
        if run_id == current_run_id:
            continue

        promoted_path = run_dir / "promoted_candidates.parquet"
        if not promoted_path.exists():
            promoted_path = run_dir / "promoted_candidates.json"

        if not promoted_path.exists():
            continue

        try:
            if promoted_path.suffix == ".parquet":
                df = read_parquet(promoted_path)
            else:
                df = pd.read_json(promoted_path, orient="records")

            if "program_id" in df.columns:
                df = df[df["program_id"] == program_id]

            if campaign_id and "campaign_id" in df.columns:
                df = df[df["campaign_id"] == campaign_id]

            if not df.empty:
                df["_source_artifact"] = str(promoted_path)
                rows.extend(df.to_dict(orient="records"))
        except Exception as exc:
            _LOG.warning("Failed to load historical promoted %s: %s", promoted_path, exc)

    if not rows:
        return pd.DataFrame()

    return _deduplicate_historical_candidates(pd.DataFrame(rows))


def _load_evidence_bundle_summaries(
    data_root: Path,
    program_id: str,
    campaign_id: str | None,
    current_run_id: str,
) -> pd.DataFrame:
    """Load candidates from prior evidence bundle summaries."""
    rows: list[dict] = []

    reports_dir = data_root / "reports" / "promotions"
    if not reports_dir.exists():
        return pd.DataFrame()

    for run_dir in reports_dir.iterdir():
        if not run_dir.is_dir():
            continue
        run_id = run_dir.name
        if run_id == current_run_id:
            continue

        summary_path = run_dir / "evidence_bundle_summary.parquet"
        if not summary_path.exists():
            summary_path = run_dir / "evidence_bundle_summary.json"

        if not summary_path.exists():
            continue

        try:
            if summary_path.suffix == ".parquet":
                df = read_parquet(summary_path)
            else:
                df = pd.read_json(summary_path, orient="records")

            if "program_id" in df.columns:
                df = df[df["program_id"] == program_id]

            if campaign_id and "campaign_id" in df.columns:
                df = df[df["campaign_id"] == campaign_id]

            if not df.empty:
                df["_source_artifact"] = str(summary_path)
                rows.extend(df.to_dict(orient="records"))
        except Exception as exc:
            _LOG.warning("Failed to load historical bundle %s: %s", summary_path, exc)

    if not rows:
        return pd.DataFrame()

    return _deduplicate_historical_candidates(pd.DataFrame(rows))


def _normalize_historical_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize historical frame to canonical columns."""
    if df.empty:
        return df

    # Ensure required columns exist
    required = [
        "candidate_id", "hypothesis_id", "run_id", "program_id",
        "campaign_id", "concept_lineage_key", "family_id", "side_policy",
        "multiplicity_pool_eligible", "q_value", "p_value_for_fdr",
    ]

    for col in required:
        if col not in df.columns:
            df[col] = None

    # Normalize column types
    if "multiplicity_pool_eligible" in df.columns:
        df["multiplicity_pool_eligible"] = df["multiplicity_pool_eligible"].fillna(True)

    if "side_policy" in df.columns:
        df["side_policy"] = df["side_policy"].fillna("directional")

    return df


def load_historical_scope_candidates(
    *,
    data_root: Path,
    program_id: str,
    campaign_id: str | None,
    scope_mode: str,
    current_run_id: str,
) -> pd.DataFrame:
    """
    Load historical tested candidates for scope multiplicity accounting.
    
    Searches prior artifacts in priority order:
    1. promotion audit artifacts
    2. promoted candidate artifacts
    3. evidence bundle summaries
    
    Args:
        data_root: Data root path
        program_id: Program ID to filter by
        campaign_id: Optional campaign ID to filter by
        scope_mode: Scope mode (affects filtering logic)
        current_run_id: Current run ID to exclude
    
    Returns:
        DataFrame with historical candidates, deduplicated
    """
    all_rows: list[dict] = []
    sources_tried = 0
    sources_succeeded = 0

    # Priority 1: promotion audit artifacts
    try:
        df = _load_promotion_audit_artifacts(
            data_root, program_id, campaign_id, current_run_id
        )
        sources_tried += 1
        if not df.empty:
            all_rows.extend(df.to_dict(orient="records"))
            sources_succeeded += 1
            _LOG.info(
                "Loaded %d historical candidates from promotion audits",
                len(df)
            )
    except Exception as exc:
        _LOG.warning("Failed to load promotion audit artifacts: %s", exc)

    # Priority 2: promoted candidates
    try:
        df = _load_promoted_candidates_artifacts(
            data_root, program_id, campaign_id, current_run_id
        )
        sources_tried += 1
        if not df.empty:
            all_rows.extend(df.to_dict(orient="records"))
            sources_succeeded += 1
            _LOG.info(
                "Loaded %d historical candidates from promoted artifacts",
                len(df)
            )
    except Exception as exc:
        _LOG.warning("Failed to load promoted candidates artifacts: %s", exc)

    # Priority 3: evidence bundle summaries
    try:
        df = _load_evidence_bundle_summaries(
            data_root, program_id, campaign_id, current_run_id
        )
        sources_tried += 1
        if not df.empty:
            all_rows.extend(df.to_dict(orient="records"))
            sources_succeeded += 1
            _LOG.info(
                "Loaded %d historical candidates from bundle summaries",
                len(df)
            )
    except Exception as exc:
        _LOG.warning("Failed to load evidence bundle summaries: %s", exc)

    if not all_rows:
        _LOG.info(
            "No historical scope candidates found for program=%s campaign=%s",
            program_id, campaign_id
        )
        return pd.DataFrame()

    combined = _deduplicate_historical_candidates(pd.DataFrame(all_rows))
    normalized = _normalize_historical_frame(combined)

    _LOG.info(
        "Historical scope pool: %d unique candidates from %d/%d sources",
        len(normalized), sources_succeeded, sources_tried
    )

    return normalized


__all__ = ["load_historical_scope_candidates"]
