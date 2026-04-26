from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from project.contracts.schemas import validate_dataframe_for_schema
from project.io.utils import ensure_dir, write_parquet


@dataclass
class ReportBundleResult:
    written_paths: dict[str, Path] = field(default_factory=dict)
    written_frames: dict[str, pd.DataFrame] = field(default_factory=dict)


_PROMOTION_SCHEMA_DEFAULTS: dict[str, dict[str, Any]] = {
    "evidence_bundle_summary": {
        "policy_version": "",
        "bundle_version": "",
        "is_reduced_evidence": False,
    },
    "promotion_decisions": {
        "policy_version": "",
        "bundle_version": "",
        "is_reduced_evidence": False,
    },
}


def _apply_schema_defaults(frame: pd.DataFrame, schema_name: str | None) -> pd.DataFrame:
    out = frame.copy()
    defaults = _PROMOTION_SCHEMA_DEFAULTS.get(str(schema_name or ""))
    if not defaults:
        return out
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
        elif not out.empty:
            out[column] = out[column].where(~out[column].isna(), default)
    return out


def write_dataframe_report(
    df: pd.DataFrame,
    output_path: Path,
    *,
    schema_name: str | None = None,
    allow_empty: bool = True,
) -> tuple[pd.DataFrame, Path]:
    frame = _apply_schema_defaults(df, schema_name)
    if schema_name is not None:
        frame = validate_dataframe_for_schema(frame, schema_name, allow_empty=allow_empty)
    ensure_dir(output_path.parent)
    actual_path, _storage = write_parquet(frame, output_path)
    return frame, Path(actual_path)


def write_json_report(payload: Mapping[str, Any], output_path: Path) -> Path:
    ensure_dir(output_path.parent)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_candidate_reports(
    *,
    out_dir: Path,
    combined_candidates: pd.DataFrame,
    symbol_candidates: Mapping[str, pd.DataFrame],
    diagnostics: Mapping[str, Any] | None = None,
) -> ReportBundleResult:
    result = ReportBundleResult()
    combined_out = out_dir / "phase2_candidates.parquet"
    combined_frame, combined_actual = write_dataframe_report(
        combined_candidates,
        combined_out,
        schema_name="phase2_candidates",
        allow_empty=True,
    )
    result.written_frames["combined_candidates"] = combined_frame
    result.written_paths["combined_candidates"] = combined_actual
    for symbol, frame in symbol_candidates.items():
        sym_out = out_dir / "symbols" / str(symbol).upper() / "phase2_candidates.parquet"
        sym_frame, sym_actual = write_dataframe_report(
            frame,
            sym_out,
            schema_name="phase2_candidates",
            allow_empty=True,
        )
        result.written_frames[f"symbol::{symbol}"] = sym_frame
        result.written_paths[f"symbol::{symbol}"] = sym_actual
    if diagnostics is not None:
        diag_out = out_dir / "phase2_diagnostics.json"
        result.written_paths["diagnostics"] = write_json_report(diagnostics, diag_out)
    return result


def write_promotion_reports(
    *,
    out_dir: Path,
    audit_df: pd.DataFrame,
    promoted_df: pd.DataFrame,
    evidence_bundle_summary: pd.DataFrame,
    promotion_decisions: pd.DataFrame,
    diagnostics: Mapping[str, Any],
    promotion_summary: pd.DataFrame,
) -> ReportBundleResult:
    result = ReportBundleResult()
    outputs = {
        "promotion_audit": (
            audit_df,
            out_dir / "promotion_audit.parquet",
            "promotion_audit",
        ),
        "promoted_candidates": (
            promoted_df,
            out_dir / "promoted_candidates.parquet",
            "promoted_candidates",
        ),
        "evidence_bundle_summary": (
            evidence_bundle_summary,
            out_dir / "evidence_bundle_summary.parquet",
            "evidence_bundle_summary",
        ),
        "promotion_decisions": (
            promotion_decisions,
            out_dir / "promotion_decisions.parquet",
            "promotion_decisions",
        ),
    }
    for key, (frame, path, schema) in outputs.items():
        written_frame, actual_path = write_dataframe_report(
            frame, path, schema_name=schema, allow_empty=True
        )
        result.written_frames[key] = written_frame
        result.written_paths[key] = actual_path
    ensure_dir(out_dir)
    promotion_summary.to_csv(out_dir / "promotion_summary.csv", index=False)
    result.written_paths["promotion_summary"] = out_dir / "promotion_summary.csv"
    result.written_paths["promotion_diagnostics"] = write_json_report(
        diagnostics, out_dir / "promotion_diagnostics.json"
    )
    return result


def append_phase2_funnel_index(payload: Mapping[str, Any], *, data_root: Path) -> Path:
    from project.io.funnel import append_funnel_index

    return append_funnel_index(payload, data_root=data_root)
