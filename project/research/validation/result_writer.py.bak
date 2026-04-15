from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import write_parquet
from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
    ValidationArtifactRef,
)


def write_validation_bundle(bundle: ValidationBundle, base_dir: Optional[Path] = None) -> Path:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / bundle.run_id
    
    base_dir.mkdir(parents=True, exist_ok=True)
    
    bundle_path = base_dir / "validation_bundle.json"
    with bundle_path.open("w", encoding="utf-8") as f:
        json.dump(bundle.to_dict(), f, indent=2)
    
    # Canonical: validation_report.json
    summary_path = base_dir / "validation_report.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(bundle.summary_stats, f, indent=2)
        
    # Canonical: effect_stability_report.json
    stability_path = base_dir / "effect_stability_report.json"
    with stability_path.open("w", encoding="utf-8") as f:
        json.dump(bundle.effect_stability_report, f, indent=2)

    return bundle_path


def write_validated_candidate_tables(bundle: ValidationBundle, base_dir: Optional[Path] = None) -> Dict[str, Path]:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / bundle.run_id
    
    base_dir.mkdir(parents=True, exist_ok=True)
    
    paths = {}
    
    # Canonical Groups
    # 1. validated_candidates.parquet
    # 2. rejection_reasons.parquet (rejected + inconclusive)
    
    groups = {
        "validated_candidates": bundle.validated_candidates,
        "rejection_reasons": bundle.rejected_candidates + bundle.inconclusive_candidates,
    }
    
    for name, candidates in groups.items():
        if not candidates:
            # Still write an empty file if it's the canonical name? 
            # Usually better to have the file exist.
            flat_df = pd.DataFrame()
        else:
            flat_data = []
            for c in candidates:
                row = {
                    "candidate_id": c.candidate_id,
                    "anchor_summary": c.anchor_summary,
                    "template_id": c.template_id,
                    "direction": c.direction,
                    "horizon_bars": c.horizon_bars,
                    "validation_stage_version": c.validation_stage_version,
                    "status": c.decision.status,
                    "run_id": c.decision.run_id,
                    "program_id": c.decision.program_id,
                    "reason_codes": "|".join(c.decision.reason_codes),
                }
                # Add metrics
                metrics_dict = c.metrics.to_dict()
                for k, v in metrics_dict.items():
                    row[f"metric_{k}"] = v
                flat_data.append(row)
            flat_df = pd.DataFrame(flat_data)
            
        path = base_dir / f"{name}.parquet"
        write_parquet(flat_df, path)
        paths[name] = path
        
    return paths


def load_validation_bundle(run_id: str, base_dir: Optional[Path] = None) -> Optional[ValidationBundle]:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / run_id
        
    bundle_path = base_dir / "validation_bundle.json"
    if not bundle_path.exists():
        return None
        
    with bundle_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        
    def _parse_candidate(c_data: Dict[str, Any]) -> ValidatedCandidateRecord:
        decision = ValidationDecision(**c_data["decision"])
        metrics = ValidationMetrics(**c_data["metrics"])
        artifacts = [ValidationArtifactRef(**a) for a in c_data.get("artifact_refs", [])]
        
        return ValidatedCandidateRecord(
            candidate_id=c_data["candidate_id"],
            decision=decision,
            metrics=metrics,
            anchor_summary=c_data.get("anchor_summary", ""),
            template_id=c_data.get("template_id", ""),
            direction=c_data.get("direction", ""),
            horizon_bars=c_data.get("horizon_bars", 0),
            artifact_refs=artifacts,
            validation_stage_version=c_data.get("validation_stage_version", "v1"),
        )

    return ValidationBundle(
        run_id=data["run_id"],
        created_at=data["created_at"],
        program_id=data.get("program_id"),
        validated_candidates=[_parse_candidate(c) for c in data.get("validated_candidates", [])],
        rejected_candidates=[_parse_candidate(c) for c in data.get("rejected_candidates", [])],
        inconclusive_candidates=[_parse_candidate(c) for c in data.get("inconclusive_candidates", [])],
        summary_stats=data.get("summary_stats", {}),
    )
