from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from project.io.utils import ensure_dir, write_parquet
from project.research.validation.contracts import ValidationBundle

_TRACE_FILENAMES = {
    "discover": "discovery_decision_trace.parquet",
    "validate": "validation_decision_trace.parquet",
    "promote": "promotion_decision_trace.parquet",
    "merged": "research_decision_trace.parquet",
}


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except Exception:
        return json.dumps(str(value))


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def _stable_id(row: Dict[str, Any]) -> str:
    parts = [
        _text(row.get("event_type") or row.get("canonical_event_type")),
        _text(row.get("symbol")),
        _text(row.get("template_id") or row.get("rule_template")),
        _text(row.get("direction")),
        _text(row.get("horizon") or row.get("horizon_bars")),
        _text(row.get("run_id")),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"trace_{digest}"


def _candidate_id(row: Dict[str, Any]) -> str:
    return _text(row.get("candidate_id")) or _text(row.get("hypothesis_id")) or _stable_id(row)


def _hypothesis_id(row: Dict[str, Any]) -> str:
    return _text(row.get("hypothesis_id")) or _candidate_id(row)


def build_discovery_trace_frame(candidates_df: pd.DataFrame, *, run_id: str) -> pd.DataFrame:
    if candidates_df is None or candidates_df.empty:
        return pd.DataFrame(
            columns=[
                "candidate_id",
                "hypothesis_id",
                "event_type",
                "symbol",
                "run_id",
                "template_id",
                "direction",
                "horizon",
                "estimate_bps",
                "t_stat",
                "robustness",
                "n_obs",
                "canonical_regime",
                "promotion_eligible",
                "discovery_payload_json",
            ]
        )
    rows = []
    for payload in candidates_df.to_dict(orient="records"):
        cid = _candidate_id(payload)
        rows.append(
            {
                "candidate_id": cid,
                "hypothesis_id": _hypothesis_id(payload),
                "event_type": _text(payload.get("canonical_event_type") or payload.get("event_type")),
                "symbol": _text(payload.get("symbol")),
                "run_id": _text(payload.get("run_id")) or run_id,
                "template_id": _text(payload.get("template_id") or payload.get("rule_template")),
                "direction": _text(payload.get("direction")),
                "horizon": _text(payload.get("horizon") or payload.get("horizon_bars")),
                "estimate_bps": payload.get("after_cost_expectancy_per_trade", payload.get("expectancy_bps", payload.get("expectancy"))),
                "t_stat": payload.get("t_stat"),
                "robustness": payload.get("robustness_score", payload.get("stability_score")),
                "n_obs": payload.get("n_obs", payload.get("total_n_obs", payload.get("sample_count"))),
                "canonical_regime": _text(payload.get("canonical_regime")),
                "promotion_eligible": _bool(payload.get("promotion_eligible") or payload.get("compile_eligible") or payload.get("is_discovery")),
                "discovery_payload_json": _json(payload),
            }
        )
    return pd.DataFrame(rows)


def build_validation_trace_frame(bundle: ValidationBundle) -> pd.DataFrame:
    rows = []
    all_candidates = list(bundle.validated_candidates) + list(bundle.rejected_candidates) + list(bundle.inconclusive_candidates)
    for item in all_candidates:
        rows.append(
            {
                "candidate_id": item.candidate_id,
                "run_id": bundle.run_id,
                "validation_status": _text(item.decision.status),
                "validation_passed": item.decision.status == "validated",
                "rejection_reasons_json": _json(item.decision.reason_codes),
                "template_id": _text(item.template_id),
                "direction": _text(item.direction),
                "horizon_bars": item.horizon_bars,
                "q_value": item.metrics.q_value,
                "p_value": item.metrics.p_value,
                "sample_count": item.metrics.sample_count,
                "net_expectancy": item.metrics.net_expectancy,
                "validation_payload_json": _json(item.to_dict()),
            }
        )
    return pd.DataFrame(rows)


def build_promotion_trace_frame(
    audit_df: pd.DataFrame,
    promoted_df: pd.DataFrame,
    *,
    run_id: str,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if audit_df is None:
        audit_df = pd.DataFrame()
    promoted_lookup = set()
    if promoted_df is not None and not promoted_df.empty:
        promoted_lookup = set(_text(v) for v in promoted_df.get("candidate_id", pd.Series(dtype=str)).tolist())
    rows = []
    if audit_df.empty and promoted_df is not None and not promoted_df.empty:
        audit_df = promoted_df.copy()
    for payload in audit_df.to_dict(orient="records"):
        cid = _candidate_id(payload)
        decision = _text(payload.get("promotion_decision"))
        if not decision:
            decision = "promoted" if cid in promoted_lookup else "rejected"
        rows.append(
            {
                "candidate_id": cid,
                "run_id": _text(payload.get("run_id")) or run_id,
                "event_type": _text(payload.get("event_type") or payload.get("canonical_event_type")),
                "symbol": _text(payload.get("symbol")),
                "promotion_decision": decision,
                "promotion_track": _text(payload.get("promotion_track")),
                "policy_version": _text(payload.get("policy_version")),
                "bundle_version": _text(payload.get("bundle_version")),
                "promotion_class": _text(payload.get("promotion_class")),
                "readiness_status": _text(payload.get("readiness_status")),
                "is_reduced_evidence": _bool(payload.get("is_reduced_evidence")),
                "promotion_payload_json": _json(payload),
            }
        )
    frame = pd.DataFrame(rows)
    if diagnostics is not None and not frame.empty:
        frame["promotion_diagnostics_json"] = _json(diagnostics)
    return frame


def _read_parquet_if_exists(path: Path) -> Optional[pd.DataFrame]:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def merge_research_decision_trace(
    *,
    discovery_trace: Optional[pd.DataFrame] = None,
    validation_trace: Optional[pd.DataFrame] = None,
    promotion_trace: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    frames = [f for f in [discovery_trace, validation_trace, promotion_trace] if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    merged = None
    for frame in frames:
        normalized = frame.copy()
        if "candidate_id" not in normalized.columns:
            continue
        if merged is None:
            merged = normalized
            continue
        extra_cols = [c for c in normalized.columns if c != "candidate_id" and c not in merged.columns]
        merged = merged.merge(normalized[["candidate_id", *extra_cols]], on="candidate_id", how="outer")
    if merged is None:
        return pd.DataFrame()
    def _final_decision(row: pd.Series) -> str:
        promotion_decision = _text(row.get("promotion_decision"))
        if promotion_decision:
            return promotion_decision
        validation_status = _text(row.get("validation_status"))
        if validation_status:
            if validation_status == "validated":
                return "not_promoted"
            return validation_status
        return "discovered"
    merged["final_decision"] = merged.apply(_final_decision, axis=1)
    return merged


def write_trace_frame(frame: pd.DataFrame, output_path: Path) -> Path:
    ensure_dir(output_path.parent)
    actual_path, _ = write_parquet(frame, output_path)
    return Path(actual_path)


def write_discovery_trace(candidates_df: pd.DataFrame, *, out_dir: Path, run_id: str) -> Dict[str, Any]:
    frame = build_discovery_trace_frame(candidates_df, run_id=run_id)
    path = write_trace_frame(frame, out_dir / _TRACE_FILENAMES["discover"])
    return {"frame": frame, "path": path}


def write_validation_trace(bundle: ValidationBundle, *, out_dir: Path) -> Dict[str, Any]:
    frame = build_validation_trace_frame(bundle)
    path = write_trace_frame(frame, out_dir / _TRACE_FILENAMES["validate"])
    return {"frame": frame, "path": path}


def write_promotion_trace(
    audit_df: pd.DataFrame,
    promoted_df: pd.DataFrame,
    *,
    out_dir: Path,
    run_id: str,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    frame = build_promotion_trace_frame(audit_df, promoted_df, run_id=run_id, diagnostics=diagnostics)
    path = write_trace_frame(frame, out_dir / _TRACE_FILENAMES["promote"])
    return {"frame": frame, "path": path}


def write_merged_research_trace(
    *,
    out_dir: Path,
    data_root: Path,
    run_id: str,
    discovery_trace: Optional[pd.DataFrame] = None,
    validation_trace: Optional[pd.DataFrame] = None,
    promotion_trace: Optional[pd.DataFrame] = None,
) -> Optional[Path]:
    if discovery_trace is None:
        discovery_trace = _read_parquet_if_exists(data_root / "reports" / "phase2" / run_id / _TRACE_FILENAMES["discover"])
    if validation_trace is None:
        validation_trace = _read_parquet_if_exists(data_root / "reports" / "validation" / run_id / _TRACE_FILENAMES["validate"])
    if promotion_trace is None:
        promotion_trace = _read_parquet_if_exists(data_root / "reports" / "promotions" / run_id / _TRACE_FILENAMES["promote"])
    merged = merge_research_decision_trace(
        discovery_trace=discovery_trace,
        validation_trace=validation_trace,
        promotion_trace=promotion_trace,
    )
    if merged.empty:
        return None
    return write_trace_frame(merged, out_dir / _TRACE_FILENAMES["merged"])
