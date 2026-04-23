from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import atomic_write_json, atomic_write_text, read_table_auto
from project.operator.stability import write_sprint4_outputs_for_run
from project.research.audit_historical_artifacts import build_run_historical_trust_summary
from project.research.knowledge.memory import read_memory_table, write_memory_table
from project.research.knowledge.reflection import build_run_reflection
from project.research.knowledge.schemas import canonical_json
from project.specs.manifest import load_run_manifest


@dataclass(frozen=True)
class OperatorSummaryPaths:
    root: Path
    json_path: Path
    markdown_path: Path


def operator_summary_paths(run_id: str, *, data_root: Path | None = None) -> OperatorSummaryPaths:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    root = resolved / "reports" / "operator" / str(run_id)
    return OperatorSummaryPaths(
        root=root,
        json_path=root / "operator_summary.json",
        markdown_path=root / "operator_summary.md",
    )


def _read_table(path: Path) -> pd.DataFrame:
    frame = read_table_auto(path)
    if isinstance(frame, pd.DataFrame) and not frame.empty:
        return frame
    if isinstance(frame, pd.DataFrame) and path.exists():
        return frame
    alt = path.with_suffix(".csv") if path.suffix.lower() == ".parquet" else path.with_suffix(".parquet")
    if alt.exists():
        alt_frame = read_table_auto(alt)
        if isinstance(alt_frame, pd.DataFrame):
            return alt_frame
    return pd.DataFrame()


def _safe_int(value: Any) -> int:
    try:
        if pd.isna(value):
            return 0
    except TypeError:
        pass
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _first_present(row: pd.Series, columns: list[str]) -> Any:
    for col in columns:
        if col in row.index:
            value = row.get(col)
            try:
                if pd.isna(value):
                    continue
            except TypeError:
                pass
            if value not in (None, ""):
                return value
    return None


def _candidate_template_label(row: pd.Series) -> str:
    return str(
        _first_present(
            row,
            ["template_id", "rule_template", "template_verb", "template", "template_family"],
        )
        or "unknown_template"
    )


def _normalize_csv_scope(raw: Any) -> list[str]:
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        return [str(v).strip() for v in raw if str(v).strip()]
    text = str(raw)
    return [part.strip() for part in text.split(",") if part.strip()]


def _load_proposal_row(program_id: str, run_id: str, *, data_root: Path) -> dict[str, Any]:
    proposals = read_memory_table(program_id, "proposals", data_root=data_root)
    if proposals.empty or "run_id" not in proposals.columns:
        return {}
    matches = proposals.loc[proposals["run_id"].astype(str) == str(run_id)]
    if matches.empty:
        return {}
    row = matches.iloc[-1]
    return {str(col): row[col] for col in proposals.columns}


def _load_candidate_tables(run_id: str, *, data_root: Path) -> dict[str, pd.DataFrame]:
    return {
        "promotion_audit": _read_table(data_root / "reports" / "promotions" / run_id / "promotion_statistical_audit.parquet"),
        "edge_candidates": _read_table(data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"),
        "phase2_candidates": _read_table(data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"),
    }


def _choose_best_candidate(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    for source in ("promotion_audit", "edge_candidates", "phase2_candidates"):
        df = tables[source]
        if df.empty:
            continue
        work = df.copy()
        score_col = None
        for candidate in ("t_stat", "t_value", "abs_t_stat", "robustness_score", "q_value"):
            if candidate in work.columns:
                score_col = candidate
                break
        if score_col is None:
            row = work.iloc[0]
            score = None
        else:
            numeric = pd.to_numeric(work[score_col], errors="coerce")
            if score_col == "q_value":
                best_idx = numeric.fillna(float("inf")).idxmin()
            elif score_col in {"t_stat", "t_value", "abs_t_stat"}:
                best_idx = numeric.abs().fillna(float("-inf")).idxmax()
            else:
                best_idx = numeric.fillna(float("-inf")).idxmax()
            row = work.loc[best_idx]
            score = _safe_float(row.get(score_col))
        label = " / ".join(
            [
                str(_first_present(row, ["event_type", "trigger_type"]) or "unknown_event"),
                _candidate_template_label(row),
                str(_first_present(row, ["direction"]) or "?"),
                str(_first_present(row, ["horizon", "horizon_bars", "horizon_label"]) or "?"),
            ]
        )
        return {
            "source": source,
            "label": label,
            "metric_name": score_col or "row_order",
            "metric_value": score,
            "primary_fail_gate": _first_present(row, ["promotion_fail_gate_primary", "primary_fail_gate"]),
            "row": {str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()},
        }
    return {
        "source": "none",
        "label": "",
        "metric_name": "",
        "metric_value": None,
        "primary_fail_gate": "",
        "row": {},
    }


def _verdict(summary: dict[str, Any]) -> str:
    terminal = str(summary.get("terminal_status", "") or "")
    statistical = str(summary.get("statistical_outcome", "") or "")
    promoted = _safe_int(summary.get("promoted_count", 0))
    if terminal == "completed" and promoted > 0:
        return "PROMOTE"
    if terminal in {"failed_mechanical", "failed_data_quality", "failed_runtime_invariants"}:
        return "REPAIR"
    if statistical in {"research_promising", "deploy_promising"}:
        return "CONFIRM"
    return "KEEP_RESEARCH"


def _next_action(summary: dict[str, Any]) -> str:
    reflection_next = str(summary.get("recommended_next_action", "") or "").strip()
    if reflection_next:
        return reflection_next
    verdict = _verdict(summary)
    if verdict == "REPAIR":
        return "repair_infrastructure"
    if verdict == "PROMOTE":
        return "prepare_promotion_review"
    if verdict == "CONFIRM":
        return "run_bounded_confirmation"
    return "test_adjacent_bounded_variant"


def build_operator_summary(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    manifest_local = resolved / "runs" / run_id / "run_manifest.json"
    if manifest_local.exists():
        manifest = json.loads(manifest_local.read_text(encoding="utf-8"))
    else:
        manifest = load_run_manifest(run_id)
    manifest = manifest if isinstance(manifest, dict) else {}
    inferred_program_id = str(program_id or manifest.get("program_id", "") or "").strip()
    reflection = build_run_reflection(run_id=run_id, data_root=resolved)
    proposal_row = _load_proposal_row(inferred_program_id, run_id, data_root=resolved) if inferred_program_id else {}
    tables = _load_candidate_tables(run_id, data_root=resolved)
    best = _choose_best_candidate(tables)

    bounded = {}
    try:
        bounded = json.loads(str(proposal_row.get("bounded_json", "") or "{}"))
        if not isinstance(bounded, dict):
            bounded = {}
    except json.JSONDecodeError:
        bounded = {}

    symbol_scope = _normalize_csv_scope(proposal_row.get("symbols", manifest.get("symbols", "")))
    event_scope = []
    proposal_path = str(proposal_row.get("proposal_path", "") or "")
    if proposal_path:
        try:
            from project.research.agent_io.proposal_schema import load_operator_proposal
            proposal = load_operator_proposal(proposal_path)
            trigger_space = proposal.trigger_space or {}
            events = trigger_space.get("events", {}) if isinstance(trigger_space, dict) else {}
            event_scope = sorted(str(k).strip() for k in events if str(k).strip())
            template_scope = list(proposal.templates)
            horizon_scope = [str(v) for v in proposal.horizons_bars]
            entry_lag_scope = [str(v) for v in proposal.entry_lags]
            date_range = f"{proposal.start} to {proposal.end}"
        except Exception:
            template_scope = []
            horizon_scope = []
            entry_lag_scope = []
            date_range = ""
    else:
        template_scope = []
        horizon_scope = []
        entry_lag_scope = []
        date_range = ""

    summary = {
        "schema_version": "operator_summary_v1",
        "run_id": run_id,
        "program_id": inferred_program_id,
        "proposal_id": str(proposal_row.get("proposal_id", "") or ""),
        "baseline_run_id": str(proposal_row.get("baseline_run_id", "") or bounded.get("baseline_run_id", "") or ""),
        "experiment_type": str(proposal_row.get("experiment_type", "") or ("discovery" if not bounded else bounded.get("experiment_type", "confirmation"))),
        "changed_field": str(
            proposal_row.get("allowed_change_field", "")
            or ((bounded.get("changed_fields", [""]) or [""])[0] if bounded else "")
        ),
        "campaign_id": str(proposal_row.get("campaign_id", "") or ""),
        "cycle_number": _safe_int(proposal_row.get("cycle_number", 0)),
        "branch_id": str(proposal_row.get("branch_id", "") or ""),
        "parent_run_id": str(proposal_row.get("parent_run_id", "") or ""),
        "mutation_type": str(proposal_row.get("mutation_type", "") or ""),
        "branch_depth": _safe_int(proposal_row.get("branch_depth", 0)),
        "decision": str(proposal_row.get("decision", "") or ""),
        "frozen_fields": list(bounded.get("frozen_fields", [])) if isinstance(bounded.get("frozen_fields", []), list) else [],
        "run_status": str(manifest.get("status", "") or ""),
        "terminal_status": str(manifest.get("terminal_status", "") or ""),
        "mechanical_outcome": str(manifest.get("mechanical_outcome", reflection.get("mechanical_outcome", "")) or ""),
        "statistical_outcome": str(manifest.get("statistical_outcome", reflection.get("statistical_outcome", "")) or ""),
        "candidate_count": _safe_int(reflection.get("candidate_count", 0)),
        "promoted_count": _safe_int(reflection.get("promoted_count", 0)),
        "primary_fail_gate": str(reflection.get("primary_fail_gate", best.get("primary_fail_gate", "")) or ""),
        "date_range": date_range,
        "symbol_scope": symbol_scope,
        "event_scope": event_scope,
        "template_scope": template_scope,
        "horizon_scope": horizon_scope,
        "entry_lag_scope": entry_lag_scope,
        "top_candidate": {
            "label": str(best.get("label", "") or ""),
            "source": str(best.get("source", "") or ""),
            "metric_name": str(best.get("metric_name", "") or ""),
            "metric_value": best.get("metric_value"),
            "primary_fail_gate": str(best.get("primary_fail_gate", "") or ""),
        },
        "recommended_next_action": str(reflection.get("recommended_next_action", "") or ""),
        "recommended_next_experiment": str(reflection.get("recommended_next_experiment", "") or ""),
        "market_findings": str(reflection.get("market_findings", "") or ""),
        "system_findings": str(reflection.get("system_findings", "") or ""),
        "historical_trust": build_run_historical_trust_summary(
            run_id=run_id,
            data_root=resolved,
        ),
    }
    summary["verdict"] = _verdict(summary)
    if not summary["recommended_next_action"]:
        summary["recommended_next_action"] = _next_action(summary)
    return summary


def _summary_markdown(summary: dict[str, Any]) -> str:
    top = summary.get("top_candidate", {}) or {}
    trust = summary.get("historical_trust", {}) or {}
    lines = [
        f"# Operator summary — {summary.get('run_id', '')}",
        "",
        f"- Program: `{summary.get('program_id', '')}`",
        f"- Experiment type: `{summary.get('experiment_type', '')}`",
        f"- Baseline run: `{summary.get('baseline_run_id', '')}`",
        f"- Changed field: `{summary.get('changed_field', '')}`",
        f"- Run status: `{summary.get('run_status', '')}`",
        f"- Terminal status: `{summary.get('terminal_status', '')}`",
        f"- Mechanical outcome: `{summary.get('mechanical_outcome', '')}`",
        f"- Statistical outcome: `{summary.get('statistical_outcome', '')}`",
        f"- Candidate count: `{summary.get('candidate_count', 0)}`",
        f"- Promoted count: `{summary.get('promoted_count', 0)}`",
        f"- Date range: `{summary.get('date_range', '')}`",
        f"- Symbols: `{', '.join(summary.get('symbol_scope', []))}`",
        f"- Events: `{', '.join(summary.get('event_scope', []))}`",
        f"- Templates: `{', '.join(summary.get('template_scope', []))}`",
        f"- Horizons: `{', '.join(summary.get('horizon_scope', []))}`",
        f"- Entry lags: `{', '.join(summary.get('entry_lag_scope', []))}`",
        "",
        "## Best candidate or strongest near-miss",
        "",
        f"- Label: `{top.get('label', '')}`",
        f"- Source: `{top.get('source', '')}`",
        f"- Metric: `{top.get('metric_name', '')}` = `{top.get('metric_value')}`",
        f"- Primary fail gate: `{summary.get('primary_fail_gate', top.get('primary_fail_gate', ''))}`",
        "",
        "## Historical trust",
        "",
        f"- Status: `{trust.get('historical_trust_status', '')}`",
        f"- Reason: `{trust.get('historical_trust_reason', '')}`",
        f"- Canonical reuse allowed: `{trust.get('canonical_reuse_allowed', False)}`",
        f"- Compat reuse allowed: `{trust.get('compat_reuse_allowed', False)}`",
        "",
        "## Decision",
        "",
        f"- Verdict: `{summary.get('verdict', '')}`",
        f"- Next action: `{summary.get('recommended_next_action', '')}`",
        f"- Next experiment: `{summary.get('recommended_next_experiment', '')}`",
    ]
    return "\n".join(lines) + "\n"


def write_operator_summary(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    summary = build_operator_summary(run_id=run_id, program_id=program_id, data_root=resolved)
    paths = operator_summary_paths(run_id, data_root=resolved)
    paths.root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(paths.json_path, summary)
    atomic_write_text(paths.markdown_path, _summary_markdown(summary))
    summary["summary_json_path"] = str(paths.json_path)
    summary["summary_markdown_path"] = str(paths.markdown_path)
    return summary


def update_evidence_ledger(*, run_id: str, program_id: str, data_root: Path | None = None, summary: dict[str, Any] | None = None) -> Path:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    payload = dict(summary) if summary is not None else write_operator_summary(run_id=run_id, program_id=program_id, data_root=resolved)
    existing = read_memory_table(program_id, "evidence_ledger", data_root=resolved)
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "ledger_id": f"ledger::{run_id}",
        "program_id": program_id,
        "run_id": run_id,
        "baseline_run_id": payload.get("baseline_run_id", ""),
        "proposal_id": payload.get("proposal_id", ""),
        "experiment_type": payload.get("experiment_type", ""),
        "changed_field": payload.get("changed_field", ""),
        "frozen_fields_json": canonical_json(payload.get("frozen_fields", [])),
        "date_range": payload.get("date_range", ""),
        "symbol_scope": ",".join(payload.get("symbol_scope", [])),
        "event_scope": ",".join(payload.get("event_scope", [])),
        "template_scope": ",".join(payload.get("template_scope", [])),
        "horizon_scope": ",".join(payload.get("horizon_scope", [])),
        "entry_lag_scope": ",".join(payload.get("entry_lag_scope", [])),
        "terminal_status": payload.get("terminal_status", ""),
        "run_status": payload.get("run_status", ""),
        "mechanical_outcome": payload.get("mechanical_outcome", ""),
        "statistical_outcome": payload.get("statistical_outcome", ""),
        "candidate_count": _safe_int(payload.get("candidate_count", 0)),
        "promoted_count": _safe_int(payload.get("promoted_count", 0)),
        "primary_metric_name": (payload.get("top_candidate", {}) or {}).get("metric_name", ""),
        "primary_metric_value": (payload.get("top_candidate", {}) or {}).get("metric_value", None),
        "top_candidate_label": (payload.get("top_candidate", {}) or {}).get("label", ""),
        "verdict": payload.get("verdict", ""),
        "recommended_next_action": payload.get("recommended_next_action", ""),
        "recommended_next_experiment": payload.get("recommended_next_experiment", ""),
        "negative_diagnosis": ((payload.get("negative_result_diagnostics", {}) or {}).get("diagnosis", "")),
        "regime_classification": ((payload.get("regime_split_report", {}) or {}).get("classification", "")),
        "summary_path": payload.get("summary_markdown_path", ""),
        "campaign_id": payload.get("campaign_id", ""),
        "cycle_number": _safe_int(payload.get("cycle_number", 0)),
        "branch_id": payload.get("branch_id", ""),
        "parent_run_id": payload.get("parent_run_id", ""),
        "mutation_type": payload.get("mutation_type", ""),
        "branch_depth": _safe_int(payload.get("branch_depth", 0)),
        "decision": payload.get("decision", ""),
        "created_at": now,
        "updated_at": now,
    }
    incoming = pd.DataFrame([row])
    if existing.empty:
        merged = incoming
    else:
        merged = pd.concat([existing, incoming], ignore_index=True)
        merged = merged.drop_duplicates(subset=["ledger_id"], keep="last").reset_index(drop=True)
        if "created_at" in merged.columns:
            created = merged["created_at"].replace("", pd.NA)
            merged["created_at"] = created.groupby(merged["ledger_id"]).transform("first").fillna(now)
        merged.loc[merged["ledger_id"] == row["ledger_id"], "updated_at"] = now
    return write_memory_table(program_id, "evidence_ledger", merged, data_root=resolved)


def write_operator_outputs_for_run(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    summary = write_operator_summary(run_id=run_id, program_id=program_id, data_root=resolved)
    sprint4 = write_sprint4_outputs_for_run(run_id=run_id, program_id=program_id, data_root=resolved)
    summary.update(
        {
            "negative_result_diagnostics": sprint4.get("negative_result_diagnostics", {}),
            "regime_split_report": sprint4.get("regime_split_report", {}),
        }
    )
    effective_program_id = str(program_id or summary.get("program_id", "") or "").strip()
    if effective_program_id:
        ledger_path = update_evidence_ledger(
            run_id=run_id,
            program_id=effective_program_id,
            data_root=resolved,
            summary=summary,
        )
        summary["evidence_ledger_path"] = str(ledger_path)
    return summary
