from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index
from project.research.candidate_traces import trace_paths
from project.research.year_split import _safe_name
from project.scripts.discover_doctor import build_discover_doctor_report

SCHEMA_VERSION = "candidate_autopsy_v1"
DEFAULT_FORBIDDEN_RESCUE_ACTIONS = [
    "change_horizon",
    "change_context",
    "loosen_gates",
    "promote_without_specificity_controls",
    "promote_without_forward_confirmation",
]


def _data_root(path: Path | str | None) -> Path:
    return Path(path or "data")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _round(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(float(value), digits)


def _phase2_candidate(data_root: Path, run_id: str, candidate_id: str) -> dict[str, Any]:
    path = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
    df = _read_table(path)
    if df.empty:
        return {}
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        if column not in df.columns:
            continue
        safe_values = df[column].astype(str).str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.strip("_")
        matched = df[(df[column].astype(str) == str(candidate_id)) | (safe_values == _safe_name(candidate_id))]
        if not matched.empty:
            return matched.iloc[0].to_dict()
    return df.iloc[0].to_dict()


def _results_row(data_root: Path, run_id: str, candidate_id: str) -> dict[str, Any]:
    payload = _read_json(data_root / "reports" / "results" / "results_index.json")
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    safe_candidate = _safe_name(candidate_id)
    for row in rows:
        if str(row.get("run_id", "") or "") != run_id:
            continue
        row_candidate = str(row.get("candidate_id", "") or "")
        if row_candidate == candidate_id or _safe_name(row_candidate) == safe_candidate:
            return row
    return {}


def _context_from_trace_or_candidate(trace: pd.DataFrame, candidate: dict[str, Any]) -> str:
    if not trace.empty:
        row = trace.iloc[0]
        key = str(row.get("context_key", "") or "")
        value = str(row.get("context_value", "") or "")
        if key and value:
            return f"{key}={value}"
    for column in ("context", "context_signature", "context_slice", "context_cell"):
        value = candidate.get(column)
        if results_index._is_missing(value):
            continue
        text = str(value)
        if "VOL_REGIME" in text.upper() and "HIGH" in text.upper():
            return "VOL_REGIME=HIGH"
        if text.strip():
            return text.strip()
    return ""


def _hypothesis(
    *,
    data_root: Path,
    run_id: str,
    candidate_id: str,
    trace: pd.DataFrame,
) -> dict[str, Any]:
    candidate = _phase2_candidate(data_root, run_id, candidate_id)
    result = _results_row(data_root, run_id, candidate_id)
    row = trace.iloc[0].to_dict() if not trace.empty else {}
    return {
        "mechanism_id": str(result.get("mechanism_id") or candidate.get("mechanism_id") or ""),
        "event_id": str(row.get("event_id") or result.get("event_id") or candidate.get("event_id") or candidate.get("event_type") or ""),
        "context": _context_from_trace_or_candidate(trace, candidate),
        "template_id": str(
            row.get("template_id")
            or result.get("template_id")
            or candidate.get("template_id")
            or candidate.get("rule_template")
            or ""
        ),
        "direction": str(row.get("direction") or result.get("direction") or candidate.get("direction") or ""),
        "horizon_bars": results_index._horizon_bars(
            row.get("horizon_bars")
            or result.get("horizon_bars")
            or candidate.get("horizon_bars")
            or candidate.get("horizon")
        ),
        "symbol": str(row.get("symbol") or result.get("symbol") or candidate.get("symbol") or ""),
    }


def _nearby_attempt_count(data_root: Path, run_id: str, hypothesis: dict[str, Any]) -> int:
    path = data_root / "reports" / "results" / "results_index.json"
    payload = _read_json(path)
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    matches = []
    for row in rows:
        if row.get("run_id") != run_id:
            continue
        if str(row.get("event_id", "")) != str(hypothesis.get("event_id", "")):
            continue
        if str(row.get("direction", "")) != str(hypothesis.get("direction", "")):
            continue
        if row.get("horizon_bars") != hypothesis.get("horizon_bars"):
            continue
        matches.append(row)
    if not matches:
        return 0
    matches.sort(key=lambda row: 0 if not row.get("symbol") else 1)
    return int(matches[0].get("nearby_attempt_count") or 0)


def _discover_doctor_status(run_id: str, data_root: Path) -> str:
    try:
        report = build_discover_doctor_report(run_id=run_id, data_root=data_root)
    except Exception:
        return ""
    return str(report.get("status", "") or "")


def _report_paths(data_root: Path, run_id: str, candidate_id: str) -> tuple[Path, Path]:
    base = data_root / "reports" / "autopsy" / run_id
    safe = _safe_name(candidate_id)
    return base / f"{safe}_autopsy.json", base / f"{safe}_autopsy.md"


def report_paths(data_root: Path | str | None, run_id: str, candidate_id: str) -> tuple[Path, Path]:
    return _report_paths(_data_root(data_root), run_id, candidate_id)


def _primary_failure_reason(result: dict[str, Any], year_split: dict[str, Any], specificity: dict[str, Any]) -> str:
    decision_reason = str(result.get("decision_reason", "") or "")
    if decision_reason:
        return decision_reason
    if (
        str(specificity.get("classification", "") or "") == "context_proxy"
        and str(year_split.get("classification", "") or "") == "year_conditional"
    ):
        max_year = (year_split.get("concentration") or {}).get("max_pnl_year") or 2022
        return f"context_proxy_and_year_pnl_concentration_{max_year}"
    if str(result.get("governed_reproduction_reason", "") or ""):
        return str(result.get("governed_reproduction_reason"))
    return str(year_split.get("reason", "") or specificity.get("reason", "") or "")


def _supporting_failure_reasons(
    *,
    primary: str,
    result: dict[str, Any],
    year_split: dict[str, Any],
    specificity: dict[str, Any],
) -> list[str]:
    reasons = []
    for reason in (
        result.get("governed_reproduction_reason"),
        year_split.get("reason"),
        specificity.get("reason"),
    ):
        text = str(reason or "")
        if text and text != primary and text not in reasons:
            reasons.append(text)
    classification_pair = (
        str(year_split.get("classification", "") or ""),
        str(specificity.get("classification", "") or ""),
    )
    if classification_pair == ("year_conditional", "context_proxy"):
        text = "year_conditional_and_context_proxy"
        if text != primary and text not in reasons:
            reasons.append(text)
    return reasons


def _conditions_to_reopen(primary_failure_reason: str) -> list[str]:
    if primary_failure_reason == "governed_reproduction_negative_t_stat":
        return [
            "detector/materialization bug found",
            "new data source changes OI_FLUSH definition",
        ]
    if primary_failure_reason.startswith("context_proxy_and_year_pnl_concentration"):
        return [
            "define ex-ante crisis/high-vol regime thesis",
            "prove event+context beats context-only outside 2022",
        ]
    return ["new ex-ante mechanism or data-quality finding invalidates this autopsy"]


def _forbidden_rescue_actions(primary_failure_reason: str) -> list[str]:
    if primary_failure_reason == "governed_reproduction_negative_t_stat":
        return [
            "retest nearby horizon",
            "switch template after failure",
            "validate despite negative reproduction",
        ]
    if primary_failure_reason.startswith("context_proxy_and_year_pnl_concentration"):
        return [
            "drop_2022_after_result",
            "change_horizon",
            "change_context",
            "loosen_gates",
        ]
    return list(DEFAULT_FORBIDDEN_RESCUE_ACTIONS)


def _render_markdown(report: dict[str, Any]) -> str:
    evidence = report["evidence"]
    lines = [
        f"# Candidate Autopsy: {report['candidate_id']}",
        "",
        "## Candidate",
        (
            f"{report['event_id']} / {report['template_id']} / {report['direction']} / "
            f"{report['horizon_bars']} bars."
        ),
        "",
        "## Decision",
        f"`{report['decision']}`: `{report['primary_failure_reason']}`.",
        "",
        "## Evidence",
        f"- discover_doctor_status: `{evidence['discover_doctor_status']}`",
        f"- governed_reproduction_status: `{evidence['governed_reproduction_status']}`",
        f"- year_split_status: `{evidence['year_split_status']}`",
        f"- specificity_status: `{evidence['specificity_status']}`",
        f"- nearby_attempt_count: `{evidence['nearby_attempt_count']}`",
        "",
        "## Supporting Failures",
    ]
    lines.extend(f"- {reason}" for reason in report["supporting_failure_reasons"])
    lines.extend(
        [
            "",
            "## Conditions To Reopen",
        ]
    )
    lines.extend(f"- {condition}" for condition in report["conditions_to_reopen"])
    lines.extend(["", "## Forbidden Rescue Actions"])
    lines.extend(f"- {action}" for action in report["forbidden_rescue_actions"])
    lines.append("")
    return "\n".join(lines)


def _normalize_decision(result: dict[str, Any], primary_failure_reason: str) -> str:
    decision = str(result.get("decision", "") or "")
    if decision in {"park", "kill"}:
        return decision
    if primary_failure_reason == "governed_reproduction_negative_t_stat":
        return "kill"
    return "park"


def _normalize_evidence_class(decision: str, result: dict[str, Any]) -> str:
    evidence_class = str(result.get("evidence_class", "") or "")
    if decision == "kill":
        return "killed_candidate"
    if decision == "park":
        return "parked_candidate"
    return evidence_class


def _context_from_result_or_candidate(result: dict[str, Any], candidate: dict[str, Any]) -> str:
    context = str(result.get("context", "") or "")
    if context:
        return context
    return _context_from_trace_or_candidate(pd.DataFrame(), candidate)


def _artifact_path(path: Path) -> str:
    return str(path) if path.exists() else ""


def _autopsy_payload(
    *,
    run_id: str,
    candidate_id: str,
    hypothesis: dict[str, Any],
    result: dict[str, Any],
    year_split: dict[str, Any],
    specificity: dict[str, Any],
    reproduction: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    primary = _primary_failure_reason(result, year_split, specificity)
    decision = _normalize_decision(result, primary)
    return {
        "schema_version": SCHEMA_VERSION,
        "mechanism_id": str(result.get("mechanism_id") or hypothesis.get("mechanism_id") or ""),
        "run_id": run_id,
        "candidate_id": candidate_id,
        "event_id": str(hypothesis.get("event_id", "") or ""),
        "template_id": str(hypothesis.get("template_id", "") or ""),
        "direction": str(hypothesis.get("direction", "") or ""),
        "horizon_bars": hypothesis.get("horizon_bars"),
        "decision": decision,
        "evidence_class": _normalize_evidence_class(decision, result),
        "primary_failure_reason": primary,
        "supporting_failure_reasons": _supporting_failure_reasons(
            primary=primary,
            result=result,
            year_split=year_split,
            specificity=specificity,
        ),
        "evidence": evidence,
        "conditions_to_reopen": _conditions_to_reopen(primary),
        "forbidden_rescue_actions": _forbidden_rescue_actions(primary),
        "source_artifacts": {
            "governed_reproduction": _artifact_path(
                Path("data") / "reports" / "reproduction" / run_id / "governed_reproduction.json"
            )
            if reproduction
            else "",
        },
    }


def build_candidate_autopsy(
    *,
    run_id: str,
    candidate_id: str,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    trace_path, trace_json_path = trace_paths(resolved_data_root, run_id, candidate_id)
    traces = _read_table(trace_path)
    trace_meta = _read_json(trace_json_path)
    result = _results_row(resolved_data_root, run_id, candidate_id)
    candidate = _phase2_candidate(resolved_data_root, run_id, candidate_id)
    hypothesis = _hypothesis(
        data_root=resolved_data_root,
        run_id=run_id,
        candidate_id=candidate_id,
        trace=traces,
    )
    year_split = _read_json(
        resolved_data_root
        / "reports"
        / "regime"
        / run_id
        / f"{_safe_name(candidate_id)}_year_split.json"
    )
    specificity = _read_json(
        resolved_data_root
        / "reports"
        / "specificity"
        / run_id
        / f"{_safe_name(candidate_id)}_specificity.json"
    )
    reproduction = _read_json(
        resolved_data_root / "reports" / "reproduction" / run_id / "governed_reproduction.json"
    )
    trace_mean = _round(_to_float(traces["net_return_bps"].mean()) if "net_return_bps" in traces else None)
    evidence = {
        "discover_doctor_status": _discover_doctor_status(run_id, resolved_data_root),
        "governed_reproduction_status": str(reproduction.get("status", "") or ""),
        "governed_reproduction_decision": str(reproduction.get("decision", "") or ""),
        "nearby_attempt_count": _nearby_attempt_count(resolved_data_root, run_id, hypothesis),
        "trace_rows": int(trace_meta.get("row_count") or len(traces)),
        "trace_mean_net_bps": trace_mean,
        "year_split_status": str(year_split.get("status", "") or ""),
        "year_split_classification": str(year_split.get("classification", "") or ""),
        "max_pnl_share": year_split.get("concentration", {}).get("max_pnl_share"),
        "max_pnl_year": year_split.get("concentration", {}).get("max_pnl_year"),
        "specificity_status": str(specificity.get("status", "") or ""),
        "specificity_classification": str(specificity.get("classification", "") or ""),
    }
    if not hypothesis.get("context"):
        hypothesis["context"] = _context_from_result_or_candidate(result, candidate)
    return _autopsy_payload(
        run_id=run_id,
        candidate_id=candidate_id,
        hypothesis=hypothesis,
        result=result,
        year_split=year_split,
        specificity=specificity,
        reproduction=reproduction,
        evidence=evidence,
    )


def run_candidate_autopsy(
    *,
    run_id: str,
    candidate_id: str,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    report = build_candidate_autopsy(
        run_id=run_id,
        candidate_id=candidate_id,
        data_root=resolved_data_root,
    )
    json_path, md_path = _report_paths(resolved_data_root, run_id, candidate_id)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    return report
