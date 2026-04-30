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
FORBIDDEN_RESCUE_ACTIONS = [
    "drop_2022_without_ex_ante_regime_rule",
    "drop_2023_2024_to_preserve_pnl",
    "change_horizon",
    "change_context",
    "loosen_gates",
    "promote_without_specificity_controls",
    "promote_without_forward_confirmation",
]
REOPEN_CONDITIONS = [
    "Define an ex-ante 2022-like high-vol/forced-flow regime label and test it forward.",
    "Generate specificity controls and show base timing strongly beats controls.",
    "Find 2023-2026 subperiod evidence with positive post-cost expectancy.",
    "Explain why 2022 PnL concentration should recur and identify it before the fact.",
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
        matched = df[df[column].astype(str).str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.strip("_") == candidate_id]
        if not matched.empty:
            return matched.iloc[0].to_dict()
    return df.iloc[0].to_dict()


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
    row = trace.iloc[0].to_dict() if not trace.empty else {}
    return {
        "event_id": str(row.get("event_id") or candidate.get("event_id") or candidate.get("event_type") or ""),
        "context": _context_from_trace_or_candidate(trace, candidate),
        "template_id": str(
            row.get("template_id")
            or candidate.get("template_id")
            or candidate.get("rule_template")
            or ""
        ),
        "direction": str(row.get("direction") or candidate.get("direction") or ""),
        "horizon_bars": results_index._horizon_bars(
            row.get("horizon_bars") or candidate.get("horizon_bars") or candidate.get("horizon")
        ),
        "symbol": str(row.get("symbol") or candidate.get("symbol") or ""),
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


def _render_markdown(report: dict[str, Any]) -> str:
    hypothesis = report["hypothesis"]
    evidence = report["evidence"]
    decision = report["decision"]
    lines = [
        f"# Candidate Autopsy: {report['candidate_id']}",
        "",
        "## 1. Candidate summary",
        (
            f"{hypothesis['symbol']} {hypothesis['event_id']} / {hypothesis['context']} / "
            f"{hypothesis['direction']} / {hypothesis['horizon_bars']} bars / "
            f"{hypothesis['template_id']}."
        ),
        "",
        "## 2. Why it looked promising",
        (
            f"Discover doctor was `{evidence['discover_doctor_status']}` and governed "
            f"reproduction was `{evidence['governed_reproduction_status']}` with "
            f"{evidence['trace_rows']} trace rows."
        ),
        "",
        "## 3. What changed after trace extraction",
        (
            f"Trace extraction produced mean net return {evidence['trace_mean_net_bps']} bps, "
            "turning the year split from event-support-only evidence into a PnL-aware check."
        ),
        "",
        "## 4. PnL concentration diagnosis",
        (
            f"Year split is `{evidence['year_split_status']}` / "
            f"`{evidence['year_split_classification']}`. Max PnL share is "
            f"{evidence['max_pnl_share']} in {evidence['max_pnl_year']}."
        ),
        "",
        "## 5. Specificity limitation",
        (
            f"Specificity is `{evidence['specificity_status']}` / "
            f"`{evidence['specificity_classification']}` because control traces are unavailable."
        ),
        "",
        "## 6. Decision: park",
        decision["reason"],
        "",
        "## 7. Conditions required to reopen",
    ]
    lines.extend(f"- {condition}" for condition in report["reopen_conditions"])
    lines.extend(
        [
            "",
            "Do not reopen by dropping years, tuning horizon, switching context, adding symbols, or loosening thresholds.",
            "",
        ]
    )
    return "\n".join(lines)


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
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "hypothesis": hypothesis,
        "evidence": evidence,
        "decision": {
            "evidence_class": "parked_candidate",
            "decision": "park",
            "reason": (
                "PnL is concentrated in 2022 despite event support not being "
                "year-concentrated; specificity controls are unavailable."
            ),
            "next_safe_command": None,
            "forbidden_rescue_actions": FORBIDDEN_RESCUE_ACTIONS,
        },
        "reopen_conditions": REOPEN_CONDITIONS,
    }


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
