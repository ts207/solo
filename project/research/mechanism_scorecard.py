from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.mechanisms import DEFAULT_REGISTRY_PATH, load_mechanism_registry

ROOT = Path(__file__).resolve().parents[2]
RESULTS_INDEX_JSON = ROOT / "data" / "reports" / "results" / "results_index.json"
SCORECARD_DIR = ROOT / "data" / "reports" / "mechanisms"
SCORECARD_JSON = SCORECARD_DIR / "mechanism_scorecard.json"
SCORECARD_PARQUET = SCORECARD_DIR / "mechanism_scorecard.parquet"
SCORECARD_MD = ROOT / "docs" / "research" / "mechanism_scorecard.md"

SCORECARD_COLUMNS = [
    "mechanism_id",
    "mechanism_state",
    "status",
    "priority",
    "candidate_count",
    "surviving_candidate_count",
    "scouting_count",
    "candidate_signal_count",
    "reproduced_signal_count",
    "research_edge_count",
    "confirmed_edge_count",
    "paper_edge_count",
    "parked_count",
    "killed_count",
    "best_candidate_id",
    "best_run_id",
    "best_candidate_decision",
    "best_candidate_autopsy_path",
    "failed_candidate_autopsy_paths",
    "mechanism_decision_path",
    "main_failure_reason",
    "failure_reasons",
    "failure_reason_counts",
    "data_quality_blocker",
    "next_research_action",
]

EVIDENCE_COLUMNS = {
    "candidate_signal_count": {"candidate_signal", "validate_ready"},
    "reproduced_signal_count": {"reproduced_signal"},
    "research_edge_count": {"research_edge"},
    "confirmed_edge_count": {"confirmed_edge"},
    "paper_edge_count": {"paper_edge"},
}


def _load_results_rows(path: Path = RESULTS_INDEX_JSON) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload, dict) else payload
    return [row for row in rows if isinstance(row, dict)]


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _load_year_split_report(root: Path, row: dict[str, Any]) -> dict[str, Any]:
    run_id = str(row.get("run_id", "") or "")
    candidate_id = str(row.get("candidate_id", "") or "")
    safe_candidate = re.sub(r"[^A-Za-z0-9_.-]+", "_", candidate_id).strip("_") or "candidate"
    path = root / "data" / "reports" / "regime" / run_id / f"{safe_candidate}_year_split.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _candidate_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("run_id", "") or ""),
        str(row.get("event_id", "") or ""),
        str(row.get("direction", "") or ""),
        str(row.get("horizon_bars", "") or ""),
    )


def _row_priority(row: dict[str, Any]) -> tuple[int, float]:
    candidate_id = str(row.get("candidate_id", "") or "")
    real_candidate_priority = 0 if "::cand" in candidate_id else 1
    return real_candidate_priority, -_to_float(row.get("t_stat_net"))


def _dedupe_candidate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = _candidate_key(row)
        current = by_key.get(key)
        if current is None or _row_priority(row) < _row_priority(current):
            by_key[key] = row
    return list(by_key.values())


def _best_candidate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    decision_rank = {"review": 0, "monitor": 1, "park": 2, "kill": 3, "archive": 4}
    return sorted(
        rows,
        key=lambda row: (
            decision_rank.get(str(row.get("decision", "") or ""), 9),
            -_to_float(row.get("t_stat_net")),
        ),
    )[0]


def _failure_reason(root: Path, row: dict[str, Any]) -> str:
    if not row:
        return ""
    if (
        str(row.get("specificity_classification", "") or "") == "context_proxy"
        and str(row.get("year_split_classification", "") or "") == "year_conditional"
    ):
        report = _load_year_split_report(root, row)
        max_year = (report.get("concentration") or {}).get("max_pnl_year") or 2022
        return f"context_proxy_and_year_pnl_concentration_{max_year}"
    reason = str(row.get("year_split_reason", "") or row.get("decision_reason", "") or "")
    if reason == "year_pnl_concentration":
        report = _load_year_split_report(root, row)
        max_year = (report.get("concentration") or {}).get("max_pnl_year")
        return f"year_pnl_concentration_{max_year}" if max_year else reason
    return reason


def _failure_reasons(root: Path, rows: list[dict[str, Any]]) -> list[str]:
    reasons = []
    for row in rows:
        reason = _failure_reason(root, row)
        if reason and str(row.get("decision", "") or "") in {"park", "kill"}:
            reasons.append(reason)
    return reasons


def _failure_reason_counts(reasons: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reason in reasons:
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _safe_candidate_name(candidate_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", candidate_id).strip("_") or "candidate"


def _autopsy_path(root: Path, row: dict[str, Any]) -> str:
    run_id = str(row.get("run_id", "") or "")
    candidate_id = str(row.get("candidate_id", "") or "")
    if not run_id or not candidate_id:
        return ""
    path = root / "data" / "reports" / "autopsy" / run_id / f"{_safe_candidate_name(candidate_id)}_autopsy.json"
    return str(path) if path.exists() else ""


def _failed_candidate_autopsy_paths(root: Path, rows: list[dict[str, Any]]) -> list[str]:
    paths = []
    for row in rows:
        if str(row.get("decision", "") or "") not in {"park", "kill"}:
            continue
        path = _autopsy_path(root, row)
        if path:
            paths.append(path)
    return paths


def _mechanism_decision_path(root: Path, mechanism_id: str) -> str:
    path = root / "data" / "reports" / "mechanisms" / mechanism_id / "decision.json"
    return str(path) if path.exists() else ""


def _surviving_candidate_count(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if str(row.get("decision", "") or "") not in {"park", "kill", "archive"}
        and str(row.get("evidence_class", "") or "")
        not in {"killed_candidate", "parked_candidate", "historical_result", "review_only"}
    )


def _mechanism_state(entry_status: str, candidates: list[dict[str, Any]], surviving_count: int) -> str:
    if not candidates:
        return str(entry_status or "")
    if surviving_count <= 0:
        return "active_no_surviving_candidate" if entry_status == "active" else f"{entry_status}_no_surviving_candidate"
    return str(entry_status or "")


def _data_quality_blocker(row: dict[str, Any]) -> str:
    if str(row.get("specificity_classification", "") or "") == "insufficient_trace_data":
        return "specificity_controls_missing"
    return ""


def _main_failure_reason(failure_counts: dict[str, int], best_failure_reason: str) -> str:
    if len(failure_counts) > 1:
        return "no_confirmed_event_specific_forced_flow_candidate"
    if failure_counts:
        return next(iter(failure_counts))
    return best_failure_reason


def _next_research_action(
    row: dict[str, Any],
    failure_reason: str,
    blocker: str,
    *,
    surviving_count: int = 0,
    failure_counts: dict[str, int] | None = None,
) -> str:
    if blocker == "specificity_controls_missing":
        return "build control traces; define crisis_forced_flow_v1 only if justified ex ante"
    if failure_counts and len(failure_counts) > 1 and surviving_count <= 0:
        return "test only a stronger forced-flow observable or define crisis/high-vol regime thesis before retesting"
    if failure_reason.startswith("context_proxy_and_year_pnl_concentration"):
        return "stop this candidate; only reopen under a new ex-ante crisis/high-vol regime thesis"
    if failure_reason:
        return "review mechanism failure before compiling new proposals"
    return "compile one bounded proposal or update mechanism observables"


def build_mechanism_scorecard(root: Path = ROOT) -> pd.DataFrame:
    registry = load_mechanism_registry(DEFAULT_REGISTRY_PATH)
    rows = _load_results_rows(root / "data" / "reports" / "results" / "results_index.json")
    mechanism_rows = [
        row
        for row in rows
        if str(row.get("methodology_epoch", "") or "") == "mechanism_backed"
        and str(row.get("mechanism_id", "") or "")
    ]
    by_mechanism: dict[str, list[dict[str, Any]]] = {}
    for row in _dedupe_candidate_rows(mechanism_rows):
        by_mechanism.setdefault(str(row.get("mechanism_id", "") or ""), []).append(row)

    scorecard_rows: list[dict[str, Any]] = []
    for mechanism_id, entry in registry.mechanisms.items():
        candidates = by_mechanism.get(mechanism_id, [])
        best = _best_candidate(candidates)
        best_failure_reason = _failure_reason(root, best)
        failure_reasons = _failure_reasons(root, candidates)
        failure_counts = _failure_reason_counts(failure_reasons)
        failure_reason = _main_failure_reason(failure_counts, best_failure_reason)
        surviving_count = _surviving_candidate_count(candidates)
        blocker = _data_quality_blocker(best)
        scorecard_rows.append(
            {
                "mechanism_id": mechanism_id,
                "mechanism_state": _mechanism_state(entry.status, candidates, surviving_count),
                "status": entry.status,
                "priority": entry.priority,
                "candidate_count": len(candidates),
                "surviving_candidate_count": surviving_count,
                "scouting_count": 0,
                **{
                    column: sum(
                        1
                        for row in candidates
                        if str(row.get("evidence_class", "") or "") in evidence_values
                    )
                    for column, evidence_values in EVIDENCE_COLUMNS.items()
                },
                "parked_count": sum(1 for row in candidates if row.get("decision") == "park"),
                "killed_count": sum(1 for row in candidates if row.get("decision") == "kill"),
                "best_candidate_id": str(best.get("candidate_id", "") or ""),
                "best_run_id": str(best.get("run_id", "") or ""),
                "best_candidate_decision": str(best.get("decision", "") or ""),
                "best_candidate_autopsy_path": _autopsy_path(root, best),
                "failed_candidate_autopsy_paths": _failed_candidate_autopsy_paths(root, candidates),
                "mechanism_decision_path": _mechanism_decision_path(root, mechanism_id),
                "main_failure_reason": failure_reason,
                "failure_reasons": failure_reasons,
                "failure_reason_counts": failure_counts,
                "data_quality_blocker": blocker,
                "next_research_action": _next_research_action(
                    best,
                    failure_reason,
                    blocker,
                    surviving_count=surviving_count,
                    failure_counts=failure_counts,
                ),
            }
        )
    return pd.DataFrame(scorecard_rows, columns=SCORECARD_COLUMNS)


def write_scorecard_json(df: pd.DataFrame, path: Path = SCORECARD_JSON) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "mechanism_scorecard_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": len(df),
        "mechanisms": df.to_dict(orient="records"),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_scorecard_parquet(df: pd.DataFrame, path: Path = SCORECARD_PARQUET) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def render_scorecard_markdown(df: pd.DataFrame) -> str:
    lines = [
        "# Mechanism Scorecard",
        "",
        "*Auto-generated. Do not edit manually - rerun `project/scripts/update_mechanism_scorecard.py`.*",
        "",
        "| Mechanism | State | Candidates | Surviving | Parked | Killed | Best Candidate | Decision | Autopsy | Mechanism Decision | Main Failure | Failures | Blocker | Next Action |",
        "|---|---|---:|---:|---:|---:|---|---|---|---|---|---|---|---|",
    ]
    for _, row in df.iterrows():
        lines.append(
            f"| {row.get('mechanism_id', '')} | {row.get('mechanism_state', row.get('status', ''))} | "
            f"{int(row.get('candidate_count') or 0)} | {int(row.get('surviving_candidate_count') or 0)} | "
            f"{int(row.get('parked_count') or 0)} | {int(row.get('killed_count') or 0)} | "
            f"{row.get('best_candidate_id', '')} | "
            f"{row.get('best_candidate_decision', '')} | {row.get('best_candidate_autopsy_path', '')} | "
            f"{row.get('mechanism_decision_path', '')} | "
            f"{row.get('main_failure_reason', '')} | "
            f"{json.dumps(row.get('failure_reason_counts', {}) or {}, sort_keys=True)} | "
            f"{row.get('data_quality_blocker', '')} | {row.get('next_research_action', '')} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_scorecard_markdown(df: pd.DataFrame, path: Path = SCORECARD_MD) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_scorecard_markdown(df), encoding="utf-8")


def update_mechanism_scorecard(root: Path = ROOT) -> pd.DataFrame:
    df = build_mechanism_scorecard(root)
    write_scorecard_json(df, root / "data" / "reports" / "mechanisms" / "mechanism_scorecard.json")
    write_scorecard_parquet(
        df, root / "data" / "reports" / "mechanisms" / "mechanism_scorecard.parquet"
    )
    write_scorecard_markdown(df, root / "docs" / "research" / "mechanism_scorecard.md")
    return df
