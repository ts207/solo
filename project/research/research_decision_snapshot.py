from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project.research.proposal_evidence import event_lift_is_passing, load_event_lift_rows

SCHEMA_VERSION = "research_decision_snapshot_v1"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "reports" / "research_decision_snapshot"
DATA_BLOCKED_DECISIONS = {"data_blocked", "draft_only"}


@dataclass(frozen=True)
class ResearchDecisionSnapshotRequest:
    data_root: Path
    mechanism_id: str = "funding_squeeze"
    generated_at: str | None = None


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows", [])
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def latest_mechanism_data_quality_path(data_root: Path) -> Path | None:
    base = data_root / "reports" / "data_quality_audit"
    if not base.exists():
        return None
    candidates = sorted(
        base.glob("*/mechanism_data_quality.json"),
        key=lambda path: (path.stat().st_mtime, str(path)),
        reverse=True,
    )
    return candidates[0] if candidates else None


def _mechanism_status(data_root: Path, mechanism_id: str) -> str:
    payload = _load_json(data_root / "reports" / "regime_event_inventory" / "mechanism_inventory.json")
    for row in _rows(payload):
        if str(row.get("id") or "") == mechanism_id:
            classification = str(row.get("classification") or "")
            enabled = bool(row.get("enabled", False))
            if enabled and classification != "draft_mechanism":
                return "active"
            return "draft"
    return "unknown"


def _mechanism_data_quality(data_root: Path, mechanism_id: str) -> tuple[dict[str, Any], str]:
    path = latest_mechanism_data_quality_path(data_root)
    if path is None:
        return {}, ""
    payload = _load_json(path)
    for item in payload.get("mechanisms", []):
        if isinstance(item, dict) and str(item.get("mechanism_id") or "") == mechanism_id:
            return item, str(path)
    return {}, str(path)


def _regime_decision_summary(data_root: Path) -> dict[str, int]:
    payload = _load_json(data_root / "reports" / "regime_baselines" / "regime_scorecard.json")
    summary: dict[str, int] = {}
    for row in _rows(payload):
        decision = str(row.get("decision") or "")
        if decision:
            summary[decision] = summary.get(decision, 0) + 1
    summary.setdefault("allow_event_lift", 0)
    summary.setdefault("reject_directional", 0)
    return dict(sorted(summary.items()))


def _event_lift_passing_count(data_root: Path, mechanism_id: str) -> int:
    base = data_root / "reports" / "event_lift"
    if not base.exists():
        return 0
    count = 0
    for path in sorted(base.glob("*/event_lift.json")):
        for row in load_event_lift_rows(path):
            if str(row.get("mechanism_id") or "") == mechanism_id and event_lift_is_passing(row):
                count += 1
    return count


def _snapshot_decision(
    *,
    data_quality_decision: str,
    data_quality_proxy_fields: list[str],
    regime_summary: dict[str, int],
    event_lift_passing_count: int,
) -> tuple[bool, bool, str, str]:
    proposal_allowed = (
        data_quality_decision not in DATA_BLOCKED_DECISIONS
        and regime_summary.get("allow_event_lift", 0) > 0
        and event_lift_passing_count > 0
    )
    paper_allowed = proposal_allowed and data_quality_decision == "research_allowed"
    if data_quality_decision in DATA_BLOCKED_DECISIONS:
        return proposal_allowed, paper_allowed, "park", "data quality blocks research progression"
    if regime_summary.get("allow_event_lift", 0) <= 0:
        reason = "data is research-usable but regimes are directionally negative"
        if data_quality_proxy_fields:
            reason += f"; paper remains blocked by proxy {', '.join(data_quality_proxy_fields)}"
        return proposal_allowed, paper_allowed, "park", reason
    if event_lift_passing_count <= 0:
        return proposal_allowed, paper_allowed, "park", "regime gate may allow event lift but no passing event_lift evidence exists"
    if data_quality_proxy_fields:
        return proposal_allowed, paper_allowed, "research_only", "proposal evidence exists but paper remains blocked by proxy fields"
    return proposal_allowed, paper_allowed, "continue", "data, regime, and event-lift gates permit proposal work"


def build_research_decision_snapshot(request: ResearchDecisionSnapshotRequest) -> dict[str, Any]:
    data_quality, data_quality_path = _mechanism_data_quality(request.data_root, request.mechanism_id)
    data_quality_decision = str(data_quality.get("data_quality_decision") or "missing")
    blocked_fields = [str(item) for item in data_quality.get("blocked_fields", [])]
    proxy_fields = [str(item) for item in data_quality.get("proxy_fields", [])]
    regime_summary = _regime_decision_summary(request.data_root)
    event_lift_count = _event_lift_passing_count(request.data_root, request.mechanism_id)
    proposal_allowed, paper_allowed, decision, reason = _snapshot_decision(
        data_quality_decision=data_quality_decision,
        data_quality_proxy_fields=proxy_fields,
        regime_summary=regime_summary,
        event_lift_passing_count=event_lift_count,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": request.generated_at or _now_iso(),
        "mechanism_id": request.mechanism_id,
        "mechanism_status": _mechanism_status(request.data_root, request.mechanism_id),
        "data_quality_decision": data_quality_decision,
        "data_quality_blocked_fields": blocked_fields,
        "data_quality_proxy_fields": proxy_fields,
        "data_quality_source_path": data_quality_path,
        "regime_decision_summary": regime_summary,
        "event_lift_passing_count": event_lift_count,
        "proposal_allowed": proposal_allowed,
        "paper_allowed": paper_allowed,
        "decision": decision,
        "reason": reason,
        "valid_next_paths": [
            "Define a new ex-ante regime matrix, not a new event.",
            "Repair or replace basis_zscore if papering funding_squeeze becomes relevant.",
            "Move to another mechanism only after its data-quality status is acceptable.",
        ],
    }


def write_research_decision_snapshot(snapshot: dict[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "research_decision_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_research_decision_snapshot_markdown(snapshot, output_dir=output_dir)


def write_research_decision_snapshot_markdown(snapshot: dict[str, Any], *, output_dir: Path) -> None:
    summary = snapshot.get("regime_decision_summary") or {}
    lines = [
        "# Research Decision Snapshot",
        "",
        f"- mechanism_id: `{snapshot.get('mechanism_id', '')}`",
        f"- mechanism_status: `{snapshot.get('mechanism_status', '')}`",
        f"- decision: `{snapshot.get('decision', '')}`",
        f"- proposal_allowed: `{snapshot.get('proposal_allowed', False)}`",
        f"- paper_allowed: `{snapshot.get('paper_allowed', False)}`",
        f"- reason: {snapshot.get('reason', '')}",
        "",
        "## Gates",
        "",
        f"- data_quality_decision: `{snapshot.get('data_quality_decision', '')}`",
        f"- data_quality_blocked_fields: `{', '.join(snapshot.get('data_quality_blocked_fields', []))}`",
        f"- data_quality_proxy_fields: `{', '.join(snapshot.get('data_quality_proxy_fields', []))}`",
        f"- event_lift_passing_count: `{snapshot.get('event_lift_passing_count', 0)}`",
        "",
        "## Regime Decisions",
        "",
    ]
    for key, value in sorted(summary.items()):
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Valid Next Paths", ""])
    for item in snapshot.get("valid_next_paths", []):
        lines.append(f"- {item}")
    (output_dir / "research_decision_snapshot.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
