from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

REQUIRED_FIELDS = (
    "id",
    "mechanism",
    "event_id",
    "template",
    "direction",
    "horizon_bars",
    "symbol",
    "timeframe",
)


def load_predeclared_hypotheses(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Predeclared hypothesis registry not found: {p}")
    payload = yaml.safe_load(p.read_text(encoding="utf-8"))
    if payload is None:
        return []
    if isinstance(payload, dict):
        payload = payload.get("hypotheses", [])
    if not isinstance(payload, list):
        raise ValueError("Predeclared hypothesis registry must be a list or {'hypotheses': [...] }.")
    return [dict(item) for item in payload if isinstance(item, dict)]


def validate_predeclared_hypotheses(path: str | Path) -> dict[str, Any]:
    rows = load_predeclared_hypotheses(path)
    errors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows):
        hyp_id = str(row.get("id", "") or "").strip()
        missing = [field for field in REQUIRED_FIELDS if not str(row.get(field, "") or "").strip()]
        if not hyp_id:
            errors.append({"index": idx, "error": "missing_id"})
        elif hyp_id in seen:
            errors.append({"index": idx, "id": hyp_id, "error": "duplicate_id"})
        else:
            seen.add(hyp_id)
        if missing:
            errors.append({"index": idx, "id": hyp_id, "error": "missing_required_fields", "fields": missing})
    return {
        "kind": "predeclared_hypothesis_registry_check",
        "path": str(path),
        "count": len(rows),
        "errors": errors,
        "status": "pass" if not errors else "fail",
    }


def _proposal_payload(path: str | Path) -> dict[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("proposal must be a YAML mapping")
    return payload


def _proposal_signature(payload: dict[str, Any]) -> dict[str, Any]:
    hyp = payload.get("hypothesis", {}) if isinstance(payload.get("hypothesis"), dict) else {}
    anchor = hyp.get("anchor", hyp.get("trigger", {})) if isinstance(hyp, dict) else {}
    template = hyp.get("template", {})
    template_id = template.get("id") if isinstance(template, dict) else template
    symbols = payload.get("symbols") or []
    symbol = symbols[0] if isinstance(symbols, list) and symbols else payload.get("symbol")
    return {
        "event_id": str(anchor.get("event_id", "") if isinstance(anchor, dict) else "").strip().upper(),
        "template": str(template_id or "").strip(),
        "direction": str(hyp.get("direction", "") or "").strip(),
        "horizon_bars": int(hyp.get("horizon_bars", 0) or 0),
        "symbol": str(symbol or "").strip().upper(),
        "timeframe": str(payload.get("timeframe", "") or "").strip(),
    }


def _row_signature(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(row.get("event_id", "") or "").strip().upper(),
        "template": str(row.get("template", "") or "").strip(),
        "direction": str(row.get("direction", "") or "").strip(),
        "horizon_bars": int(row.get("horizon_bars", 0) or 0),
        "symbol": str(row.get("symbol", "") or "").strip().upper(),
        "timeframe": str(row.get("timeframe", "") or "").strip(),
    }


def check_proposal_against_registry(
    *,
    registry_path: str | Path,
    proposal_path: str | Path,
    hypothesis_id: str | None = None,
) -> dict[str, Any]:
    rows = load_predeclared_hypotheses(registry_path)
    proposal = _proposal_payload(proposal_path)
    proposal_sig = _proposal_signature(proposal)
    candidates = rows
    if hypothesis_id:
        candidates = [row for row in rows if str(row.get("id", "")) == str(hypothesis_id)]
    matches = []
    mismatches = []
    for row in candidates:
        row_sig = _row_signature(row)
        diff = {
            key: {"proposal": proposal_sig.get(key), "registry": row_sig.get(key)}
            for key in proposal_sig
            if proposal_sig.get(key) != row_sig.get(key)
        }
        if not diff:
            matches.append(str(row.get("id", "")))
        else:
            mismatches.append({"id": row.get("id"), "diff": diff})
    return {
        "kind": "predeclared_proposal_check",
        "registry_path": str(registry_path),
        "proposal_path": str(proposal_path),
        "hypothesis_id": hypothesis_id,
        "proposal_signature": proposal_sig,
        "matches": matches,
        "mismatches": mismatches[:10],
        "status": "pass" if matches else "fail",
    }


def dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, default=str)
