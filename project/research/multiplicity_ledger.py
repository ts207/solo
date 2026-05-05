from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_multiplicity_record(
    *,
    campaign_id: str,
    run_id: str,
    proposal_path: str | Path,
    symbols: list[str] | None = None,
    horizons: list[int] | None = None,
    directions: list[str] | None = None,
    filters: list[str] | None = None,
    templates: list[str] | None = None,
) -> dict[str, Any]:
    symbols = list(symbols or [])
    horizons = list(horizons or [])
    directions = list(directions or [])
    filters = list(filters or [])
    templates = list(templates or [])
    hypothesis_count = (
        max(1, len(symbols) or 1)
        * max(1, len(horizons) or 1)
        * max(1, len(directions) or 1)
        * max(1, len(templates) or 1)
        * max(1, len(filters) or 1)
    )
    return {
        "kind": "multiplicity_record",
        "campaign_id": str(campaign_id),
        "run_id": str(run_id),
        "proposal_path": str(proposal_path),
        "symbols": symbols,
        "horizons": horizons,
        "directions": directions,
        "templates": templates,
        "filters": filters,
        "estimated_hypothesis_count": int(hypothesis_count),
    }


def append_multiplicity_record(path: str | Path, record: dict[str, Any]) -> dict[str, Any]:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    return {"status": "written", "path": str(p), "record": record}


def load_multiplicity_records(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def build_multiplicity_report(path: str | Path, *, campaign_id: str | None = None) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"kind": "multiplicity_report", "path": str(p), "status": "missing", "records": 0}
    rows = load_multiplicity_records(p)
    if campaign_id:
        rows = [row for row in rows if str(row.get("campaign_id", "")) == str(campaign_id)]
    total = sum(int(row.get("estimated_hypothesis_count", 0) or 0) for row in rows)
    return {
        "kind": "multiplicity_report",
        "path": str(p),
        "campaign_id": campaign_id,
        "records": len(rows),
        "total_estimated_hypothesis_count": int(total),
        "run_ids": [str(row.get("run_id", "")) for row in rows],
        "status": "pass",
    }
