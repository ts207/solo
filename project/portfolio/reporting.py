from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from project.portfolio.engine import PortfolioCapitalDecision


def write_portfolio_decision_trace(decisions: Iterable[PortfolioCapitalDecision], out_dir: str | Path) -> dict:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "thesis_id": d.thesis_id,
            "symbol": d.symbol,
            "family": d.family,
            "requested_notional": d.requested_notional,
            "allocated_notional": d.allocated_notional,
            "decision_status": d.decision_status,
            "priority_score": d.priority_score,
            "available_capacity_notional": d.available_capacity_notional,
            "clip_factors": list(d.clip_factors),
            "reasons": list(d.reasons),
        }
        for d in decisions
    ]
    payload = {
        "schema_version": "portfolio_decision_trace_v1",
        "decision_count": len(records),
        "allocated_count": sum(1 for r in records if r["allocated_notional"] > 0.0),
        "records": records,
    }
    (out_path / "portfolio_decision_trace.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    lines = ["# Portfolio Decision Trace", "", f"- Decisions: {payload['decision_count']}", f"- Allocated: {payload['allocated_count']}", "", "## Records", ""]
    for r in records:
        lines.append(f"- `{r['thesis_id']}` status={r['decision_status']} allocated={r['allocated_notional']:.2f} requested={r['requested_notional']:.2f} reasons={', '.join(r['reasons'])}")
    (out_path / "portfolio_decision_trace.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return payload
