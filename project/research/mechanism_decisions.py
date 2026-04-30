from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mechanism_decision_v1"


def mechanism_decision_paths(
    data_root: Path | str | None,
    mechanism_id: str,
) -> tuple[Path, Path]:
    base = Path(data_root or "data") / "reports" / "mechanisms" / mechanism_id
    return base / "decision.json", base / "decision.md"


def forced_flow_reversal_pause_decision() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "mechanism_id": "forced_flow_reversal",
        "decision": "pause",
        "reason": (
            "No surviving event-specific candidate after PRICE_DOWN_OI_DOWN parked "
            "and OI_FLUSH killed."
        ),
        "candidate_evidence": [
            {
                "candidate": "PRICE_DOWN_OI_DOWN",
                "decision": "park",
                "reason": "context_proxy_and_year_pnl_concentration_2022",
            },
            {
                "candidate": "OI_FLUSH",
                "decision": "kill",
                "reason": "governed_reproduction_negative_t_stat",
            },
        ],
        "allowed_reopen_conditions": [
            "new ex-ante crisis/high-vol regime specification",
            "new forced-flow observable closer to actual liquidation/deleveraging",
            "material data-quality upgrade such as liquidation prints, order-book depth, or better funding/OI alignment",
        ],
        "forbidden_next_actions": [
            "test nearby horizons",
            "switch symbol to rescue result",
            "drop bad years",
            "loosen gates",
            "continue testing OI/price variants without new mechanism rationale",
        ],
        "next_research_action": "Define crisis_vol_reversal or move to another high-priority mechanism.",
    }


def render_mechanism_decision_markdown(decision: dict[str, Any]) -> str:
    lines = [
        f"# Mechanism Decision: {decision['mechanism_id']}",
        "",
        f"Decision: `{decision['decision']}`",
        "",
        decision["reason"],
        "",
        "## Candidate Evidence",
    ]
    for row in decision.get("candidate_evidence", []):
        lines.append(
            f"- `{row.get('candidate', '')}`: `{row.get('decision', '')}` "
            f"because `{row.get('reason', '')}`"
        )
    lines.extend(["", "## Allowed Reopen Conditions"])
    lines.extend(f"- {item}" for item in decision.get("allowed_reopen_conditions", []))
    lines.extend(["", "## Forbidden Next Actions"])
    lines.extend(f"- {item}" for item in decision.get("forbidden_next_actions", []))
    lines.extend(["", "## Next Research Action", decision["next_research_action"], ""])
    return "\n".join(lines)


def write_mechanism_decision(
    decision: dict[str, Any],
    *,
    data_root: Path | str | None = None,
) -> dict[str, str]:
    json_path, md_path = mechanism_decision_paths(data_root, str(decision["mechanism_id"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_mechanism_decision_markdown(decision), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path)}

