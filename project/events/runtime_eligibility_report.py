from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT

_DEFAULT_DOMAIN_GRAPH = PROJECT_ROOT.parent / "spec" / "domain" / "domain_graph.yaml"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_domain_graph(path: str | Path | None = None) -> dict[str, Any]:
    graph_path = Path(path) if path else _DEFAULT_DOMAIN_GRAPH
    if not graph_path.exists():
        raise FileNotFoundError(f"domain graph not found: {graph_path}")
    payload = yaml.safe_load(graph_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"domain graph must be a mapping: {graph_path}")
    return payload


def _blocking_reason(event: dict[str, Any]) -> str:
    eligibility = event.get("eligibility") if isinstance(event.get("eligibility"), dict) else {}
    reasons: list[str] = []
    if not _as_bool(event.get("runtime_eligible")):
        reasons.append("runtime_eligible=false")
    if not _as_bool(event.get("promotion_eligible")):
        reasons.append("promotion_eligible=false")
    if not _as_bool(event.get("primary_anchor_eligible")):
        reasons.append("primary_anchor_eligible=false")
    if not _as_bool(eligibility.get("shadow_runtime_allowed")):
        reasons.append("shadow_runtime_allowed=false")
    if not (_as_bool(eligibility.get("micro_live_allowed")) or _as_bool(eligibility.get("scaled_live_allowed"))):
        reasons.append("live_allowed=false")
    disposition = str(event.get("deployment_disposition") or "").strip()
    if disposition and disposition not in {"live_eligible", "primary_runtime", "primary_live"}:
        reasons.append(f"deployment_disposition={disposition}")
    band = str(event.get("detector_band") or "").strip()
    if band in {"context_only", "composite_or_fragile"}:
        reasons.append(f"detector_band={band}")
    return "; ".join(dict.fromkeys(reasons)) or "eligible"


def _event_row(event_type: str, event: dict[str, Any]) -> dict[str, Any]:
    eligibility = event.get("eligibility") if isinstance(event.get("eligibility"), dict) else {}
    paper_eligible = _as_bool(eligibility.get("paper_anchor_allowed")) or _as_bool(event.get("primary_anchor_eligible"))
    live_eligible = _as_bool(eligibility.get("micro_live_allowed")) or _as_bool(eligibility.get("scaled_live_allowed"))
    runtime_eligible = _as_bool(event.get("runtime_eligible"))
    return {
        "event_id": event_type,
        "event_family": event.get("canonical_family") or event.get("research_family") or "",
        "detector_name": event.get("detector_name") or "",
        "detector_band": event.get("detector_band") or "",
        "detector_version": event.get("detector_version") or event.get("version") or "",
        "operational_role": event.get("operational_role") or "",
        "deployment_disposition": event.get("deployment_disposition") or "",
        "research_eligible": _as_bool(eligibility.get("research_planning_allowed")) or _as_bool(event.get("planning_eligible")),
        "promotion_eligible": _as_bool(event.get("promotion_eligible")),
        "runtime_eligible": runtime_eligible,
        "paper_eligible": paper_eligible,
        "live_eligible": live_eligible,
        "blocking_reason": "eligible" if runtime_eligible and live_eligible else _blocking_reason(event),
    }


def build_runtime_eligibility_report(domain_graph_path: str | Path | None = None) -> dict[str, Any]:
    graph_path = Path(domain_graph_path) if domain_graph_path else _DEFAULT_DOMAIN_GRAPH
    graph = _load_domain_graph(graph_path)
    events = graph.get("events") or {}
    if not isinstance(events, dict):
        raise ValueError("domain graph field 'events' must be a mapping")

    rows = [_event_row(str(event_id), event if isinstance(event, dict) else {}) for event_id, event in sorted(events.items())]
    totals = {
        "events": len(rows),
        "research_eligible": sum(1 for row in rows if row["research_eligible"]),
        "promotion_eligible": sum(1 for row in rows if row["promotion_eligible"]),
        "runtime_eligible": sum(1 for row in rows if row["runtime_eligible"]),
        "paper_eligible": sum(1 for row in rows if row["paper_eligible"]),
        "live_eligible": sum(1 for row in rows if row["live_eligible"]),
    }
    try:
        display_path = str(graph_path.resolve().relative_to(PROJECT_ROOT.parent.resolve()))
    except Exception:
        display_path = str(graph_path)
    return {
        "status": "pass",
        "domain_graph_path": display_path,
        "generated_at_utc": (graph.get("metadata") or {}).get("generated_at_utc", ""),
        "totals": totals,
        "rows": rows,
    }


def _markdown_bool(value: Any) -> str:
    return "yes" if _as_bool(value) else "no"


def render_runtime_eligibility_markdown(report: dict[str, Any]) -> str:
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    rows = report.get("rows") if isinstance(report.get("rows"), list) else []
    lines = [
        "# Runtime-Eligible Events Inventory",
        "",
        "Generated from the compiled domain graph. This file is an operator aid; the domain graph remains the source of truth.",
        "",
        "## Summary",
        "",
        f"- Domain graph: `{report.get('domain_graph_path', '')}`",
        f"- Domain graph generated at: `{report.get('generated_at_utc', '')}`",
        f"- Events: `{totals.get('events', 0)}`",
        f"- Research eligible: `{totals.get('research_eligible', 0)}`",
        f"- Promotion eligible: `{totals.get('promotion_eligible', 0)}`",
        f"- Runtime eligible: `{totals.get('runtime_eligible', 0)}`",
        f"- Paper eligible: `{totals.get('paper_eligible', 0)}`",
        f"- Live eligible: `{totals.get('live_eligible', 0)}`",
        "",
        "## Event matrix",
        "",
        "| Event | Family | Detector | Role | Promotion | Runtime | Paper | Live | Blocking reason |",
        "|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("event_id", "")),
                    str(row.get("event_family", "")),
                    str(row.get("detector_name", "")),
                    str(row.get("operational_role", "")),
                    _markdown_bool(row.get("promotion_eligible")),
                    _markdown_bool(row.get("runtime_eligible")),
                    _markdown_bool(row.get("paper_eligible")),
                    _markdown_bool(row.get("live_eligible")),
                    str(row.get("blocking_reason", "")).replace("|", "\\|"),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def write_runtime_eligibility_markdown(*, output_path: str | Path, domain_graph_path: str | Path | None = None) -> dict[str, Any]:
    report = build_runtime_eligibility_report(domain_graph_path=domain_graph_path)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_runtime_eligibility_markdown(report), encoding="utf-8")
    return {"status": "pass", "output_path": str(path), "totals": report["totals"]}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build runtime event eligibility report from domain graph")
    parser.add_argument("--domain_graph", default="spec/domain/domain_graph.yaml")
    parser.add_argument("--output", default="docs/generated/runtime_eligible_events.md")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of writing markdown")
    parser.add_argument("--check", action="store_true", help="fail if generated markdown differs from disk")
    args = parser.parse_args(argv)
    if args.json:
        print(json.dumps(build_runtime_eligibility_report(args.domain_graph), indent=2, sort_keys=True))
        return 0
    report = build_runtime_eligibility_report(args.domain_graph)
    rendered = render_runtime_eligibility_markdown(report)
    output = Path(args.output)
    if args.check:
        current = output.read_text(encoding="utf-8") if output.exists() else ""
        if current != rendered:
            print(f"runtime eligibility report drifted: {output}")
            return 1
        print(json.dumps({"status": "pass", "output_path": str(output), "totals": report["totals"]}, indent=2, sort_keys=True))
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(json.dumps({"status": "pass", "output_path": str(output), "totals": report["totals"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
