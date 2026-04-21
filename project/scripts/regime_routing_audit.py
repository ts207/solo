from __future__ import annotations

import argparse
import json
from pathlib import Path

from project import PROJECT_ROOT
from project.research.regime_routing import validate_regime_routing_spec


def render_markdown(payload: dict) -> str:
    lines = [
        "# Regime Routing Audit",
        "",
        f"- valid: {payload.get('is_valid', False)}",
        f"- routing_profile_id: {payload.get('routing_profile_id', '')}",
        f"- scorecard_version: {payload.get('scorecard_version', '')}",
        f"- scorecard_source_run: {payload.get('scorecard_source_run', '')}",
        f"- executable_regimes: {len(payload.get('executable_regimes', []))}",
        f"- routed_regimes: {len(payload.get('routed_regimes', []))}",
        "",
        "## Findings",
        f"- missing_regimes: {payload.get('missing_regimes', [])}",
        f"- unexpected_regimes: {payload.get('unexpected_regimes', [])}",
        f"- non_routable_entries: {payload.get('non_routable_entries', [])}",
        f"- invalid_templates: {payload.get('invalid_templates', {})}",
        f"- bucket_mismatches: {payload.get('bucket_mismatches', {})}",
        f"- empty_intersection_regimes: {payload.get('empty_intersection_regimes', [])}",
        "",
    ]
    unsupported_templates = payload.get("eligible_templates_without_event_support", {})
    unsupported_events = payload.get("events_without_supported_templates", {})
    if unsupported_templates:
        lines.append("## Routed Templates Without Event Support")
        lines.append("")
        for regime, templates in unsupported_templates.items():
            lines.append(f"- `{regime}`: `{templates}`")
        lines.append("")
    if unsupported_events:
        lines.append("## Executable Events Without Routed Template Support")
        lines.append("")
        for regime, events in unsupported_events.items():
            lines.append(f"- `{regime}`: `{events}`")
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit authored regime routing against the ontology.")
    parser.add_argument(
        "--json-out",
        default=None,
    )
    parser.add_argument(
        "--md-out",
        default=None,
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    payload = validate_regime_routing_spec()
    if args.json_out:
        json_out = Path(args.json_out)
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.md_out:
        md_out = Path(args.md_out)
        md_out.parent.mkdir(parents=True, exist_ok=True)
        md_out.write_text(render_markdown(payload), encoding="utf-8")
    if args.check and not payload.get("is_valid", False):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
