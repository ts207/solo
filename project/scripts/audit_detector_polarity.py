from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

from project.events.polarity import infer_semantics_from_event, anchor_role_from_event

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "spec" / "events" / "event_registry_unified.yaml"
CSV_PATH = ROOT / "data" / "reports" / "detector_polarity_audit.csv"
MD_PATH = ROOT / "docs" / "generated" / "detector_polarity_audit.md"

DIRECTIONAL_TEMPLATE_HINTS = {
    "trend_continuation", "breakout_followthrough", "volatility_expansion_follow", "pullback_entry",
    "momentum_fade", "false_breakout_reversal", "forced_flow_rebound", "long_flush_rebound",
    "positioning_flush_reversal", "squeeze_followthrough_confirmed", "overshoot_repair", "range_reversion",
    "basis_repair", "basis_convergence", "basis_funding_convergence", "desync_repair", "lead_lag_follow",
}
FILTER_TEMPLATES = {
    "only_if_funding", "only_if_oi", "only_if_liquidity", "only_if_regime", "only_if_highvol",
    "only_if_trend", "only_if_no_news_window", "slippage_aware_filter", "tail_risk_avoid", "drawdown_filter",
}
GUARD_ROLES = {"execution_guard", "temporal_guard", "context_filter", "risk_guard"}


def _load_events() -> dict[str, dict[str, Any]]:
    payload = yaml.safe_load(REGISTRY_PATH.read_text())
    events = payload.get("events", {}) if isinstance(payload, dict) else {}
    return events if isinstance(events, dict) else {}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def build_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event_id, row in sorted(_load_events().items()):
        family = str(row.get("canonical_family") or row.get("research_family") or "").strip().upper()
        role = str(row.get("operational_role") or "").strip().lower()
        semantics = str(row.get("polarity_semantics") or "").strip().lower() or infer_semantics_from_event(
            event_id=event_id,
            family=family,
            subtype=row.get("subtype", ""),
            role=role,
            metadata=row.get("parameters", {}) if isinstance(row.get("parameters"), dict) else {},
        )
        anchor_role = str(row.get("anchor_role") or "").strip().lower() or anchor_role_from_event(
            role=role,
            deployment_disposition=row.get("deployment_disposition", ""),
            family=family,
            event_id=event_id,
            semantics=semantics,
        )
        templates = [str(t).strip() for t in row.get("templates", []) if str(t).strip()]
        directional_templates = [t for t in templates if t in DIRECTIONAL_TEMPLATE_HINTS]
        filter_templates = [t for t in templates if t in FILTER_TEMPLATES]
        eligibility = row.get("eligibility", {}) if isinstance(row.get("eligibility"), dict) else {}
        issues: list[str] = []
        if not str(row.get("detector_name", "")).strip():
            issues.append("missing_detector_class")
        if semantics == "unknown" and directional_templates:
            issues.append("unknown_polarity_for_directional_templates")
        if anchor_role in GUARD_ROLES and directional_templates:
            issues.append("guard_event_has_directional_templates")
        if semantics == "basis_spread_direction" and any(t in {"trend_continuation", "breakout_followthrough", "volatility_expansion_follow"} for t in templates):
            issues.append("basis_event_uses_price_template")
        if _as_bool(eligibility.get("promotion_candidate_allowed")) and not directional_templates:
            issues.append("promotion_candidate_lacks_expression_template")
        if str(row.get("deployment_disposition", "")).strip().lower() == "primary_trigger_candidate" and semantics == "unknown":
            issues.append("primary_candidate_unknown_polarity")
        side_available = semantics not in {"unknown"}
        directional_safe = side_available and anchor_role not in GUARD_ROLES and semantics in {
            "price_direction", "deviation_direction", "liquidity_sweep_side", "liquidation_side", "price_oi_quadrant",
        }
        contrarian_safe = side_available and anchor_role not in {"execution_guard", "temporal_guard", "context_filter"} and semantics in {
            "price_direction", "deviation_direction", "liquidity_sweep_side", "liquidation_side", "price_oi_quadrant", "basis_spread_direction", "funding_crowding_side",
        }
        rows.append({
            "event_id": event_id,
            "detector_class": row.get("detector_name", ""),
            "family": family,
            "role": role,
            "anchor_role": anchor_role,
            "tier": row.get("tier", ""),
            "lifecycle_stage": row.get("lifecycle_stage", ""),
            "deployment_disposition": row.get("deployment_disposition", ""),
            "polarity_semantics": semantics,
            "polarity_source": row.get("polarity_source", ""),
            "magnitude_source": row.get("magnitude_source", ""),
            "side_available": side_available,
            "directional_template_safe": directional_safe,
            "contrarian_template_safe": contrarian_safe,
            "filter_template_count": len(filter_templates),
            "expression_template_count": len(directional_templates),
            "planning_allowed": _as_bool(eligibility.get("research_planning_allowed", row.get("planning_eligible"))),
            "primary_anchor_allowed": _as_bool(eligibility.get("primary_anchor_allowed", row.get("primary_anchor_eligible"))),
            "promotion_candidate_allowed": _as_bool(eligibility.get("promotion_candidate_allowed", row.get("promotion_eligible"))),
            "paper_anchor_allowed": _as_bool(eligibility.get("paper_anchor_allowed")),
            "issues": "|".join(sorted(set(issues))),
        })
    return rows


def write_outputs(rows: list[dict[str, Any]]) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with CSV_PATH.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    issue_counter = Counter()
    sem_counter = Counter()
    role_counter = Counter()
    for row in rows:
        sem_counter[row["polarity_semantics"]] += 1
        role_counter[row["anchor_role"]] += 1
        for issue in str(row["issues"] or "").split("|"):
            if issue:
                issue_counter[issue] += 1
    lines = [
        "# Detector Polarity Audit",
        "",
        "Generated by `project/scripts/audit_detector_polarity.py`.",
        "",
        f"Total events: {len(rows)}",
        "",
        "## Polarity semantics counts",
        "",
    ]
    for k, v in sorted(sem_counter.items()):
        lines.append(f"- `{k}`: {v}")
    lines += ["", "## Anchor role counts", ""]
    for k, v in sorted(role_counter.items()):
        lines.append(f"- `{k}`: {v}")
    lines += ["", "## Issue counts", ""]
    if issue_counter:
        for k, v in sorted(issue_counter.items()):
            lines.append(f"- `{k}`: {v}")
    else:
        lines.append("- none")
    lines += ["", "## Events with issues", ""]
    issue_rows = [r for r in rows if r["issues"]]
    if issue_rows:
        lines.append("| event | detector | semantics | anchor_role | issues |")
        lines.append("|---|---|---|---|---|")
        for r in issue_rows:
            lines.append(f"| `{r['event_id']}` | `{r['detector_class']}` | `{r['polarity_semantics']}` | `{r['anchor_role']}` | `{r['issues']}` |")
    else:
        lines.append("No issues found.")
    MD_PATH.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-on-runtime-unknown", action="store_true")
    args = parser.parse_args()
    rows = build_rows()
    write_outputs(rows)
    if args.fail_on_runtime_unknown:
        bad = [r for r in rows if r["planning_allowed"] and r["polarity_semantics"] == "unknown" and r["expression_template_count"]]
        if bad:
            print(f"detector polarity audit failed: {len(bad)} planning events have unknown polarity with expression templates")
            for r in bad[:20]:
                print(f"  {r['event_id']}: {r['issues']}")
            return 1
    print(f"wrote {CSV_PATH}")
    print(f"wrote {MD_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
