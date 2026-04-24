from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project import PROJECT_ROOT  # noqa: E402

KEYWORDS = ("legacy", "deprecated", "compat", "stale", "not_supported", "blocked until")
SCAN_EXTENSIONS = {".py", ".md", ".yaml", ".yml", ".toml"}
SCAN_ROOTS = ("project", "spec", "docs", "plugins")
EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "data",
    "docs/generated",
}


def _repo_root() -> Path:
    return PROJECT_ROOT.parent


def _is_excluded(path: Path, root: Path) -> bool:
    rel = path.relative_to(root).as_posix()
    parts = set(path.relative_to(root).parts)
    return bool(parts & EXCLUDED_PARTS) or rel.startswith("docs/generated/")


def _iter_scanned_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for scan_root in SCAN_ROOTS:
        base = root / scan_root
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in SCAN_EXTENSIONS:
                continue
            if _is_excluded(path, root):
                continue
            files.append(path)
    return sorted(files)


def _line_hits(path: Path) -> list[dict[str, object]]:
    hits: list[dict[str, object]] = []
    text = path.read_text(encoding="utf-8", errors="ignore")
    for lineno, line in enumerate(text.splitlines(), start=1):
        lowered = line.lower()
        matched = sorted(keyword for keyword in KEYWORDS if keyword in lowered)
        if not matched:
            continue
        hits.append(
            {
                "line": lineno,
                "keywords": matched,
                "text": line.strip()[:220],
            }
        )
    return hits


def _legacy_detector_rows(root: Path) -> list[dict[str, object]]:
    path = root / "docs" / "generated" / "legacy_detector_retirement.md"
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or line.startswith("| Event ") or line.startswith("|---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != 6:
            continue
        event, role, runtime, promotion, primary_anchor, retired_safe = cells
        rows.append(
            {
                "event": event,
                "role": role,
                "runtime_eligible": runtime == "True",
                "promotion_eligible": promotion == "True",
                "primary_anchor": primary_anchor == "True",
                "retired_safe": retired_safe == "True",
            }
        )
    return rows


def _classify_file(rel_path: str, hits: list[dict[str, object]]) -> tuple[str, str]:
    joined = " ".join(str(hit["text"]).lower() for hit in hits)
    if rel_path == "project/scripts/build_legacy_surface_inventory.py":
        return (
            "inventory_tool",
            "Keep; this generator necessarily contains legacy/stale classification keywords.",
        )
    if "/tests/" in f"/{rel_path}" or rel_path.startswith("project/tests/"):
        return (
            "fixture_or_regression",
            "Keep only while it protects canonical behavior or old artifact fixtures.",
        )
    if rel_path in {
        "project/events/canonical_audit.py",
        "project/events/detector_contract.py",
        "project/events/detectors/registry.py",
        "project/events/governance.py",
        "project/events/registry.py",
        "project/research/approval_workflow_v2.py",
        "project/scripts/build_event_deep_analysis_suite.py",
        "project/scripts/ontology_consistency_audit.py",
    }:
        return (
            "status_policy",
            "Keep if it only defines, reads, or audits active lifecycle/status vocabulary.",
        )
    if rel_path in {
        "project/events/event_repository.py",
        "project/live/venue_rules.py",
        "project/live/contracts/live_trade_context.py",
        "project/live/contracts/promoted_thesis.py",
        "project/live/retriever.py",
        "project/live/runner.py",
        "project/live/thesis_store.py",
        "project/research/agent_io/hypothesis_contract.py",
        "project/research/gating.py",
        "project/research/gating_primitives.py",
        "project/scripts/run_benchmark_matrix.py",
    }:
        return (
            "compat_supported",
            "Keep temporarily only with explicit warning, reason, or compatibility semantics.",
        )
    if rel_path == "spec/multiplicity/families.yaml":
        return (
            "historical_spec",
            "Keep as read-only cutover input only while a drift check depends on it.",
        )
    if rel_path.startswith("spec/events/") and all(
        set(hit["keywords"]) == {"stale"} for hit in hits
    ):
        return (
            "domain_risk_language",
            "Keep; this describes stale market data risk, not stale repository surface.",
        )
    if rel_path.startswith("docs/lifecycle/") and (
        "postmortem" in rel_path or "reflections" in rel_path or "matrix" in rel_path
    ):
        return "historical_doc", "Mark as historical; do not teach as current operator guidance."
    if rel_path == "docs/reference/supported-path.md":
        return "policy_doc", "Keep as canonical supported/compat/deprecated policy."
    if rel_path.startswith("docs/"):
        return "doc_review", "Review for stale guidance; either update, mark historical, or remove."
    if "compat" in rel_path or "compatibility" in joined:
        return (
            "compat_surface",
            "Keep only behind explicit compatibility switches or fixture-only helpers.",
        )
    if "deprecated" in joined or "not_supported" in joined or "blocked until" in joined:
        return "deprecation_cleanup", "Delete, migrate, or attach a tracked blocker with an owner."
    if rel_path.startswith("spec/events/"):
        return (
            "legacy_detector_spec",
            "Retain only as historical audit input or migrate to governed v2.",
        )
    if rel_path.startswith("spec/"):
        return "spec_review", "Classify as active contract, historical spec, or delete candidate."
    return "code_review", "Classify as delete_now, migrate, fixture_only, or compat_supported."


def collect_legacy_surface_inventory(root: Path | None = None) -> dict[str, object]:
    repo_root = (root or _repo_root()).resolve()
    file_rows: list[dict[str, object]] = []
    keyword_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()

    for path in _iter_scanned_files(repo_root):
        hits = _line_hits(path)
        if not hits:
            continue
        rel_path = path.relative_to(repo_root).as_posix()
        for hit in hits:
            keyword_counts.update(str(keyword) for keyword in hit["keywords"])
        category, action = _classify_file(rel_path, hits)
        category_counts[category] += 1
        file_rows.append(
            {
                "path": rel_path,
                "category": category,
                "match_count": len(hits),
                "keywords": sorted({keyword for hit in hits for keyword in hit["keywords"]}),
                "recommended_action": action,
                "evidence": hits[:5],
            }
        )

    legacy_detectors = _legacy_detector_rows(repo_root)
    detector_summary_path = repo_root / "docs" / "generated" / "detector_governance_summary.json"
    detector_summary = (
        json.loads(detector_summary_path.read_text(encoding="utf-8"))
        if detector_summary_path.exists()
        else {}
    )
    return {
        "schema_version": "legacy_surface_inventory_v1",
        "file_count": len(file_rows),
        "match_count": sum(int(row["match_count"]) for row in file_rows),
        "keyword_counts": dict(sorted(keyword_counts.items())),
        "category_counts": dict(sorted(category_counts.items())),
        "detector_summary": detector_summary,
        "legacy_detectors": legacy_detectors,
        "legacy_detector_count": len(legacy_detectors),
        "legacy_detectors_retired_safe": sum(1 for row in legacy_detectors if row["retired_safe"]),
        "files": sorted(file_rows, key=lambda row: (str(row["category"]), str(row["path"]))),
    }


def render_legacy_surface_markdown(inventory: dict[str, object]) -> str:
    lines = [
        "# Legacy Surface Inventory",
        "",
        "Generated from repo source and governance artifacts. Do not edit by hand.",
        "",
        "## Summary",
        "",
        f"- Files with legacy/stale markers: **{inventory['file_count']}**",
        f"- Total marker hits: **{inventory['match_count']}**",
        f"- Legacy detectors: **{inventory['legacy_detector_count']}**",
        f"- Legacy detectors retired-safe: **{inventory['legacy_detectors_retired_safe']}**",
        "",
        "## Categories",
        "",
        "| Category | Files |",
        "|---|---:|",
    ]
    for category, count in dict(inventory["category_counts"]).items():
        lines.append(f"| `{category}` | {count} |")

    lines.extend(["", "## Files", "", "| Category | File | Hits | Action |", "|---|---|---:|---|"])
    for row in inventory["files"]:
        category = row["category"]
        path = row["path"]
        match_count = row["match_count"]
        action = row["recommended_action"]
        lines.append(
            f"| `{category}` | `{path}` | {match_count} | {action} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_legacy_surface_json(inventory: dict[str, object]) -> str:
    return json.dumps(inventory, indent=2, sort_keys=True)


def _target_paths(root: Path) -> tuple[Path, Path]:
    generated = root / "docs" / "generated"
    return generated / "legacy_surface_inventory.md", generated / "legacy_surface_inventory.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate legacy/stale surface inventory.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated outputs drift from disk.",
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    inventory = collect_legacy_surface_inventory(repo_root)
    markdown = render_legacy_surface_markdown(inventory)
    json_text = render_legacy_surface_json(inventory)
    md_path, json_path = _target_paths(repo_root)
    expected = [(md_path, markdown), (json_path, json_text)]

    if args.check:
        drift: list[str] = []
        for path, content in expected:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f"legacy surface inventory drift: {path}", file=sys.stderr)
            return 1
        return 0

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
