from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
import hashlib

from project.pipelines.pipeline_defaults import DATA_ROOT, PROJECT_ROOT


def _report_dirs_for_run(run_id: str) -> List[Path]:
    reports_root = DATA_ROOT / "reports"
    if not reports_root.exists():
        return []
    run_dirs: List[Path] = []
    for child in reports_root.iterdir():
        if not child.is_dir():
            continue
        candidate = child / run_id
        if candidate.exists() and candidate.is_dir():
            run_dirs.append(candidate)
    return sorted(run_dirs)


def collect_late_artifacts(run_id: str, cutoff: datetime) -> List[str]:
    """Find files for this run written after the terminal cutoff."""
    late_files = []
    scan_dirs = [DATA_ROOT / "runs" / run_id, *_report_dirs_for_run(run_id)]
    cutoff_ts = cutoff.timestamp()

    for d in scan_dirs:
        if not d.exists():
            continue
        for root, _, files in os.walk(d):
            for f in files:
                p = Path(root) / f
                try:
                    if p.stat().st_mtime > cutoff_ts:
                        # Use relative path from DATA_ROOT for cleaner manifest
                        try:
                            rel_p = p.relative_to(DATA_ROOT)
                        except ValueError:
                            rel_p = p
                        late_files.append(str(rel_p))
                except OSError:
                    continue
    return sorted(late_files)


def _artifact_catalog_for_run(run_id: str) -> Dict[str, Any]:
    scan_dirs = [DATA_ROOT / "runs" / run_id, *_report_dirs_for_run(run_id)]
    artifacts: List[Dict[str, Any]] = []
    for d in scan_dirs:
        if not d.exists():
            continue
        for root, _, files in os.walk(d):
            for f in files:
                p = Path(root) / f
                if not p.is_file():
                    continue
                try:
                    rel = str(p.relative_to(DATA_ROOT))
                except ValueError:
                    rel = str(p)
                try:
                    size = int(p.stat().st_size)
                except OSError:
                    size = 0
                artifacts.append({"path": rel, "size_bytes": size})
    artifacts = sorted(artifacts, key=lambda x: x["path"])
    catalog = {"run_id": str(run_id), "artifact_count": len(artifacts), "artifacts": artifacts}
    digest = hashlib.sha256(json.dumps(catalog, sort_keys=True).encode()).hexdigest()
    catalog["catalog_hash"] = f"sha256:{digest}"
    return catalog


def apply_run_terminal_audit(run_id: str, manifest: Dict[str, Any]) -> None:
    """Performs a final audit of the run artifacts and updates the manifest."""
    cutoff_str = manifest.get("finished_at") or manifest.get("started_at")
    if cutoff_str:
        try:
            cutoff = datetime.fromisoformat(str(cutoff_str).replace("Z", "+00:00"))
        except ValueError:
            cutoff = datetime.now(timezone.utc)
    else:
        cutoff = datetime.now(timezone.utc)

    late = collect_late_artifacts(run_id, cutoff)
    manifest["late_artifacts"] = late
    manifest["terminal_audit_ts"] = datetime.now(timezone.utc).isoformat()
    catalog = _artifact_catalog_for_run(run_id)
    manifest["artifact_count"] = int(catalog.get("artifact_count", 0))
    manifest["artifact_catalog"] = {
        "artifact_count": int(catalog.get("artifact_count", 0)),
        "catalog_hash": str(catalog.get("catalog_hash", "")),
    }
    out_path = DATA_ROOT / "runs" / run_id / "artifact_manifest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(catalog, indent=2, sort_keys=True), encoding="utf-8")


def load_checklist_decision(run_id: str) -> str | None:
    """Loads the research checklist decision for a given run."""
    path = DATA_ROOT / "runs" / run_id / "research_checklist" / "checklist.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return str(payload.get("decision", "")).strip().upper() or None
    except Exception:
        return None


def run_runtime_postflight_audit(
    run_id: str,
    data_root: Path = DATA_ROOT,
    repo_root: Path = PROJECT_ROOT,
    determinism_replay_checks: bool = False,
    max_events: int = 250_000,
    **kwargs,
) -> Dict[str, Any]:
    """Runs the runtime postflight audit by calling the core implementation."""
    from project.runtime.invariants import run_runtime_postflight_audit as _run

    return _run(
        data_root=data_root,
        repo_root=repo_root,
        run_id=run_id,
        determinism_replay_checks=determinism_replay_checks,
        max_events=max_events,
    )


def apply_runtime_postflight_to_manifest(
    run_manifest: Dict[str, Any], runtime_postflight: Dict[str, Any]
) -> str:
    """Copies key stats from the runtime postflight audit result to the manifest."""
    status = runtime_postflight.get("status", "pass")
    run_manifest["runtime_postflight_status"] = status
    run_manifest["runtime_postflight_event_count"] = runtime_postflight.get("event_count", 0)
    run_manifest["runtime_postflight_violation_count"] = runtime_postflight.get(
        "watermark_violation_count", 0
    )
    run_manifest["runtime_postflight_max_lag_us"] = runtime_postflight.get("max_observed_lag_us", 0)
    for src, dst in [
        ("normalization_issue_count", "runtime_normalization_issue_count"),
        ("firewall_violation_count", "runtime_firewall_violation_count"),
        ("determinism_status", "determinism_status"),
        ("replay_digest", "replay_digest"),
        ("oms_replay_status", "oms_replay_status"),
        ("oms_replay_violation_count", "oms_replay_violation_count"),
        ("oms_replay_digest", "oms_replay_digest"),
    ]:
        if src in runtime_postflight:
            run_manifest[dst] = runtime_postflight.get(src)
    return status


def enforce_runtime_postflight(
    run_manifest: Dict[str, Any], runtime_invariants_mode: str = "warn", **kwargs
) -> tuple[bool, List[str]]:
    """Determines if the pipeline should fail based on runtime postflight results."""
    status = run_manifest.get("runtime_postflight_status", "pass")
    violations = run_manifest.get("runtime_postflight_violation_count", 0)

    messages = []
    if status != "pass" or violations > 0:
        msg = f"Runtime postflight audit failed: status={status}, violations={violations}"
        messages.append(msg)
        if runtime_invariants_mode == "enforce":
            return True, messages
    return False, messages


def emit_failure_messages(messages: List[str]) -> None:
    """Prints failure messages to stderr."""
    for m in messages:
        print(f"AUDIT FAILURE: {m}", file=sys.stderr)


def record_non_production_overrides(
    run_manifest: Dict[str, Any],
    overrides: Optional[Any] = None,
    *,
    run_id: Optional[str] = None,
    non_production_overrides: Optional[Any] = None,
    write_run_manifest: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
) -> None:
    """Saves non-production overrides to the manifest. Supports list and dict styles."""
    actual_overrides = overrides or non_production_overrides
    if not actual_overrides:
        return

    if isinstance(actual_overrides, dict):
        sorted_val = {k: actual_overrides[k] for k in sorted(actual_overrides.keys())}
    elif isinstance(actual_overrides, list):
        sorted_val = sorted([str(x) for x in actual_overrides])
    else:
        sorted_val = actual_overrides

    run_manifest["non_production_overrides"] = sorted_val

    if write_run_manifest and run_id:
        write_run_manifest(run_id, run_manifest)
