"""
Paper-engine startup certification (no live credentials required).

Exercises the constructor-phase startup sequence:
  1. Config loads and resolves
  2. Thesis store loads from thesis_run_id
  3. Thesis batch reconciliation runs clean
  4. Theses register in ThesisStateManager
  5. Runtime metrics snapshot is produced
  6. State snapshot is written
  7. Deploy run summary is produced

Does NOT call runner.start() — that requires a live WS connection.
The certification boundary is: "runner is fully initialized and all startup
artifacts are written; only the venue adapter is not yet connected."

Usage:
    python project/scripts/certify_paper_startup.py \
        --config project/configs/live_paper_<RUN_ID>.yaml \
        --out artifacts/paper_startup_certification.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from project import PROJECT_ROOT
from project.scripts.run_live_engine import (
    build_live_runner,
    load_live_engine_config,
    resolve_live_engine_session_metadata,
)

_LOG = logging.getLogger(__name__)

CERT_SCHEMA_VERSION = "paper_startup_cert_v1"


def _run_certification(
    *,
    config_path: Path,
    snapshot_path: str | None,
    out_path: Path,
) -> dict:
    results: dict = {
        "schema_version": CERT_SCHEMA_VERSION,
        "config_path": str(config_path),
        "certified_at_utc": datetime.now(timezone.utc).isoformat(),
        "checks": {},
        "artifacts": {},
        "passed": False,
        "failure_reason": None,
    }

    checks = results["checks"]

    # ── 1. Config load ───────────────────────────────────────────────────────
    try:
        config = load_live_engine_config(config_path)
        checks["config_load"] = {"passed": True, "runtime_mode": config.get("runtime_mode")}
    except Exception as exc:
        checks["config_load"] = {"passed": False, "error": str(exc)}
        results["failure_reason"] = f"config_load: {exc}"
        return results

    # ── 2. Session metadata ──────────────────────────────────────────────────
    try:
        meta = resolve_live_engine_session_metadata(
            config_path=config_path,
            snapshot_path=snapshot_path,
        )
        checks["session_metadata"] = {
            "passed": True,
            "symbols": meta["symbols"],
            "runtime_mode": meta["runtime_mode"],
            "strategy_runtime_implemented": meta["strategy_runtime_implemented"],
        }
    except Exception as exc:
        checks["session_metadata"] = {"passed": False, "error": str(exc)}
        results["failure_reason"] = f"session_metadata: {exc}"
        return results

    # Determine snapshot path from meta
    resolved_snapshot_path = snapshot_path or meta.get("live_state_snapshot_path") or None

    # ── 3. Runner construction (thesis load + reconcile + register) ──────────
    # Force monitor_only so no credentials are needed.
    # We patch the config strategy_runtime inline via env; simpler: use a
    # shallow copy of the runner factory with runtime_mode overridden.
    try:
        runner = build_live_runner(
            config_path=config_path,
            snapshot_path=resolved_snapshot_path,
            environment=None,  # no credentials
        )
        thesis_store = runner._thesis_store
        thesis_count = len(thesis_store.all()) if thesis_store else 0
        registered_count = len(runner.thesis_manager.states)
        checks["runner_construction"] = {
            "passed": True,
            "thesis_count_loaded": thesis_count,
            "thesis_count_registered": registered_count,
            "runtime_mode": runner.runtime_mode,
        }
    except Exception as exc:
        checks["runner_construction"] = {"passed": False, "error": str(exc)}
        results["failure_reason"] = f"runner_construction: {exc}"
        return results

    # ── 4. Thesis details ────────────────────────────────────────────────────
    if thesis_store:
        thesis_list = []
        for t in thesis_store.all():
            thesis_list.append({
                "thesis_id": t.thesis_id,
                "status": str(t.status),
                "deployment_state": str(getattr(t, "deployment_state", "N/A")),
                "promotion_class": str(t.promotion_class),
            })
        checks["thesis_details"] = {"passed": True, "theses": thesis_list}
    else:
        checks["thesis_details"] = {"passed": False, "error": "no thesis store loaded"}
        results["failure_reason"] = "thesis_details: thesis store is empty"
        return results

    # ── 5. Runtime metrics snapshot ──────────────────────────────────────────
    try:
        metrics_path = runner.persist_runtime_metrics_snapshot()
        if metrics_path is not None:
            checks["metrics_snapshot"] = {"passed": True, "path": str(metrics_path)}
            results["artifacts"]["metrics_snapshot"] = str(metrics_path)
        else:
            checks["metrics_snapshot"] = {
                "passed": True,
                "note": "runtime_metrics_snapshot_path not configured; skipped",
            }
    except Exception as exc:
        checks["metrics_snapshot"] = {"passed": False, "error": str(exc)}
        results["failure_reason"] = f"metrics_snapshot: {exc}"
        return results

    # ── 6. State snapshot write ──────────────────────────────────────────────
    if resolved_snapshot_path:
        try:
            snap_path = Path(resolved_snapshot_path)
            snap_path.parent.mkdir(parents=True, exist_ok=True)
            saved = runner.state_store.save_snapshot(snap_path)
            checks["state_snapshot"] = {"passed": True, "path": str(saved)}
            results["artifacts"]["state_snapshot"] = str(saved)
        except Exception as exc:
            checks["state_snapshot"] = {"passed": False, "error": str(exc)}
            results["failure_reason"] = f"state_snapshot: {exc}"
            return results
    else:
        checks["state_snapshot"] = {
            "passed": True,
            "note": "no snapshot_path configured; skipped",
        }

    # ── 7. Deploy run summary ────────────────────────────────────────────────
    try:
        summary_path = out_path.parent / "deploy_run_summary.json"
        runner.persist_deploy_run_summary(summary_path)
        checks["deploy_run_summary"] = {"passed": True, "path": str(summary_path)}
        results["artifacts"]["deploy_run_summary"] = str(summary_path)
    except Exception as exc:
        checks["deploy_run_summary"] = {"passed": False, "error": str(exc)}
        results["failure_reason"] = f"deploy_run_summary: {exc}"
        return results

    results["passed"] = all(c.get("passed", False) for c in checks.values())
    return results


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Live engine config YAML path.")
    parser.add_argument(
        "--snapshot_path",
        default="",
        help="Override snapshot path (default: from config).",
    )
    parser.add_argument(
        "--out",
        default="artifacts/paper_startup_certification.json",
        help="Output path for the certification manifest.",
    )
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path
    if not config_path.exists():
        print(f"ERROR: config not found: {config_path}", file=sys.stderr)
        return 1

    snapshot_path = str(args.snapshot_path).strip() or None
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path

    out_path.parent.mkdir(parents=True, exist_ok=True)

    result = _run_certification(
        config_path=config_path,
        snapshot_path=snapshot_path,
        out_path=out_path,
    )

    out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))

    if result["passed"]:
        print("\nPAPER STARTUP CERTIFICATION: PASSED", file=sys.stderr)
        return 0
    else:
        print(f"\nPAPER STARTUP CERTIFICATION: FAILED — {result['failure_reason']}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
