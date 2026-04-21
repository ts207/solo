from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT


@dataclass(frozen=True)
class MatrixPlan:
    run_label: str
    proposal_id: str
    proposal_path: str
    horizon_bars: int
    context: str
    template: str = "exhaustion_reversal"
    event_id: str = "LIQUIDATION_EXHAUSTION_REVERSAL"
    direction: str = "long"


PHASE_A: tuple[MatrixPlan, ...] = (
    MatrixPlan(
        run_label="A1",
        proposal_id="single_event_liq_exhaust_exhaustion_reversal_long_h03_base_v1",
        proposal_path="spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h03_base_v1.yaml",
        horizon_bars=3,
        context="base",
    ),
    MatrixPlan(
        run_label="A2",
        proposal_id="single_event_liq_exhaust_exhaustion_reversal_long_h12_base_v1",
        proposal_path="spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h12_base_v1.yaml",
        horizon_bars=12,
        context="base",
    ),
    MatrixPlan(
        run_label="A3",
        proposal_id="single_event_liq_exhaust_exhaustion_reversal_long_h24_base_v1",
        proposal_path="spec/proposals/single_event_liq_exhaust_exhaustion_reversal_long_h24_base_v1.yaml",
        horizon_bars=24,
        context="base",
    ),
)


def _run_plan(plan: MatrixPlan) -> dict[str, Any]:
    proposal_path = PROJECT_ROOT.parent / plan.proposal_path
    command = [
        sys.executable,
        "-m",
        "project.cli",
        "discover",
        "plan",
        "--proposal",
        str(proposal_path),
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT.parent,
        text=True,
        capture_output=True,
    )
    payload: dict[str, Any] = {}
    if completed.stdout.strip().startswith("{"):
        try:
            loaded = json.loads(completed.stdout)
            payload = loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            payload = {}
    execution = payload.get("execution", {}) if isinstance(payload.get("execution"), dict) else {}
    validated_plan = (
        execution.get("validated_plan", {}) if isinstance(execution.get("validated_plan"), dict) else {}
    )
    estimated = int(validated_plan.get("estimated_hypothesis_count") or 0)
    status = "plan_failed" if completed.returncode != 0 else "reject" if estimated == 0 else "planned"
    return {
        "matrix": asdict(plan),
        "status": status,
        "run_id": payload.get("run_id") or execution.get("run_id") or "",
        "proposal_memory_dir": payload.get("proposal_memory_dir") or "",
        "returncode": int(completed.returncode),
        "estimated_hypothesis_count": estimated,
        "required_detectors": list(validated_plan.get("required_detectors") or []),
        "required_features": list(validated_plan.get("required_features") or []),
        "required_states": list(validated_plan.get("required_states") or []),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# Liquidation Exhaustion Phase A Plan Summary",
        "",
        f"- matrix: `{report['matrix_id']}`",
        f"- generated_at: `{report['generated_at']}`",
        f"- status: `{report['status']}`",
        "",
        "| run | proposal id | horizon | context | estimated hypotheses | requirements | decision |",
        "|---|---|---:|---|---:|---|---|",
    ]
    for item in report["plans"]:
        requirements: list[str] = []
        for key in ("required_detectors", "required_features", "required_states"):
            values = item.get(key) or []
            if values:
                requirements.append(f"{key}={','.join(values)}")
        lines.append(
            "| {run} | `{proposal}` | {horizon} | {context} | {estimated} | {requirements} | {status} |".format(
                run=item["matrix"]["run_label"],
                proposal=item["matrix"]["proposal_id"],
                horizon=item["matrix"]["horizon_bars"],
                context=item["matrix"]["context"],
                estimated=item["estimated_hypothesis_count"],
                requirements="<br>".join(requirements) if requirements else "none",
                status=item["status"],
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan the bounded liquidation-exhaustion Phase A matrix only."
    )
    parser.add_argument(
        "--out_dir",
        default=str(
            PROJECT_ROOT.parent
            / "data"
            / "reports"
            / "liquidation_exhaustion_matrix"
            / "plans"
        ),
    )
    args = parser.parse_args(argv)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_dir) / f"phase_a_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    plans = [_run_plan(item) for item in PHASE_A]
    failures = [
        item
        for item in plans
        if item["returncode"] != 0 or int(item["estimated_hypothesis_count"]) <= 0
    ]
    report = {
        "schema_version": "liquidation_exhaustion_matrix_plan_v1",
        "matrix_id": "liquidation_exhaustion_phase_a",
        "generated_at": stamp,
        "status": "blocked" if failures else "planned",
        "execution_boundary": "plan_only_no_discover_run",
        "plans": plans,
        "failures": failures,
    }
    json_path = out_dir / "phase_a_plan_summary.json"
    md_path = out_dir / "phase_a_plan_summary.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    _write_markdown(report, md_path)
    print(json.dumps({"report": str(json_path), "summary": str(md_path), "status": report["status"]}, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
