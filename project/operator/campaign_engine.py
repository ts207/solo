from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.operator.decision_engine import decide_next_action
from project.operator.mutation_engine import generate_next_proposal, write_mutated_proposal
from project.research.agent_io.issue_proposal import issue_proposal
from project.research.campaign_contract import CampaignContract, load_campaign_contract
from project.research.knowledge.memory import read_memory_table, write_memory_table
from project.research.reports.operator_reporting import write_operator_outputs_for_run


@dataclass(frozen=True)
class CampaignSpec:
    campaign_id: str
    initial_proposal: str
    max_cycles: int = 1
    max_fail_streak: int = 1
    registry_root: str = "project/configs/registries"
    stop_conditions: dict[str, Any] | None = None
    program_id: str = ""
    mode: str = "operator_guided"
    schema_version: str = "campaign_contract_v1"


@dataclass(frozen=True)
class CampaignPaths:
    root: Path
    proposals_dir: Path
    reports_dir: Path


def load_campaign_spec(path_or_payload: str | Path | dict[str, Any]) -> CampaignSpec:
    contract = load_campaign_contract(path_or_payload)
    return CampaignSpec(
        campaign_id=contract.campaign_id,
        initial_proposal=contract.initial_proposal,
        max_cycles=int(contract.stop_conditions.max_cycles),
        max_fail_streak=int(contract.stop_conditions.max_fail_streak),
        registry_root=str(contract.registry_root),
        stop_conditions=contract.stop_conditions.to_dict(),
        program_id=str(contract.program_id),
        mode=str(contract.mode),
        schema_version=str(contract.schema_version),
    )


def _campaign_contract_payload(spec: CampaignSpec) -> dict[str, Any]:
    contract = CampaignContract(
        campaign_id=spec.campaign_id,
        program_id=spec.program_id,
        initial_proposal=spec.initial_proposal,
        mode=str(spec.mode),
        registry_root=spec.registry_root,
        stop_conditions=load_campaign_contract({
            "campaign_id": spec.campaign_id,
            "program_id": spec.program_id,
            "initial_proposal": spec.initial_proposal,
            "mode": spec.mode,
            "registry_root": spec.registry_root,
            "stop_conditions": spec.stop_conditions or {
                "max_cycles": spec.max_cycles,
                "max_fail_streak": spec.max_fail_streak,
            },
        }).stop_conditions,
        metadata={"adapter_surface": "project.operator.campaign_engine"},
    )
    return contract.to_dict()


def campaign_paths(campaign_id: str, *, data_root: Path | None = None) -> CampaignPaths:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    root = resolved / "artifacts" / "operator_campaigns" / str(campaign_id)
    return CampaignPaths(root=root, proposals_dir=root / "proposals", reports_dir=root / "reports")


def _persist_campaign_metadata(*, program_id: str, run_id: str, data_root: Path, campaign_id: str, cycle_number: int, branch_id: str, parent_run_id: str, mutation_type: str, branch_depth: int, decision: str = "") -> None:
    proposals = read_memory_table(program_id, "proposals", data_root=data_root)
    if proposals.empty or "run_id" not in proposals.columns:
        return
    mask = proposals["run_id"].astype(str) == str(run_id)
    if not mask.any():
        return
    updates = {
        "campaign_id": campaign_id,
        "cycle_number": int(cycle_number),
        "branch_id": branch_id,
        "parent_run_id": parent_run_id,
        "mutation_type": mutation_type,
        "branch_depth": int(branch_depth),
        "decision": decision,
    }
    for column, value in updates.items():
        if column not in proposals.columns:
            proposals[column] = ""
        proposals.loc[mask, column] = value
    write_memory_table(program_id, "proposals", proposals, data_root=data_root)


def _load_latest_cycle_report(paths: CampaignPaths) -> dict[str, Any]:
    report_path = paths.reports_dir / "campaign_report.json"
    if report_path.exists():
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise DataIntegrityError(
                f"Failed to read campaign report from {report_path}: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise DataIntegrityError(
                f"Campaign report {report_path} did not contain an object payload"
            )
        return payload
    return {}


def _load_campaign_mutation_type(path: Path) -> str:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read campaign proposal {path}: {exc}") from exc

    if path.suffix == ".json":
        try:
            payload = json.loads(raw_text)
        except Exception as exc:
            raise DataIntegrityError(f"Failed to parse campaign proposal json {path}: {exc}") from exc
    else:
        try:
            payload = yaml.safe_load(raw_text)
        except Exception as exc:
            raise DataIntegrityError(f"Failed to parse campaign proposal yaml {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise DataIntegrityError(f"Campaign proposal {path} did not contain an object payload")

    campaign = payload.get("campaign", {})
    if isinstance(campaign, dict):
        return str(campaign.get("mutation_type", "generated"))
    return "generated"


def _write_campaign_report(paths: CampaignPaths, payload: dict[str, Any]) -> Path:
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    target = paths.reports_dir / "campaign_report.json"
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target


def run_campaign(*, campaign_spec_path: str | Path, data_root: Path | None = None, plan_only: bool = False) -> dict[str, Any]:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    spec = load_campaign_spec(campaign_spec_path)
    paths = campaign_paths(spec.campaign_id, data_root=resolved)
    paths.proposals_dir.mkdir(parents=True, exist_ok=True)

    cycles: list[dict[str, Any]] = []
    fail_streak = 0
    current_proposal_path = Path(spec.initial_proposal)
    parent_run_id = ""
    branch_id = "main"
    branch_depth = 0
    stop_reason = "max_cycles_reached"
    program_id = ""

    for cycle_number in range(1, spec.max_cycles + 1):
        issued = issue_proposal(
            current_proposal_path,
            registry_root=Path(spec.registry_root),
            data_root=resolved,
            plan_only=bool(plan_only),
            dry_run=False,
            check=False,
        )
        run_id = str(issued.get("run_id", "") or "")
        program_id = str(issued.get("program_id", "") or program_id)
        mutation_type = (
            "initial"
            if cycle_number == 1
            else _load_campaign_mutation_type(Path(current_proposal_path))
        )

        summary = write_operator_outputs_for_run(run_id=run_id, program_id=program_id, data_root=resolved)
        diagnostics = dict(summary.get("negative_result_diagnostics", {}) or {})
        decision = decide_next_action(run_summary=summary, diagnostics=diagnostics)
        _persist_campaign_metadata(
            program_id=program_id,
            run_id=run_id,
            data_root=resolved,
            campaign_id=spec.campaign_id,
            cycle_number=cycle_number,
            branch_id=branch_id,
            parent_run_id=parent_run_id,
            mutation_type=mutation_type,
            branch_depth=branch_depth,
            decision=decision.action,
        )
        summary = write_operator_outputs_for_run(run_id=run_id, program_id=program_id, data_root=resolved)
        cycle_payload = {
            "cycle_number": cycle_number,
            "run_id": run_id,
            "proposal_path": str(current_proposal_path),
            "decision": decision.to_dict(),
            "summary": {
                "terminal_status": summary.get("terminal_status", ""),
                "verdict": summary.get("verdict", ""),
                "candidate_count": summary.get("candidate_count", 0),
                "promoted_count": summary.get("promoted_count", 0),
                "top_candidate": summary.get("top_candidate", {}),
            },
        }
        cycles.append(cycle_payload)
        parent_run_id = run_id

        if decision.action in {"PROMOTE", "STOP", "REPAIR"}:
            stop_reason = f"decision_{decision.action.lower()}"
            if decision.action == "STOP":
                fail_streak += 1
            break

        fail_streak = 0 if decision.action == "MODIFY" else fail_streak + 1
        if fail_streak >= spec.max_fail_streak:
            stop_reason = "max_fail_streak_reached"
            break

        branch_depth += 1
        mutation = generate_next_proposal(
            baseline_proposal_path=current_proposal_path,
            parent_run_id=run_id,
            diagnostics=diagnostics,
            decision=decision.to_dict(),
            campaign_id=spec.campaign_id,
            cycle_number=cycle_number + 1,
            branch_id=branch_id,
            branch_depth=branch_depth,
        )
        current_proposal_path = write_mutated_proposal(
            mutation=mutation,
            destination=paths.proposals_dir / f"cycle_{cycle_number + 1:03d}.yaml",
        )
    report = {
        "schema_version": "campaign_report_v2",
        "campaign_id": spec.campaign_id,
        "program_id": program_id or spec.program_id,
        "campaign_mode": spec.mode,
        "max_cycles": spec.max_cycles,
        "executed_cycles": len(cycles),
        "stop_reason": stop_reason,
        "plan_only": bool(plan_only),
        "control_plane": {
            "controller_module": "project.research.campaign_controller",
            "operator_adapter_module": "project.operator.campaign_engine",
            "orchestration_surface": "canonical",
        },
        "campaign_contract": _campaign_contract_payload(spec),
        "cycles": cycles,
    }
    report_path = _write_campaign_report(paths, report)
    report["report_path"] = str(report_path)
    return report
