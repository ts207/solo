from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from project.core.config import get_data_root
from project.io.utils import atomic_write_text
from project.research.agent_io.execute_proposal import execute_proposal
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.operator.bounded import validate_bounded_proposal
from project.research.knowledge.memory import (
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)
from project.research.knowledge.schemas import canonical_json


def _proposal_signature(payload: Dict[str, Any]) -> str:
    material = canonical_json(payload)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:10]


def generate_run_id(program_id: str, proposal_payload: Dict[str, Any]) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    compact_program = "".join(
        ch if ch.isalnum() else "_" for ch in str(program_id).strip().lower()
    ).strip("_")
    compact_program = compact_program[:24] or "proposal"
    return f"{compact_program}_{stamp}_{_proposal_signature(proposal_payload)}"


def _proposal_artifact_path(paths: Any, run_id: str, suffix: str) -> Path:
    run_root = paths.proposals_dir / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root / suffix


def _write_proposal_copy(destination: Path, source_path: str | Path) -> None:
    text = Path(source_path).read_text(encoding="utf-8")
    atomic_write_text(destination, text)


def _write_proposal_payload(destination: Path, payload: Dict[str, Any]) -> None:
    atomic_write_text(destination, yaml.safe_dump(payload, sort_keys=False))


def _load_raw_proposal_payload(path: str | Path) -> Dict[str, Any]:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Proposal must be a YAML mapping: {path}")
    return dict(raw)


def _merge_proposal_rows(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return incoming.copy()
    if incoming.empty:
        return existing.copy()
    out = pd.concat([existing, incoming], ignore_index=True)
    return out.drop_duplicates(subset=["proposal_id"], keep="last").reset_index(drop=True)


def issue_proposal(
    proposal_path: str | Path,
    *,
    registry_root: Path,
    data_root: Path | None = None,
    run_id: str | None = None,
    plan_only: bool = True,
    dry_run: bool = False,
    check: bool = False,
    promotion_profile: str | None = None,
) -> Dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    promotion_override = str(promotion_profile or "").strip().lower()
    if promotion_override:
        raw_payload = _load_raw_proposal_payload(proposal_path)
        raw_payload["promotion_profile"] = promotion_override
        proposal = load_operator_proposal(raw_payload)
    else:
        raw_payload = None
        proposal = load_operator_proposal(proposal_path)
    bounded_validation = validate_bounded_proposal(proposal, data_root=resolved_data_root)
    proposal_payload = proposal.to_dict()
    resolved_run_id = (
        str(run_id).strip() if run_id else generate_run_id(proposal.program_id, proposal_payload)
    )
    paths = ensure_memory_store(proposal.program_id, data_root=resolved_data_root)

    proposal_copy_path = _proposal_artifact_path(
        paths, resolved_run_id, Path(proposal_path).name or "proposal.yaml"
    )
    if raw_payload is not None:
        _write_proposal_payload(proposal_copy_path, raw_payload)
    else:
        _write_proposal_copy(proposal_copy_path, proposal_path)

    execution = execute_proposal(
        proposal_copy_path,
        run_id=resolved_run_id,
        registry_root=registry_root,
        out_dir=proposal_copy_path.parent,
        data_root=resolved_data_root,
        plan_only=bool(plan_only),
        dry_run=bool(dry_run),
        check=bool(check),
    )

    issued_at = datetime.now(timezone.utc).isoformat()
    campaign_meta = proposal_payload.get("campaign", {}) or {}
    if not isinstance(campaign_meta, dict):
        campaign_meta = {}

    proposal_row = {
        "proposal_id": f"proposal::{resolved_run_id}",
        "program_id": proposal.program_id,
        "run_id": resolved_run_id,
        "issued_at": issued_at,
        "proposal_path": str(proposal_copy_path),
        "experiment_config_path": str(execution["experiment_config_path"]),
        "run_all_overrides_path": str(execution["run_all_overrides_path"]),
        "status": (
            "failed"
            if int(execution["returncode"]) != 0
            else ("planned" if plan_only else ("dry_run" if dry_run else "executed"))
        ),
        "plan_only": bool(plan_only),
        "dry_run": bool(dry_run),
        "returncode": int(execution["returncode"]),
        "objective_name": proposal.objective_name,
        "promotion_profile": proposal.promotion_profile,
        "symbols": ",".join(proposal.symbols),
        "command_json": canonical_json(execution["command"]),
        "validated_plan_json": canonical_json(execution["validated_plan"]),
        "bounded_json": canonical_json(bounded_validation.to_dict()) if bounded_validation is not None else "",
        "baseline_run_id": bounded_validation.baseline_run_id if bounded_validation is not None else "",
        "experiment_type": proposal.bounded.experiment_type if proposal.bounded is not None else "discovery",
        "allowed_change_field": proposal.bounded.allowed_change_field if proposal.bounded is not None else "",
        "campaign_id": str(campaign_meta.get("campaign_id", "") or ""),
        "cycle_number": int(campaign_meta.get("cycle_number", 0) or 0),
        "branch_id": str(campaign_meta.get("branch_id", "") or ""),
        "parent_run_id": str(campaign_meta.get("parent_run_id", "") or ""),
        "mutation_type": str(campaign_meta.get("mutation_type", "") or ""),
        "branch_depth": int(campaign_meta.get("branch_depth", 0) or 0),
        "decision": str(campaign_meta.get("decision", "") or ""),
    }
    existing = read_memory_table(proposal.program_id, "proposals", data_root=resolved_data_root)
    proposals = _merge_proposal_rows(existing, pd.DataFrame([proposal_row]))
    write_memory_table(proposal.program_id, "proposals", proposals, data_root=resolved_data_root)

    return {
        "proposal_id": proposal_row["proposal_id"],
        "program_id": proposal.program_id,
        "run_id": resolved_run_id,
        "proposal_memory_dir": str(proposal_copy_path.parent),
        "proposal_record_path": str(paths.proposals),
        "execution": execution,
        "bounded_validation": bounded_validation.to_dict() if bounded_validation is not None else None,
        "promotion_profile_override": promotion_override or None,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Choose a run_id, store a proposal under memory, and invoke run_all."
    )
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--plan_only", type=int, default=1)
    parser.add_argument("--dry_run", type=int, default=0)
    parser.add_argument("--check", type=int, default=0)
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = issue_proposal(
        args.proposal,
        registry_root=Path(args.registry_root),
        data_root=Path(args.data_root) if args.data_root else None,
        run_id=args.run_id,
        plan_only=bool(args.plan_only),
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["execution"]["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())
