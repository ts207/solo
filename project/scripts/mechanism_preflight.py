#!/usr/bin/env python3
"""Classify a proposal before discovery using mechanism constraints."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from project.research.mechanisms import (
    CandidateHypothesis,
    MechanismIssue,
    MechanismPreflightReport,
    load_mechanism,
    validate_candidate_against_mechanism,
)


def _load_payload(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    payload = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON/YAML object")
    return payload


def _mechanism_id_from_payload(payload: dict[str, Any]) -> str:
    mechanism = payload.get("mechanism")
    if isinstance(mechanism, dict):
        return str(mechanism.get("id") or mechanism.get("mechanism_id") or "").strip()
    return str(payload.get("mechanism_id") or "").strip()


def _next_command(proposal_path: Path) -> str:
    return f"make discover-proposal PROPOSAL={proposal_path} RUN_ID=<run_id> DATA_ROOT=<lake>"


def scouting_report(proposal_path: Path) -> MechanismPreflightReport:
    return MechanismPreflightReport(
        schema_version="mechanism_preflight_v1",
        proposal=str(proposal_path),
        status="warning",
        classification="scouting_only",
        mechanism_id="",
        checks=[
            MechanismIssue(
                id="mechanism_present",
                status="warning",
                detail="Proposal has no mechanism block; evidence class is capped at scouting_signal",
            )
        ],
        required_falsification=[],
        forbidden_rescue_actions=[],
        next_safe_command=_next_command(proposal_path),
    )


def invalid_mechanism_report(
    proposal_path: Path,
    mechanism_id: str,
    detail: str,
) -> MechanismPreflightReport:
    return MechanismPreflightReport(
        schema_version="mechanism_preflight_v1",
        proposal=str(proposal_path),
        status="fail",
        classification="invalid_mechanism",
        mechanism_id=mechanism_id,
        checks=[MechanismIssue(id="mechanism_loadable", status="fail", detail=detail)],
        required_falsification=[],
        forbidden_rescue_actions=[],
        next_safe_command=_next_command(proposal_path),
    )


def build_preflight_report(proposal_path: Path) -> MechanismPreflightReport:
    payload = _load_payload(proposal_path)
    mechanism_id = _mechanism_id_from_payload(payload)
    if not mechanism_id:
        return scouting_report(proposal_path)

    try:
        mechanism = load_mechanism(mechanism_id)
    except (FileNotFoundError, KeyError, ValueError) as exc:
        return invalid_mechanism_report(proposal_path, mechanism_id, str(exc))

    candidate = CandidateHypothesis.from_proposal_payload(payload)
    return validate_candidate_against_mechanism(
        candidate,
        mechanism,
        proposal_path=str(proposal_path),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposal", required=True)
    args = parser.parse_args(argv)

    report = build_preflight_report(Path(args.proposal))
    print(report.to_json())
    return 0 if report.status in {"pass", "warning"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
