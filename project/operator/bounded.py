
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from project.core.config import get_data_root
from project.research.agent_io.proposal_schema import AgentProposal, load_operator_proposal
from project.research.knowledge.memory import read_memory_table

_TRACKED_FIELDS: tuple[str, ...] = (
    "start",
    "end",
    "symbols",
    "templates",
    "timeframe",
    "horizons_bars",
    "directions",
    "entry_lags",
    "trigger_space",
    "contexts",
    "objective_name",
    "promotion_profile",
    "discovery_profile",
    "phase2_gate_profile",
    "search_spec",
    "config_overlays",
    "knobs",
)


@dataclass(frozen=True)
class BoundedValidationResult:
    baseline_run_id: str
    changed_fields: list[str] = field(default_factory=list)
    frozen_fields: list[str] = field(default_factory=list)
    baseline_proposal_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline_run_id": self.baseline_run_id,
            "changed_fields": list(self.changed_fields),
            "frozen_fields": list(self.frozen_fields),
            "baseline_proposal_path": self.baseline_proposal_path,
        }


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _normalize_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_value(v) for v in value]
    return value


def _proposal_field_map(proposal: AgentProposal) -> dict[str, Any]:
    payload = proposal.to_dict()
    return {field: _normalize_value(payload.get(field)) for field in _TRACKED_FIELDS}


def _load_baseline_proposal_path(*, program_id: str, baseline_run_id: str, data_root: Path) -> Path:
    proposals = read_memory_table(program_id, 'proposals', data_root=data_root)
    if proposals.empty or 'run_id' not in proposals.columns:
        raise ValueError(f'No proposal memory rows found for program_id={program_id!r}')
    matches = proposals.loc[proposals['run_id'].astype(str) == str(baseline_run_id)]
    if matches.empty:
        raise ValueError(f'Baseline run_id {baseline_run_id!r} not found in proposal memory for program_id={program_id!r}')
    proposal_path = str(matches.iloc[-1].get('proposal_path', '') or '').strip()
    if not proposal_path:
        raise ValueError(f'Baseline run_id {baseline_run_id!r} has no stored proposal_path')
    path = Path(proposal_path)
    if not path.exists():
        raise FileNotFoundError(f'Baseline proposal path does not exist: {path}')
    return path


def validate_bounded_proposal(
    proposal: AgentProposal,
    *,
    data_root: Path | None = None,
) -> BoundedValidationResult | None:
    bounded = getattr(proposal, 'bounded', None)
    if bounded is None:
        return None
    baseline_run_id = str(bounded.baseline_run_id or '').strip()
    allowed_change_field = str(bounded.allowed_change_field or '').strip()
    if not baseline_run_id or not allowed_change_field:
        raise ValueError('bounded proposals require baseline_run_id and allowed_change_field')

    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    baseline_path = _load_baseline_proposal_path(
        program_id=proposal.program_id,
        baseline_run_id=baseline_run_id,
        data_root=resolved_data_root,
    )
    baseline = load_operator_proposal(baseline_path)
    if baseline.program_id != proposal.program_id:
        raise ValueError(
            f'Bounded baseline program mismatch: baseline={baseline.program_id!r} current={proposal.program_id!r}'
        )

    current_fields = _proposal_field_map(proposal)
    baseline_fields = _proposal_field_map(baseline)
    changed = [field for field in _TRACKED_FIELDS if current_fields.get(field) != baseline_fields.get(field)]
    if not changed:
        raise ValueError('Bounded proposal must change exactly one tracked field; found no changes relative to baseline')
    if changed != [allowed_change_field]:
        raise ValueError(
            'Bounded proposal changed disallowed fields relative to baseline. '
            f'allowed_change_field={allowed_change_field!r}; changed_fields={json.dumps(changed)}'
        )

    return BoundedValidationResult(
        baseline_run_id=baseline_run_id,
        changed_fields=changed,
        frozen_fields=[field for field in _TRACKED_FIELDS if field != allowed_change_field],
        baseline_proposal_path=str(baseline_path),
    )
