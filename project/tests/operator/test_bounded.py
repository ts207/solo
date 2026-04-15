
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.operator.bounded import validate_bounded_proposal
from project.research.agent_io.proposal_schema import load_agent_proposal
from project.research.knowledge.memory import ensure_memory_store, write_memory_table


def _proposal(end: str, *, bounded: bool = False) -> dict:
    payload = {
        "program_id": "prog1",
        "start": "2021-01-01",
        "end": end,
        "symbols": ["BTCUSDT"],
        "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"include": ["VOL_SHOCK"]}},
        "templates": ["mean_reversion"],
        "horizons_bars": [12, 24],
        "directions": ["short"],
        "entry_lags": [1],
        "search_spec": "spec/search_space.yaml",
    }
    if bounded:
        payload["bounded"] = {
            "baseline_run_id": "base_run",
            "experiment_type": "confirmation",
            "allowed_change_field": "end",
            "change_reason": "2022 confirm",
            "compare_to_baseline": True,
        }
    return payload


def test_bounded_validation_accepts_single_allowed_change(tmp_path):
    data_root = tmp_path / 'data'
    paths = ensure_memory_store('prog1', data_root=data_root)
    baseline_path = paths.proposals_dir / 'base_run' / 'proposal.yaml'
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    import yaml
    baseline_path.write_text(yaml.safe_dump(_proposal('2021-12-31')), encoding='utf-8')
    write_memory_table('prog1', 'proposals', pd.DataFrame([{
        'proposal_id': 'proposal::base_run', 'program_id': 'prog1', 'run_id': 'base_run', 'proposal_path': str(baseline_path)
    }]), data_root=data_root)
    current = load_agent_proposal(_proposal('2022-12-31', bounded=True))
    result = validate_bounded_proposal(current, data_root=data_root)
    assert result is not None
    assert result.changed_fields == ['end']


def test_bounded_validation_rejects_scope_expansion(tmp_path):
    data_root = tmp_path / 'data'
    paths = ensure_memory_store('prog1', data_root=data_root)
    baseline_path = paths.proposals_dir / 'base_run' / 'proposal.yaml'
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    import yaml
    baseline_path.write_text(yaml.safe_dump(_proposal('2021-12-31')), encoding='utf-8')
    write_memory_table('prog1', 'proposals', pd.DataFrame([{
        'proposal_id': 'proposal::base_run', 'program_id': 'prog1', 'run_id': 'base_run', 'proposal_path': str(baseline_path)
    }]), data_root=data_root)
    payload = _proposal('2022-12-31', bounded=True)
    payload['horizons_bars'] = [12]
    current = load_agent_proposal(payload)
    try:
        validate_bounded_proposal(current, data_root=data_root)
    except ValueError as exc:
        assert 'changed disallowed fields' in str(exc)
    else:
        raise AssertionError('expected bounded validation failure')
