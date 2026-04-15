from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from project.operator.bounded import validate_bounded_proposal
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.research.knowledge.memory import ensure_memory_store, write_memory_table


def _single_hypothesis_payload(end: str, *, bounded: bool = False) -> dict:
    payload = {
        "program_id": "prog_single",
        "start": "2021-01-01",
        "end": end,
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "hypothesis": {
            "trigger": {"type": "event", "event_id": "VOL_SHOCK"},
            "template": "mean_reversion",
            "direction": "short",
            "horizon_bars": 12,
            "entry_lag_bars": 1,
        },
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


def test_bounded_validation_rejects_single_hypothesis_baseline_on_canonical_path(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    paths = ensure_memory_store("prog_single", data_root=data_root)
    baseline_path = paths.proposals_dir / "base_run" / "proposal.yaml"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(
        yaml.safe_dump(_single_hypothesis_payload("2021-12-31"), sort_keys=False),
        encoding="utf-8",
    )
    write_memory_table(
        "prog_single",
        "proposals",
        pd.DataFrame(
            [
                {
                    "proposal_id": "proposal::base_run",
                    "program_id": "prog_single",
                    "run_id": "base_run",
                    "proposal_path": str(baseline_path),
                }
            ]
        ),
        data_root=data_root,
    )

    with pytest.raises(ValueError, match="no longer supported"):
        load_operator_proposal(
            _single_hypothesis_payload("2022-12-31", bounded=True),
        )



def test_bounded_validation_normalizes_structured_proposals(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    paths = ensure_memory_store("prog_structured", data_root=data_root)
    baseline_path = paths.proposals_dir / "base_run" / "proposal.yaml"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_payload = {
        "program_id": "prog_structured",
        "start": "2021-01-01",
        "end": "2021-12-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "hypothesis": {
            "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
            "template": {"id": "mean_reversion"},
            "direction": "short",
            "horizon_bars": 12,
            "sampling_policy": {"entry_lag_bars": 1},
        },
    }
    baseline_path.write_text(yaml.safe_dump(baseline_payload, sort_keys=False), encoding="utf-8")

    write_memory_table(
        "prog_structured",
        "proposals",
        pd.DataFrame(
            [
                {
                    "proposal_id": "proposal::base_run",
                    "program_id": "prog_structured",
                    "run_id": "base_run",
                    "proposal_path": str(baseline_path),
                }
            ]
        ),
        data_root=data_root,
    )

    current_payload = dict(baseline_payload)
    current_payload["end"] = "2022-12-31"
    current_payload["bounded"] = {
        "baseline_run_id": "base_run",
        "experiment_type": "confirmation",
        "allowed_change_field": "end",
        "change_reason": "2022 confirm",
        "compare_to_baseline": True,
    }

    current = load_operator_proposal(current_payload)
    result = validate_bounded_proposal(current, data_root=data_root)

    assert result is not None
    assert result.changed_fields == ["end"]
