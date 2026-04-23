from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import yaml

from project.operator.bounded import validate_bounded_proposal
from project.research.agent_io.issue_proposal import issue_proposal
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.research.knowledge.memory import ensure_memory_store, write_memory_table


def test_canonical_event_h24_frontdoor_runs_end_to_end_without_runtime_side_effects(
    tmp_path: Path,
    monkeypatch,
) -> None:
    proposal_path = tmp_path / "canonical_event_hypothesis_h24.yaml"
    source_payload = yaml.safe_load(
        Path("spec/proposals/canonical_event_hypothesis.yaml").read_text(encoding="utf-8")
    )
    proposal_payload = dict(source_payload)
    proposal_payload["hypothesis"] = dict(source_payload["hypothesis"])
    proposal_payload["hypothesis"]["horizon_bars"] = 24
    proposal_payload["bounded"] = {
        "baseline_run_id": "volshock_btc_long_12b_20260403T214050Z_38fb41d30e",
        "experiment_type": "horizon_test",
        "allowed_change_field": "horizons_bars",
        "change_reason": "Extend holding horizon from 12 bars to 24 bars",
        "compare_to_baseline": True,
    }
    proposal_path.write_text(
        yaml.safe_dump(proposal_payload, sort_keys=False),
        encoding="utf-8",
    )
    data_root = tmp_path / "data"
    baseline_run_id = "volshock_btc_long_12b_20260403T214050Z_38fb41d30e"

    paths = ensure_memory_store("volshock_btc_long_12b", data_root=data_root)
    baseline_path = paths.proposals_dir / baseline_run_id / "proposal.yaml"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)

    baseline_payload = yaml.safe_load(proposal_path.read_text(encoding="utf-8"))
    baseline_payload["hypothesis"]["horizon_bars"] = 12
    baseline_payload.pop("bounded", None)
    baseline_path.write_text(
        yaml.safe_dump(baseline_payload, sort_keys=False),
        encoding="utf-8",
    )
    write_memory_table(
        "volshock_btc_long_12b",
        "proposals",
        pd.DataFrame(
            [
                {
                    "proposal_id": f"proposal::{baseline_run_id}",
                    "program_id": "volshock_btc_long_12b",
                    "run_id": baseline_run_id,
                    "proposal_path": str(baseline_path),
                }
            ]
        ),
        data_root=data_root,
    )

    normalized = load_operator_proposal(proposal_path)
    assert normalized.program_id == "volshock_btc_long_12b"
    assert normalized.symbols == ["BTCUSDT"]
    assert normalized.templates == ["continuation"]
    assert normalized.directions == ["long"]
    assert normalized.horizons_bars == [24]
    assert normalized.entry_lags == [1]
    assert normalized.trigger_space["allowed_trigger_types"] == ["EVENT"]
    assert normalized.trigger_space["events"]["include"] == ["VOL_SHOCK"]

    bounded = validate_bounded_proposal(normalized, data_root=data_root)
    assert bounded is not None
    assert bounded.changed_fields == ["horizons_bars"]
    assert bounded.baseline_run_id == baseline_run_id

    monkeypatch.setattr(
        "project.research.agent_io.proposal_to_experiment._build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(
            program_id="volshock_btc_long_12b",
            estimated_hypothesis_count=1,
            required_detectors=["vol_shock"],
            required_features=["ret_1"],
            required_states=[],
        ),
    )
    monkeypatch.setattr(
        "project.research.agent_io.execute_proposal.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="planned\n", stderr=""),
    )

    result = issue_proposal(
        proposal_path,
        registry_root=Path("project/configs/registries"),
        data_root=data_root,
        plan_only=True,
    )

    assert result["program_id"] == "volshock_btc_long_12b"
    assert result["bounded_validation"]["changed_fields"] == ["horizons_bars"]
    execution = result["execution"]
    assert execution["returncode"] == 0
    assert "project.pipelines.run_all" in " ".join(execution["command"])
    assert "--plan_only" in execution["command"]

    experiment_config = yaml.safe_load(Path(execution["experiment_config_path"]).read_text(encoding="utf-8"))
    run_all_overrides = json.loads(Path(execution["run_all_overrides_path"]).read_text(encoding="utf-8"))
    assert experiment_config["program_id"] == "volshock_btc_long_12b"
    assert experiment_config["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]
    assert experiment_config["templates"]["include"] == ["continuation"]
    assert experiment_config["evaluation"]["horizons_bars"] == [24]
    assert experiment_config["evaluation"]["directions"] == ["long"]
    assert experiment_config["evaluation"]["entry_lags"] == [1]
    assert run_all_overrides["research_compare_baseline_run_id"] == baseline_run_id

    proposal_memory_dir = Path(result["proposal_memory_dir"])
    assert proposal_memory_dir.exists()
    assert proposal_memory_dir.joinpath("experiment.yaml").exists()
    assert proposal_memory_dir.joinpath("run_all_overrides.json").exists()
    assert proposal_memory_dir.joinpath("canonical_event_hypothesis_h24.yaml").exists()

    assert not (data_root / "live" / "theses").exists()
    assert not (data_root / "reports" / "promotions").exists()
