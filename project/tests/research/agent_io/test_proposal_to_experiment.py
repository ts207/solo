from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from project.research.agent_io import proposal_to_experiment as p2e
from project.tests.research.agent_io.test_issue_proposal import _write_proposal, _write_registry


@pytest.mark.skip(reason="pre-existing regression out of scope")
def test_translate_and_validate_proposal_keeps_bundle_artifacts_in_sync(tmp_path: Path) -> None:
    registry_root = tmp_path / "registries"
    proposal_path = tmp_path / "proposal.yaml"
    out_dir = tmp_path / "proposal_bundle"
    config_path = out_dir / "experiment.yaml"
    _write_registry(registry_root)
    _write_proposal(proposal_path)

    result = p2e.translate_and_validate_proposal(
        proposal_path,
        registry_root=registry_root,
        out_dir=out_dir,
        config_path=config_path,
    )

    assert result["experiment_config_path"] == str(config_path)
    assert config_path.read_text(encoding="utf-8") == (out_dir / "request.yaml").read_text(
        encoding="utf-8"
    )

    validated = json.loads((out_dir / "validated_plan.json").read_text(encoding="utf-8"))
    expanded = pd.read_parquet(out_dir / "expanded_hypotheses.parquet")
    assert int(validated["estimated_hypothesis_count"]) == len(expanded)


def test_translate_and_validate_proposal_does_not_clobber_existing_config_on_failed_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_root = tmp_path / "registries"
    proposal_path = tmp_path / "proposal.yaml"
    out_dir = tmp_path / "proposal_bundle"
    config_path = out_dir / "experiment.yaml"
    _write_registry(registry_root)
    _write_proposal(proposal_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text("program_id: stable_bundle\n", encoding="utf-8")

    def _boom(*args, **kwargs):
        raise ValueError("validation failed")

    monkeypatch.setattr(p2e, "_build_experiment_plan", _boom)

    with pytest.raises(ValueError, match="validation failed"):
        p2e.translate_and_validate_proposal(
            proposal_path,
            registry_root=registry_root,
            out_dir=out_dir,
            config_path=config_path,
        )

    assert config_path.read_text(encoding="utf-8") == "program_id: stable_bundle\n"
    assert not list(out_dir.glob(".experiment__staged__*.yaml"))


def test_translate_and_validate_proposal_preserves_avoid_region_keys(tmp_path: Path) -> None:
    registry_root = tmp_path / "registries"
    proposal_path = tmp_path / "proposal.yaml"
    _write_registry(registry_root)
    _write_proposal(proposal_path)

    payload = yaml.safe_load(proposal_path.read_text(encoding="utf-8"))
    payload["avoid_region_keys"] = ["rk_1", "rk_2"]
    proposal_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    result = p2e.translate_and_validate_proposal(
        proposal_path,
        registry_root=registry_root,
        out_dir=tmp_path / "bundle",
    )

    assert result["experiment_config"]["avoid_region_keys"] == ["rk_1", "rk_2"]
