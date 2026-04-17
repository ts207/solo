from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import yaml

from project.research.agent_io.issue_proposal import issue_proposal
from project.research.knowledge.query import query_memory_rows

issue_proposal_module = importlib.import_module("project.research.agent_io.issue_proposal")


def _write_registry(reg_dir: Path) -> None:
    reg_dir.mkdir(parents=True, exist_ok=True)
    (reg_dir / "events.yaml").write_text(
        yaml.dump(
            {
                "events": {
                    "BASIS_DISLOC": {
                        "enabled": False,
                        "instrument_classes": ["equities"],
                        "sequence_eligible": False,
                        "requires_features": ["poison_feature"],
                    }
                }
            }
        )
    )
    (reg_dir / "states.yaml").write_text(
        yaml.dump({"states": {"HIGH_VOL_REGIME": {"enabled": False, "instrument_classes": ["equities"]}}})
    )
    (reg_dir / "features.yaml").write_text(yaml.dump({"features": {}}))
    (reg_dir / "templates.yaml").write_text(
        yaml.dump(
            {"templates": {"continuation": {"enabled": False, "supports_trigger_types": ["FEATURE_PREDICATE", "STATISTICAL_DISLOCATION"]}}}
        )
    )
    (reg_dir / "contexts.yaml").write_text(
        yaml.dump({"context_dimensions": {"session": {"allowed_values": ["open", "close"]}}})
    )
    (reg_dir / "search_limits.yaml").write_text(
        yaml.dump(
            {
                "limits": {
                    "max_events_per_run": 10,
                    "max_templates_per_run": 10,
                    "max_horizons_per_run": 10,
                    "max_directions_per_run": 10,
                    "max_entry_lags_per_run": 4,
                    "max_hypotheses_total": 1000,
                    "max_hypotheses_per_template": 250,
                    "max_hypotheses_per_event_family": 300,
                },
                "defaults": {
                    "horizons_bars": [12, 24],
                    "directions": ["long", "short"],
                    "entry_lags": [1, 2],
                },
            }
        )
    )
    (reg_dir / "detectors.yaml").write_text(
        yaml.dump({"detector_ownership": {"BASIS_DISLOC": "BasisDislocDetector"}})
    )


def _write_proposal(path: Path) -> None:
    path.write_text(
        yaml.dump(
            {
                "program_id": "btc_campaign",
                "description": "basis continuation slice",
                "run_mode": "research",
                "objective_name": "retail_profitability",
                "promotion_profile": "research",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "instrument_classes": ["crypto"],
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "BASIS_DISLOC"},
                    "filters": {"contexts": {"session": ["open"]}},
                    "sampling_policy": {"entry_lag_bars": 1},
                    "template": {"id": "continuation"},
                    "direction": "long",
                    "horizon_bars": 12,
                },
            }
        ),
        encoding="utf-8",
    )


def test_issue_proposal_writes_program_memory_audit(monkeypatch, tmp_path):
    proposal_path = tmp_path / "proposal.yaml"
    registry_root = tmp_path / "registries"
    data_root = tmp_path / "data"
    _write_registry(registry_root)
    _write_proposal(proposal_path)

    monkeypatch.setattr(
        issue_proposal_module,
        "execute_proposal",
        lambda *args, **kwargs: {
            "run_id": kwargs["run_id"],
            "proposal_path": str(args[0]),
            "experiment_config_path": str(Path(kwargs["out_dir"]) / "experiment.yaml"),
            "run_all_overrides_path": str(Path(kwargs["out_dir"]) / "run_all_overrides.json"),
            "command": ["python", "-m", "project.pipelines.run_all"],
            "returncode": 0,
            "stdout": "Plan for run\n",
            "stderr": "",
            "validated_plan": {"program_id": "btc_campaign", "estimated_hypothesis_count": 2},
        },
    )

    result = issue_proposal(
        proposal_path,
        registry_root=registry_root,
        data_root=data_root,
        plan_only=True,
    )

    proposals_path = (
        data_root / "artifacts" / "experiments" / "btc_campaign" / "memory" / "proposals.parquet"
    )
    proposals = pd.read_parquet(proposals_path)
    proposal_dir = Path(result["proposal_memory_dir"])
    memory_payload = query_memory_rows(program_id="btc_campaign", data_root=data_root)

    assert proposals_path.exists()
    assert len(proposals) == 1
    assert proposals.iloc[0]["program_id"] == "btc_campaign"
    assert proposals.iloc[0]["status"] == "planned"
    assert proposal_dir.exists()
    assert any(Path(row["proposal_path"]).exists() for row in memory_payload["proposals"])
    assert result["run_id"].startswith("btc_campaign_")


def test_build_run_all_command_repeats_config_overlays():
    from project.research.agent_io.execute_proposal import build_run_all_command

    cmd = build_run_all_command(
        run_id="demo_run",
        registry_root=Path("project/configs/registries"),
        experiment_config_path=Path("/tmp/experiment.yaml"),
        run_all_overrides={
            "config": ["project/configs/a.yaml", "project/configs/b.yaml"],
            "discovery_profile": "synthetic",
            "search_spec": "synthetic_truth",
        },
        symbols=["BTCUSDT"],
        start="2025-01-01",
        end="2025-01-31",
        plan_only=True,
        dry_run=False,
    )

    config_pairs = [
        (cmd[idx], cmd[idx + 1])
        for idx in range(len(cmd) - 1)
        if cmd[idx] == "--config"
    ]
    assert config_pairs == [
        ("--config", "project/configs/a.yaml"),
        ("--config", "project/configs/b.yaml"),
    ]
