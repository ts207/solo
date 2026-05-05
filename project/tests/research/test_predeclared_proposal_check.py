from pathlib import Path

import yaml

from project.research.predeclared import check_proposal_against_registry


def test_check_proposal_against_registry_passes_for_matching_signature(tmp_path: Path) -> None:
    registry = tmp_path / "predeclared.yaml"
    proposal = tmp_path / "proposal.yaml"
    registry.write_text(
        yaml.safe_dump({
            "hypotheses": [{
                "id": "h1",
                "mechanism": "shock continuation",
                "event_id": "VOL_SHOCK",
                "template": "continuation",
                "direction": "long",
                "horizon_bars": 12,
                "symbol": "BTCUSDT",
                "timeframe": "5m",
            }]
        }),
        encoding="utf-8",
    )
    proposal.write_text(
        yaml.safe_dump({
            "symbols": ["BTCUSDT"],
            "timeframe": "5m",
            "hypothesis": {
                "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
                "template": {"id": "continuation"},
                "direction": "long",
                "horizon_bars": 12,
            },
        }),
        encoding="utf-8",
    )
    report = check_proposal_against_registry(registry_path=registry, proposal_path=proposal)
    assert report["status"] == "pass"
    assert report["matches"] == ["h1"]
