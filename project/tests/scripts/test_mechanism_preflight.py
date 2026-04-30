from __future__ import annotations

import yaml

from project.scripts.compile_mechanism_proposals import compile_mechanism_proposals
from project.scripts.mechanism_preflight import build_preflight_report, main


def test_mechanism_preflight_passes_generated_proposal(tmp_path):
    proposal = compile_mechanism_proposals(
        mechanism_id="forced_flow_reversal",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        limit=1,
    )[0]

    report = build_preflight_report(proposal)

    assert report.status == "pass"
    assert report.classification == "mechanism_backed"
    assert report.mechanism_id == "forced_flow_reversal"
    assert "make discover-proposal" in report.next_safe_command


def test_mechanism_preflight_marks_missing_mechanism_as_scouting(tmp_path):
    proposal = tmp_path / "scouting.yaml"
    proposal.write_text(
        yaml.safe_dump(
            {
                "program_id": "scouting",
                "start": "2022-01-01",
                "end": "2022-12-31",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "PRICE_DOWN_OI_DOWN"},
                    "template": {"id": "mean_reversion"},
                    "direction": "long",
                    "horizon_bars": 24,
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_preflight_report(proposal)

    assert report.status == "warning"
    assert report.classification == "scouting_only"


def test_mechanism_preflight_fails_for_mechanism_violation(tmp_path):
    proposal = compile_mechanism_proposals(
        mechanism_id="forced_flow_reversal",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        limit=1,
    )[0]
    payload = yaml.safe_load(proposal.read_text(encoding="utf-8"))
    payload["hypothesis"]["filters"]["contexts"] = {"vol_regime": ["low"]}
    payload["contexts"] = {"vol_regime": ["low"]}
    proposal.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    report = build_preflight_report(proposal)

    assert report.status == "fail"
    assert report.classification == "mechanism_violation"
    assert main(["--proposal", str(proposal)]) == 1
