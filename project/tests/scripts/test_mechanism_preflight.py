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


def test_mechanism_preflight_rejects_generated_funding_squeeze_until_event_repaired(tmp_path):
    proposal = tmp_path / "funding_squeeze_invalid_event.yaml"
    proposal.write_text(
        yaml.safe_dump(
            {
                "program_id": "funding_squeeze_invalid_event",
                "mechanism": {"id": "funding_squeeze", "version": "v1"},
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "start": "2022-01-01",
                "end": "2024-12-31",
                "contexts": {"carry_state": ["funding_neg"]},
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "FUNDING_EXTREME"},
                    "filters": {"contexts": {"carry_state": ["funding_neg"]}},
                    "template": {"id": "continuation"},
                    "direction": "short",
                    "horizon_bars": 24,
                },
                "required_falsification": [
                    "governed_reproduction",
                    "search_burden",
                    "candidate_traces",
                    "year_pnl_split",
                    "event_only_control",
                    "context_only_control",
                    "opposite_direction_control",
                    "entry_lag_sensitivity",
                    "cost_stress",
                    "forward_confirmation",
                ],
                "forbidden_rescue_actions": [
                    "drop_bad_years_after_result",
                    "change_horizon_after_failure",
                    "switch_direction_after_failure",
                    "loosen_gates",
                    "add_symbols_after_failure",
                    "promote_without_forward_confirmation",
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    report = build_preflight_report(proposal)

    assert report.status == "fail"
    assert report.classification == "mechanism_violation"
    assert report.mechanism_id == "funding_squeeze"
    check = {item.id: item for item in report.checks}["event_in_authoritative_registry"]
    assert check.detail == "FUNDING_EXTREME is not in the authoritative registry"


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
