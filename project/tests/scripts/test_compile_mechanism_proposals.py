from __future__ import annotations

import json

import pytest
import yaml

from project.research.mechanisms import (
    CandidateHypothesis,
    load_mechanism,
    validate_candidate_against_mechanism,
)
from project.scripts.compile_mechanism_proposals import (
    EVENT_LIFT_TUPLE_REQUIRED_MESSAGE,
    compile_mechanism_proposals,
    main,
)


EVENT_LIFT_TUPLE = {
    "mechanism_id": "funding_squeeze",
    "event_id": "FUNDING_EXTREME_ONSET",
    "regime_id": "vol_regime=high+carry_state=funding_neg",
    "symbol": "BTCUSDT",
    "direction": "long",
    "horizon_bars": 24,
}
COMPILE_EVENT_LIFT_TUPLE = {
    key: EVENT_LIFT_TUPLE[key]
    for key in ("event_id", "regime_id", "direction", "horizon_bars")
}


def _write_event_lift_report(tmp_path, run_id: str, **overrides):
    row = {
        "schema_version": "event_lift_v1",
        "run_id": run_id,
        "scorecard_decision": "allow_event_lift",
        "audit_only": False,
        "promotion_eligible": True,
        "controls": {},
        "lift": {},
        "year_stats": {},
        "max_year_pnl_share": 0.25,
        "mean_net_bps_2x_cost": 4.0,
        "classification": "event_specific",
        "decision": "advance_to_mechanism_proposal",
        "reason": "passes initial event-lift controls",
        **EVENT_LIFT_TUPLE,
    }
    row.update(overrides)
    path = tmp_path / "reports" / "event_lift" / run_id / "event_lift.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"schema_version": "event_lift_v1", "row_count": 1, "rows": [row]}),
        encoding="utf-8",
    )
    return path, row


def test_compile_forced_flow_proposals_is_bounded_and_mechanism_valid(tmp_path):
    paths = compile_mechanism_proposals(
        mechanism_id="forced_flow_reversal",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        limit=3,
    )

    assert len(paths) == 3
    assert [path.name for path in paths] == [
        "forced_flow_oi_flush_highvol_long_h24_btc.yaml",
        "forced_flow_climax_volume_funding_neg_long_h24_btc.yaml",
        "forced_flow_liquidation_exhaustion_highvol_long_h24_btc.yaml",
    ]
    for path in paths:
        assert path.exists()
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        search_spec_path = path.parent / "search_specs" / path.name.replace(".yaml", "_search.yaml")
        search_spec = yaml.safe_load(search_spec_path.read_text(encoding="utf-8"))
        mechanism = load_mechanism(payload["mechanism"]["id"])
        candidate = CandidateHypothesis.from_proposal_payload(payload)
        assert candidate.horizon_bars == 24
        assert candidate.direction == "long"
        assert payload["search_spec"]["path"] == str(search_spec_path)
        assert search_spec["triggers"]["events"] == [candidate.event_id]
        assert search_spec["expression_templates"] == [candidate.template_id]
        assert search_spec["template_policy"]["generic_templates_allowed"] is True
        assert set(mechanism.required_falsification) <= set(payload["required_falsification"])
        assert set(mechanism.forbidden_rescue_actions) <= set(payload["forbidden_rescue_actions"])


def test_compile_limit_prevents_cartesian_expansion(tmp_path):
    paths = compile_mechanism_proposals(
        mechanism_id="forced_flow_reversal",
        symbol="ETHUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        limit=1,
    )

    assert len(paths) == 1
    assert paths[0].name == "forced_flow_oi_flush_highvol_long_h24_eth.yaml"


def test_compile_requires_event_lift_tuple_args_when_required(tmp_path):
    with pytest.raises(ValueError, match=EVENT_LIFT_TUPLE_REQUIRED_MESSAGE):
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            require_event_lift_pass=True,
            limit=1,
        )


def test_compile_fails_without_passing_event_lift_report(tmp_path):
    out_dir = tmp_path / "generated"

    with pytest.raises(ValueError) as excinfo:
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            output_dir=out_dir,
            require_event_lift_pass=True,
            **COMPILE_EVENT_LIFT_TUPLE,
        )

    assert str(excinfo.value) == (
        "no passing event_lift report found for "
        "mechanism=funding_squeeze event=FUNDING_EXTREME_ONSET "
        "regime=vol_regime=high+carry_state=funding_neg symbol=BTCUSDT "
        "direction=long horizon_bars=24"
    )
    assert not out_dir.exists()


def test_compile_rejects_audit_only_event_lift_report(tmp_path):
    _write_event_lift_report(
        tmp_path,
        "audit_run",
        decision="audit_only",
        promotion_eligible=False,
        audit_only=True,
        classification="audit_only",
    )

    with pytest.raises(ValueError) as excinfo:
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            require_event_lift_pass=True,
            event_lift_run_id="audit_run",
            **COMPILE_EVENT_LIFT_TUPLE,
        )

    assert str(excinfo.value) == (
        "event_lift report is not promotable: "
        "decision=audit_only promotion_eligible=false audit_only=true"
    )


def test_compile_rejects_parked_event_lift_report(tmp_path):
    _write_event_lift_report(
        tmp_path,
        "parked_run",
        decision="park",
        promotion_eligible=False,
        audit_only=False,
        classification="insufficient_support",
    )

    with pytest.raises(ValueError) as excinfo:
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            require_event_lift_pass=True,
            event_lift_run_id="parked_run",
            **COMPILE_EVENT_LIFT_TUPLE,
        )

    assert str(excinfo.value) == (
        "event_lift report is not promotable: "
        "decision=park promotion_eligible=false audit_only=false"
    )


def test_compile_requires_template_id_when_mechanism_has_multiple_templates(tmp_path):
    _write_event_lift_report(tmp_path, "passing_run")

    with pytest.raises(
        ValueError,
        match="--template-id is required when mechanism has multiple allowed templates",
    ):
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            require_event_lift_pass=True,
            event_lift_run_id="passing_run",
            **COMPILE_EVENT_LIFT_TUPLE,
        )


def test_compile_writes_evidence_gated_funding_squeeze_proposal(tmp_path):
    _write_event_lift_report(tmp_path, "passing_run")

    paths = compile_mechanism_proposals(
        mechanism_id="funding_squeeze",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        require_event_lift_pass=True,
        event_lift_run_id="passing_run",
        template_id="exhaustion_reversal",
        **COMPILE_EVENT_LIFT_TUPLE,
    )

    assert len(paths) == 1
    assert paths[0].name == "funding_squeeze_funding_extreme_onset_long_h24_btc.yaml"
    assert paths[0].exists()
    search_spec_path = (
        paths[0].parent
        / "search_specs"
        / "funding_squeeze_funding_extreme_onset_long_h24_btc_search.yaml"
    )
    assert search_spec_path.exists()


def test_compile_written_proposal_contains_event_lift_evidence(tmp_path):
    event_lift_path, _ = _write_event_lift_report(tmp_path, "passing_run")

    [path] = compile_mechanism_proposals(
        mechanism_id="funding_squeeze",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        require_event_lift_pass=True,
        event_lift_run_id="passing_run",
        template_id="exhaustion_reversal",
        **COMPILE_EVENT_LIFT_TUPLE,
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["evidence"]["event_lift"] == {
        "run_id": "passing_run",
        "decision": "advance_to_mechanism_proposal",
        "classification": "event_specific",
        "promotion_eligible": True,
        "audit_only": False,
        "regime_id": "vol_regime=high+carry_state=funding_neg",
        "event_id": "FUNDING_EXTREME_ONSET",
    }
    assert payload["artifacts"]["event_lift_path"] == str(event_lift_path)


def test_compile_written_proposal_passes_mechanism_preflight(tmp_path):
    _write_event_lift_report(tmp_path, "passing_run")

    [path] = compile_mechanism_proposals(
        mechanism_id="funding_squeeze",
        symbol="BTCUSDT",
        start="2022-01-01",
        end="2024-12-31",
        data_root=tmp_path,
        require_event_lift_pass=True,
        event_lift_run_id="passing_run",
        template_id="exhaustion_reversal",
        **COMPILE_EVENT_LIFT_TUPLE,
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    mechanism = load_mechanism("funding_squeeze")
    candidate = CandidateHypothesis.from_proposal_payload(payload)
    preflight = validate_candidate_against_mechanism(candidate, mechanism)

    assert preflight.status == "pass"


def test_compile_funding_squeeze_cli_reports_event_lift_gate(tmp_path, capsys):
    rc = main(
        [
            "--mechanism",
            "funding_squeeze",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2022-01-01",
            "--end",
            "2024-12-31",
            "--data-root",
            str(tmp_path),
            "--limit",
            "1",
        ]
    )

    assert rc == 1
    assert capsys.readouterr().out.strip() == f"fail: {EVENT_LIFT_TUPLE_REQUIRED_MESSAGE}"
