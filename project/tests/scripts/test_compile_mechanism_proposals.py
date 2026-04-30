from __future__ import annotations

import pytest
import yaml

from project.research.mechanisms import CandidateHypothesis, load_mechanism
from project.scripts.compile_mechanism_proposals import (
    FUNDING_SQUEEZE_EVENT_LIFT_BLOCK_MESSAGE,
    compile_mechanism_proposals,
    main,
)


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


def test_compile_funding_squeeze_fails_without_event_lift_pass(tmp_path):
    with pytest.raises(ValueError, match=FUNDING_SQUEEZE_EVENT_LIFT_BLOCK_MESSAGE):
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            limit=1,
        )


def test_compile_mechanism_proposals_does_not_emit_funding_extreme(tmp_path):
    out_dir = tmp_path / "generated"

    with pytest.raises(ValueError, match=FUNDING_SQUEEZE_EVENT_LIFT_BLOCK_MESSAGE):
        compile_mechanism_proposals(
            mechanism_id="funding_squeeze",
            symbol="BTCUSDT",
            start="2022-01-01",
            end="2024-12-31",
            data_root=tmp_path,
            output_dir=out_dir,
            limit=1,
        )

    assert not out_dir.exists()


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
    assert capsys.readouterr().out.strip() == f"fail: {FUNDING_SQUEEZE_EVENT_LIFT_BLOCK_MESSAGE}"
