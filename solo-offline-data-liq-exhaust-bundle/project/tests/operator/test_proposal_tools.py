from __future__ import annotations

import yaml

import project.operator.proposal_tools as proposal_tools


def test_explain_and_lint_proposal(monkeypatch, tmp_path):
    proposal_path = tmp_path / "proposal.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "btc_campaign",
                "start": "2021-01-01",
                "end": "2021-12-31",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
                    "template": {"id": "mean_reversion"},
                    "direction": "short",
                    "horizon_bars": 12,
                    "entry_lag_bars": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        proposal_tools,
        "translate_and_validate_proposal",
        lambda *args, **kwargs: {
            "experiment_config": {
                "instrument_scope": {"symbols": ["BTCUSDT"], "timeframe": "5m"},
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["long", "short"],
                    "entry_lags": [1],
                },
                "promotion": {"enabled": True, "track": "standard"},
                "bounded": None,
            },
            "validated_plan": {
                "estimated_hypothesis_count": 3,
                "required_detectors": ["VolShockDetector"],
                "required_features": ["ret_5m"],
                "required_states": [],
            },
            "run_all_overrides": {"program_id": "btc_campaign"},
        },
    )
    monkeypatch.setattr(proposal_tools, "validate_bounded_proposal", lambda *args, **kwargs: None)

    explained = proposal_tools.explain_proposal(proposal_path=proposal_path)
    linted = proposal_tools.lint_proposal(proposal_path=proposal_path)

    assert explained["estimated_hypothesis_count"] == 3
    assert explained["required_detectors"] == ["VolShockDetector"]
    assert explained["proposal_format"] == "structured_hypothesis"
    assert linted["status"] == "pass"


def test_explain_proposal_surfaces_normalized_single_hypothesis(monkeypatch, tmp_path):
    proposal_path = tmp_path / "proposal.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "btc_campaign",
                "start": "2021-01-01",
                "end": "2021-12-31",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "instrument_classes": ["crypto"],
                "hypothesis": {"anchor": {"type": "event", "event_id": "VOL_SHOCK"}, "template": {"id": "mean_reversion"}, "direction": "long", "horizon_bars": 12, "entry_lag_bars": 1},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        proposal_tools,
        "translate_and_validate_proposal",
        lambda *args, **kwargs: {
            "experiment_config": {
                "instrument_scope": {"symbols": ["BTCUSDT"], "timeframe": "5m"},
                "templates": {"include": ["continuation"]},
                "evaluation": {
                    "horizons_bars": [24],
                    "directions": ["long"],
                    "entry_lags": [1],
                },
                "promotion": {"enabled": True, "track": "standard"},
                "bounded": None,
            },
            "validated_plan": {
                "estimated_hypothesis_count": 1,
                "required_detectors": ["VolShockDetector"],
                "required_features": ["ret_5m"],
                "required_states": [],
            },
            "run_all_overrides": {"program_id": "btc_campaign"},
        },
    )
    monkeypatch.setattr(proposal_tools, "validate_bounded_proposal", lambda *args, **kwargs: None)

    explained = proposal_tools.explain_proposal(proposal_path=proposal_path)

    assert explained["proposal_format"] == "structured_hypothesis"
