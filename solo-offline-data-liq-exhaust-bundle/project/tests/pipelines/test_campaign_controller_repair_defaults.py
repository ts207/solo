import yaml

from project.research.campaign_controller import CampaignController, CampaignConfig


def _make_controller(tmp_path):
    reg_dir = tmp_path / "registries"
    reg_dir.mkdir()
    (reg_dir / "events.yaml").write_text(
        yaml.dump({
            "events": {
                "ALPHA_EVENT": {"enabled": True, "family": "A", "instrument_classes": ["crypto"]},
                "VOL_SHOCK": {"enabled": True, "family": "VOL", "instrument_classes": ["crypto"]},
                "ZSCORE_STRETCH": {"enabled": True, "family": "STAT", "instrument_classes": ["crypto"]},
            }
        }),
        encoding="utf-8",
    )
    (reg_dir / "templates.yaml").write_text(
        yaml.dump({"templates": {"mean_reversion": {"enabled": True, "supports_trigger_types": ["EVENT"]}}}),
        encoding="utf-8",
    )
    (reg_dir / "contexts.yaml").write_text(yaml.dump({"context_dimensions": {}}), encoding="utf-8")
    (reg_dir / "search_limits.yaml").write_text(yaml.dump({"limits": {}}), encoding="utf-8")
    (reg_dir / "states.yaml").write_text(yaml.dump({"states": {}}), encoding="utf-8")
    (reg_dir / "features.yaml").write_text(yaml.dump({"features": {}}), encoding="utf-8")
    (reg_dir / "detectors.yaml").write_text(yaml.dump({"detector_ownership": {}}), encoding="utf-8")
    data_root = tmp_path / "data"
    data_root.mkdir()
    return CampaignController(CampaignConfig(program_id="repair_defaults"), data_root, reg_dir)


def test_phase2_search_engine_stage_uses_canonical_repair_default(tmp_path):
    controller = _make_controller(tmp_path)
    proposal = controller._step_repair(
        {
            "latest_reflection": {},
            "superseded_stages": set(),
            "next_actions": {
                "repair": [
                    {
                        "stage": "phase2_search_engine",
                        "failure_detail": "",
                        "proposed_scope": {"stage": "phase2_search_engine"},
                    }
                ]
            },
        }
    )
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]


def test_repair_global_fallback_prefers_vol_shock_over_hardcoded_stat_event(tmp_path):
    controller = _make_controller(tmp_path)
    proposal = controller._step_repair(
        {
            "latest_reflection": {},
            "superseded_stages": set(),
            "next_actions": {
                "repair": [
                    {
                        "stage": "unknown_stage",
                        "failure_detail": "",
                        "proposed_scope": {"stage": "unknown_stage"},
                    }
                ]
            },
        }
    )
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]
