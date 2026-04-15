import json
from pathlib import Path

import yaml

from project.research.campaign_controller import CampaignConfig, CampaignController


def test_campaign_controller_writes_contract_artifact(tmp_path: Path):
    reg_dir = tmp_path / 'registries'
    reg_dir.mkdir()
    for name, payload in {
        'events.yaml': {'events': {}},
        'templates.yaml': {'templates': {}},
        'contexts.yaml': {'context_dimensions': {}},
        'search_limits.yaml': {'limits': {}},
        'states.yaml': {'states': {}},
        'features.yaml': {'features': {}},
        'detectors.yaml': {'detector_ownership': {}},
    }.items():
        (reg_dir / name).write_text(yaml.dump(payload), encoding='utf-8')
    data_root = tmp_path / 'data'
    data_root.mkdir()

    controller = CampaignController(CampaignConfig(program_id='test_campaign'), data_root, reg_dir)
    payload = json.loads((data_root / 'artifacts' / 'experiments' / 'test_campaign' / 'campaign_contract.json').read_text(encoding='utf-8'))
    assert payload['program_id'] == 'test_campaign'
    assert payload['mode'] == 'autonomous'
