from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / 'docs' / 'generated'


def main() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    inventory = {
        'schema_version': 'control_plane_inventory_v1',
        'campaign_control': {
            'canonical_controller': 'project.research.campaign_controller.CampaignController',
            'operator_adapter': 'project.operator.campaign_engine.run_campaign',
            'campaign_contract': 'project.research.campaign_contract.CampaignContract',
        },
        'promotion_control': {
            'canonical_service': 'project.research.services.promotion_service.execute_promotion',
            'live_export': 'project.research.live_export.export_promoted_theses_for_run',
            'promoted_thesis_contract': 'project.live.contracts.promoted_thesis.PromotedThesis',
        },
        'planning_control': {
            'campaign_planner': 'project.research.agent_io.campaign_planner.CampaignPlanner',
            'validator': 'project.research.experiment_engine.validate_agent_request',
            'search_intelligence': 'project.research.search_intelligence.update_search_intelligence',
        },
    }
    (DOCS / 'control_plane_inventory.json').write_text(json.dumps(inventory, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    md = [
        '# Control plane inventory',
        '',
        '## Campaign control',
        '- canonical controller: `project.research.campaign_controller.CampaignController`',
        '- operator adapter: `project.operator.campaign_engine.run_campaign`',
        '- campaign contract: `project.research.campaign_contract.CampaignContract`',
        '',
        '## Promotion control',
        '- canonical service: `project.research.services.promotion_service.execute_promotion`',
        '- live export: `project.research.live_export.export_promoted_theses_for_run`',
        '- promoted thesis contract: `project.live.contracts.promoted_thesis.PromotedThesis`',
        '',
        '## Planning control',
        '- planner: `project.research.agent_io.campaign_planner.CampaignPlanner`',
        '- validator: `project.research.experiment_engine.validate_agent_request`',
        '- search intelligence: `project.research.search_intelligence.update_search_intelligence`',
    ]
    (DOCS / 'control_plane_inventory.md').write_text('\n'.join(md) + '\n', encoding='utf-8')

    lineage_md = [
        '# Lineage map',
        '',
        '1. `event contract` -> `campaign planner / controller`',
        '2. `campaign` -> `validated experiment request`',
        '3. `validated experiment request` -> `promotion_service`',
        '4. `promotion_service` -> `evidence_bundles.jsonl`',
        '5. `evidence bundles + promoted candidates` -> `PromotedThesis` export',
        '6. `PromotedThesis` -> `project.live.retriever / decision`',
    ]
    (DOCS / 'lineage_map.md').write_text('\n'.join(lineage_md) + '\n', encoding='utf-8')

    live_surface = {
        'schema_version': 'live_surface_baseline_v1',
        'decision_module': 'project.live.decision',
        'event_detector_module': 'project.live.event_detector',
        'retriever_module': 'project.live.retriever',
        'promoted_thesis_contract': 'project.live.contracts.promoted_thesis.PromotedThesis',
        'runner_module': 'project.live.runner',
        'notes': [
            'Wave 1 keeps live deployment conservative and contract-driven.',
            'Promoted thesis export now includes governance, requirements, and source lineage metadata.',
        ],
    }
    (DOCS / 'live_surface_baseline.json').write_text(json.dumps(live_surface, indent=2, sort_keys=True) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
