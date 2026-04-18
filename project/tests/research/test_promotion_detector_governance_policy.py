from __future__ import annotations

import pandas as pd

from project.research.services.promotion_service import _apply_detector_governance_policy


def test_proxy_detector_is_forced_to_paper_only() -> None:
    promoted = pd.DataFrame([
        {
            'source_event_name': 'LIQUIDATION_CASCADE_PROXY',
            'promotion_class': 'production_promoted',
            'deployment_state': 'live_enabled',
        }
    ])
    out, stats = _apply_detector_governance_policy(promoted)
    assert out.iloc[0]['promotion_class'] == 'paper_promoted'
    assert out.iloc[0]['deployment_state'] == 'paper_only'
    assert stats['paper_only_overrides'] >= 1 or stats['blocked_primary_anchor'] >= 1
