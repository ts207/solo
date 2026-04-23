from __future__ import annotations

import pandas as pd

from project.research.services.evaluation_service import extract_detector_lineage
from project.research.live_export import _resolve_detector_lineage


def test_extract_detector_lineage_from_validation_row() -> None:
    row = pd.Series(
        {
            'event_type': 'VOL_SPIKE',
            'source_event_version': 'v2',
            'source_detector_class': 'VolSpikeDetectorV2',
            'evidence_mode': 'direct',
            'source_threshold_version': '2.0',
        }
    )
    lineage = extract_detector_lineage(row)
    assert lineage['source_event_name'] == 'VOL_SPIKE'
    assert lineage['source_event_version'] == 'v2'
    assert lineage['source_detector_class'] == 'VolSpikeDetectorV2'
    assert lineage['source_evidence_mode'] == 'direct'


def test_live_export_detector_lineage_resolution_prefers_promoted_row() -> None:
    bundle = {'metadata': {'event_type': 'VOL_SPIKE', 'source_event_version': 'v1', 'evidence_mode': 'proxy'}}
    promoted_row = {
        'source_event_name': 'VOL_SPIKE',
        'source_event_version': 'v2',
        'source_detector_class': 'VolSpikeDetectorV2',
        'source_evidence_mode': 'direct',
        'source_threshold_version': '2.0',
    }
    lineage = _resolve_detector_lineage(bundle, promoted_row)
    assert lineage['source_event_version'] == 'v2'
    assert lineage['source_detector_class'] == 'VolSpikeDetectorV2'
    assert lineage['source_evidence_mode'] == 'direct'
