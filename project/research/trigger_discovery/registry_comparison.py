from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.events.event_specs import EVENT_REGISTRY_SPECS

log = logging.getLogger(__name__)


def compute_registry_overlaps(
    proposal_mask: pd.Series,
    features: pd.DataFrame
) -> dict[str, Any]:
    """
    Computes overlap metrics between a candidate trigger mask and all available
    canonical events currently instantiated in the features DataFrame.
    """
    if not proposal_mask.any() or features.empty:
        return {
            "nearest_existing_trigger_id": "NONE",
            "registry_similarity_score": 0.0,
            "registry_redundancy_flag": False,
            "novelty_vs_registry_score": 1.0,
        }

    prop_bool = proposal_mask.fillna(False).astype(bool)
    prop_count = prop_bool.sum()

    overlaps = []

    for event_id in EVENT_REGISTRY_SPECS.keys():
        spec_event = EVENT_REGISTRY_SPECS.get(event_id)
        if not spec_event:
            continue

        signal_col = spec_event.signal_column
        cols = ColumnRegistry.event_cols(event_id, signal_col=signal_col)

        for col in cols:
            if col in features.columns:
                ref_mask = features[col].where(features[col].notna(), False).astype(bool)
                ref_count = ref_mask.sum()
                if ref_count == 0:
                    continue

                # Jaccard index
                intersection = (prop_bool & ref_mask).sum()
                union = (prop_bool | ref_mask).sum()
                if union == 0:
                    continue

                jaccard = intersection / union

                # Check for parameter variant matching (near containment)
                containment = intersection / prop_count

                overlaps.append({
                    "event_id": event_id,
                    "jaccard": float(jaccard),
                    "containment": float(containment)
                })

    if not overlaps:
        return {
            "nearest_existing_trigger_id": "NONE",
            "registry_similarity_score": 0.0,
            "registry_redundancy_flag": False,
            "novelty_vs_registry_score": 1.0,
        }

    nearest = max(overlaps, key=lambda x: x["jaccard"])
    max_sim = nearest["jaccard"]

    return {
        "nearest_existing_trigger_id": nearest["event_id"],
        "registry_similarity_score": round(max_sim, 4),
        "registry_redundancy_flag": max_sim > 0.6,
        "novelty_vs_registry_score": round(1.0 - max_sim, 4)
    }
