import logging

import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.events.interaction_analyzer import detect_interactions

log = logging.getLogger(__name__)


def build_interactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all registered interaction motifs to the event stream.
    """
    motifs = get_domain_registry().interaction_rows()

    interaction_events = []
    for motif in motifs:
        log.info("Detecting interaction: %s", motif["name"])
        int_df = detect_interactions(
            df, motif["left"], motif["right"], motif["op"], motif.get("lag", 6), motif["name"]
        )
        if not int_df.empty:
            interaction_events.append(int_df)

    if not interaction_events:
        return pd.DataFrame()

    return pd.concat(interaction_events, ignore_index=True)
