"""
Centralized column naming registry for the Edge framework.
Ensures consistency across data ingestion, feature engineering, and evaluation.
"""

from __future__ import annotations


class ColumnRegistry:
    @staticmethod
    def event_cols(event_id: str, signal_col: str | None = None) -> list[str]:
        """Returns standard search patterns for event columns."""
        eid = event_id.lower()
        cols = [f"event_{eid}", f"event_flag_{eid}", f"evt_{eid}", eid]
        if signal_col:
            sc = signal_col.lower()
            if sc not in cols:
                cols.insert(0, sc)
        return cols

    @staticmethod
    def event_direction_cols(event_id: str) -> list[str]:
        """Returns standard search patterns for event-direction metadata columns."""
        eid = event_id.lower()
        return [f"evt_direction_{eid}", f"event_direction_{eid}", f"{eid}_direction"]

    @staticmethod
    def state_cols(state_id: str) -> list[str]:
        """Returns standard search patterns for state columns."""
        sid = state_id.lower()
        return [f"state_{sid}", f"ms_{sid}", sid]

    @staticmethod
    def feature_cols(feature_id: str) -> list[str]:
        """Returns standard search patterns for feature columns."""
        fid = feature_id.lower()
        return [fid, f"feat_{fid}", f"feature_{fid}"]

    @staticmethod
    def sequence_cols(sequence_id: str) -> list[str]:
        """Returns standard search patterns for sequence columns."""
        sid = sequence_id.lower()
        return [f"sequence_{sid}", f"seq_{sid}", sid]

    @staticmethod
    def interaction_cols(interaction_id: str) -> list[str]:
        """Returns standard search patterns for interaction columns."""
        iid = interaction_id.lower()
        return [f"interaction_{iid}", f"int_{iid}", iid]
