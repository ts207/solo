"""
Advanced trigger-discovery lane.

This module provides data-driven routines to identify and propose new
trigger patterns that do not yet exist in the canonical registry.
It supports:
- Parameter sweeps for known detector families (Lane A)
- Excursion / Pattern clustering over arbitrary feature signals (Lane B)

These triggers are output as *proposals*. They do not advance into
promotion or live registry paths automatically.
"""

from project.research.trigger_discovery.candidate_generation import (
    TriggerFeatureColumns,
    TriggerProposal,
)

__all__ = ["TriggerFeatureColumns", "TriggerProposal"]
