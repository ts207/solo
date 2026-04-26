# project/research/event_quality/__init__.py
from project.research.event_quality.cooccurrence import compute_cooccurrence
from project.research.event_quality.firing_rate import compute_firing_rates
from project.research.event_quality.information_gain import compute_information_gain
from project.research.event_quality.lead_lag import (
    compute_event_event_lead_lag,
    compute_event_return_lead_lag,
)

__all__ = [
    "compute_cooccurrence",
    "compute_event_event_lead_lag",
    "compute_event_return_lead_lag",
    "compute_firing_rates",
    "compute_information_gain",
]
