"""Feature and context utilities.

Keep the package root lightweight so importing one detector module does not pull
in the whole feature/event registry graph during test collection.
"""

from __future__ import annotations

__all__ = [
    "FeatureSelectionReport",
    "build_features",
    "calculate_funding_rate_bps",
    "calculate_rv_percentile_24h",
    "is_pit_safe_feature_column",
    "select_model_feature_frame",
    "select_pit_safe_feature_columns",
    "split_feature_columns",
]


def __getattr__(name: str):
    if name == "build_features":
        from project.features.assembly import build_features

        return build_features
    if name == "calculate_funding_rate_bps":
        from project.features.carry_state import calculate_funding_rate_bps

        return calculate_funding_rate_bps
    if name == "calculate_rv_percentile_24h":
        from project.features.vol_regime import calculate_rv_percentile_24h

        return calculate_rv_percentile_24h
    if name in {
        "FeatureSelectionReport",
        "is_pit_safe_feature_column",
        "select_model_feature_frame",
        "select_pit_safe_feature_columns",
        "split_feature_columns",
    }:
        from project.features import event_scoring

        return getattr(event_scoring, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
