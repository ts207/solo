from __future__ import annotations

from typing import Any

import pandas as pd

from project.research.analyzers.base import AnalyzerResult, BaseEventAnalyzer
from project.research.analyzers.edge import EdgeAnalyzer
from project.research.analyzers.integrity import IntegrityAnalyzer
from project.research.analyzers.morphology import MorphologyAnalyzer
from project.research.analyzers.overlap import OverlapAnalyzer
from project.research.analyzers.stability import StabilityAnalyzer


def run_analyzer_suite(
    events: pd.DataFrame,
    *,
    market: pd.DataFrame | None = None,
    reference_events: pd.DataFrame | None = None,
    include_overlap: bool = False,
    **kwargs: Any,
) -> dict[str, AnalyzerResult]:
    results: dict[str, AnalyzerResult] = {}
    analyzers: list[BaseEventAnalyzer] = [
        IntegrityAnalyzer(),
        MorphologyAnalyzer(),
        EdgeAnalyzer(),
        StabilityAnalyzer(),
    ]
    if include_overlap:
        analyzers.append(OverlapAnalyzer())

    current_events = events.copy()
    for analyzer in analyzers:
        extra_kwargs = dict(kwargs)
        if analyzer.name == "overlap":
            result = analyzer.analyze(
                current_events, market=market, reference_events=reference_events, **extra_kwargs
            )
        else:
            result = analyzer.analyze(current_events, market=market, **extra_kwargs)
        results[analyzer.name] = result
        # Stability works better if edge columns are present.
        if analyzer.name == "edge" and "edge_events" in result.tables:
            current_events = result.tables["edge_events"].copy()
    return results


__all__ = [
    "AnalyzerResult",
    "BaseEventAnalyzer",
    "EdgeAnalyzer",
    "IntegrityAnalyzer",
    "MorphologyAnalyzer",
    "OverlapAnalyzer",
    "StabilityAnalyzer",
    "run_analyzer_suite",
]
