from __future__ import annotations

from project.core.coercion import safe_float, safe_int, as_bool
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.research.research_core import (
    CANONICAL_CANDIDATE_COLUMNS,
    ensure_candidate_schema,
    StructuralEdgeComponents,
    structural_edge_components,
    edge_id_from_components,
    edge_id_from_row,
    load_research_features,
    normalize_research_dataframe,
    sparsify_event_mask,
    rolling_z_score,
)

from project.research.analyzers import (
    AnalyzerResult,
    BaseEventAnalyzer,
    EdgeAnalyzer,
    IntegrityAnalyzer,
    MorphologyAnalyzer,
    OverlapAnalyzer,
    StabilityAnalyzer,
    run_analyzer_suite,
)

__all__ = [
    "AnalyzerResult",
    "BaseEventAnalyzer",
    "IntegrityAnalyzer",
    "MorphologyAnalyzer",
    "EdgeAnalyzer",
    "StabilityAnalyzer",
    "OverlapAnalyzer",
    "run_analyzer_suite",
]
