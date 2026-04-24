from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.pit import PITValidationError, validate_event_frame_pit
from project.events.validate import validate_event_frame_columns
from project.research.promotion_gates_v2 import (
    PromotionDecision,
    evaluate_family_promotion,
)


class PipelineEnforcementError(RuntimeError):
    pass


def enforce_event_pipeline_contract(
    events: pd.DataFrame,
    *,
    analyzer_results: dict[str, Any] | None = None,
    require_promotion: bool = False,
    min_events: int = 1,
) -> PromotionDecision | None:
    frame = events if isinstance(events, pd.DataFrame) else pd.DataFrame()
    if frame.empty:
        if min_events > 0:
            raise PipelineEnforcementError("event frame is empty")
        return None
    try:
        validate_event_frame_columns(frame.columns)
    except Exception as exc:  # noqa: BLE001
        raise PipelineEnforcementError(str(exc)) from exc
    try:
        validate_event_frame_pit(frame)
    except PITValidationError as exc:
        raise PipelineEnforcementError(str(exc)) from exc
    if len(frame) < int(min_events):
        raise PipelineEnforcementError(
            f"event count below minimum: {len(frame)} < {int(min_events)}"
        )
    if analyzer_results is None:
        return None
    decision = evaluate_family_promotion(analyzer_results, min_events=max(int(min_events), 1))
    if require_promotion and not decision.passed:
        raise PipelineEnforcementError("; ".join(decision.reasons) or "promotion gate failed")
    return decision
