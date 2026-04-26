from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class EventRecord:
    """
    Canonical event record introduced in milestone 1.

    This is the target schema for detector and analyzer migration work.
    Existing pipelines may still emit legacy shapes; this dataclass provides
    the normalized contract that future emitters should satisfy.
    """

    event_id: str
    event_family: str
    event_type: str
    observable_type: str
    interpretation: str

    asset: str
    bar_type: str

    eval_bar_ts: Any
    detected_ts: Any
    signal_ts: Any

    intensity: float
    severity: int

    episode_id: str | None = None
    attribution_id: str | None = None
    detector_version: str = "v1"
    event_version: str = "v1"
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CANONICAL_EVENT_FIELDS = tuple(EventRecord.__dataclass_fields__.keys())
