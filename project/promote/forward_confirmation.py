from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from project.core.config import get_data_root


@dataclass(frozen=True)
class ForwardConfirmation:
    run_id: str
    confirmed_at: str
    oos_window_start: str
    oos_window_end: str
    metrics: dict[str, float]
    evidence_bundle_path: str = ""

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ForwardConfirmation":
        raw_metrics = payload.get("metrics", {})
        metrics: dict[str, float] = {}
        if isinstance(raw_metrics, Mapping):
            for key, value in raw_metrics.items():
                try:
                    metrics[str(key)] = float(value)
                except (TypeError, ValueError):
                    continue
        return cls(
            run_id=str(payload.get("run_id", "") or ""),
            confirmed_at=str(payload.get("confirmed_at", "") or ""),
            oos_window_start=str(payload.get("oos_window_start", "") or ""),
            oos_window_end=str(payload.get("oos_window_end", "") or ""),
            metrics=metrics,
            evidence_bundle_path=str(payload.get("evidence_bundle_path", "") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "confirmed_at": self.confirmed_at,
            "oos_window_start": self.oos_window_start,
            "oos_window_end": self.oos_window_end,
            "metrics": dict(self.metrics),
            "evidence_bundle_path": self.evidence_bundle_path,
        }


def _confirmation_path(run_id: str, data_root: Path | None = None) -> Path:
    root = Path(data_root) if data_root is not None else get_data_root()
    return root / "reports" / "validation" / str(run_id) / "forward_confirmation.json"


def load_forward_confirmation(run_id: str, data_root: Path | None = None) -> ForwardConfirmation | None:
    path = _confirmation_path(run_id, data_root)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    confirmation = ForwardConfirmation.from_mapping(payload)
    return confirmation if confirmation.run_id else None


def _metric(payload: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        if name not in payload:
            continue
        try:
            value = float(payload[name])
        except (TypeError, ValueError):
            continue
        if value == value:
            return value
    return None


def _sign(value: float | None) -> int:
    if value is None:
        return 0
    return 1 if value > 0 else (-1 if value < 0 else 0)


def validate_forward_confirmation(
    confirmation: ForwardConfirmation | None,
    *,
    candidate_metrics: Mapping[str, Any],
    drift_tolerance: float = 0.70,
) -> tuple[bool, list[str]]:
    """Validate that held-out confirmation is directionally consistent with candidate metrics."""
    if confirmation is None:
        return False, ["forward_confirmation_missing"]

    reasons: list[str] = []
    metrics = confirmation.metrics
    confirmed_t = _metric(metrics, "t_stat_net", "t_stat", "net_t_stat")
    candidate_t = _metric(candidate_metrics, "t_stat_net", "t_stat", "net_t_stat")
    confirmed_mean = _metric(metrics, "mean_return_net_bps", "after_cost_expectancy_bps", "cost_adjusted_return_bps")
    candidate_mean = _metric(candidate_metrics, "mean_return_net_bps", "after_cost_expectancy_bps", "cost_adjusted_return_bps")

    if confirmed_t is None:
        reasons.append("forward_confirmation_missing_t_stat_net")
    if confirmed_mean is None:
        reasons.append("forward_confirmation_missing_mean_return_net_bps")

    if confirmed_t is not None and candidate_t is not None:
        if abs(confirmed_t) < float(drift_tolerance) * abs(candidate_t):
            reasons.append("forward_confirmation_drift")
        if _sign(candidate_t) and _sign(confirmed_t) and _sign(candidate_t) != _sign(confirmed_t):
            reasons.append("forward_confirmation_sign_flip")
    elif confirmed_t is not None and confirmed_t <= 0:
        reasons.append("forward_confirmation_nonpositive_t_stat_net")

    if confirmed_mean is not None and candidate_mean is not None:
        if _sign(candidate_mean) and _sign(confirmed_mean) and _sign(candidate_mean) != _sign(confirmed_mean):
            reasons.append("forward_confirmation_mean_sign_flip")
    elif confirmed_mean is not None and confirmed_mean <= 0:
        reasons.append("forward_confirmation_nonpositive_mean_return_net_bps")

    return not reasons, reasons


__all__ = [
    "ForwardConfirmation",
    "load_forward_confirmation",
    "validate_forward_confirmation",
]
