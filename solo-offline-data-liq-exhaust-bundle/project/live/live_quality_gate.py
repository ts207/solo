from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class LiveQualityThresholds:
    min_samples: int = 5
    max_slippage_drift_bps: float = 5.0
    disable_slippage_drift_bps: float = 15.0
    min_fill_rate: float = 0.70
    disable_fill_rate: float = 0.40
    max_edge_divergence_bps: float = 10.0
    disable_edge_divergence_bps: float = 25.0
    max_stale_data_frequency: float = 0.05
    disable_stale_data_frequency: float = 0.20
    max_thesis_decay_rate: float = 0.25
    disable_thesis_decay_rate: float = 0.60
    min_risk_scale: float = 0.10


@dataclass(frozen=True)
class LiveQualityGateResult:
    thesis_id: str
    action: str
    risk_scale: float
    reason_codes: tuple[str, ...] = ()
    metrics: dict[str, float] = field(default_factory=dict)

    @property
    def should_disable(self) -> bool:
        return self.action == "disable"

    @property
    def should_downscale(self) -> bool:
        return self.action in {"downscale", "disable"} and self.risk_scale < 1.0


def evaluate_live_quality_gate(
    thesis_id: str,
    metrics: Mapping[str, Any],
    thresholds: LiveQualityThresholds | None = None,
) -> LiveQualityGateResult:
    cfg = thresholds or LiveQualityThresholds()
    sample_count = int(metrics.get("sample_count", metrics.get("fills", 0)) or 0)
    normalized = {
        "sample_count": float(sample_count),
        "slippage_drift_bps": float(metrics.get("slippage_drift_bps", 0.0) or 0.0),
        "fill_rate": float(metrics.get("fill_rate", 1.0) or 0.0),
        "edge_divergence_bps": float(metrics.get("edge_divergence_bps", 0.0) or 0.0),
        "stale_data_frequency": float(metrics.get("stale_data_frequency", 0.0) or 0.0),
        "thesis_decay_rate": float(metrics.get("thesis_decay_rate", 0.0) or 0.0),
    }
    if sample_count < cfg.min_samples:
        return LiveQualityGateResult(
            thesis_id=str(thesis_id),
            action="allow",
            risk_scale=1.0,
            metrics=normalized,
        )

    reasons: list[str] = []
    disable = False
    risk_scale = 1.0

    slippage = normalized["slippage_drift_bps"]
    if slippage >= cfg.disable_slippage_drift_bps:
        disable = True
        reasons.append("slippage_drift_disable")
    elif slippage > cfg.max_slippage_drift_bps:
        reasons.append("slippage_drift")
        risk_scale = min(risk_scale, cfg.max_slippage_drift_bps / max(slippage, 1e-9))

    fill_rate = normalized["fill_rate"]
    if fill_rate <= cfg.disable_fill_rate:
        disable = True
        reasons.append("fill_rate_disable")
    elif fill_rate < cfg.min_fill_rate:
        reasons.append("fill_rate_drift")
        risk_scale = min(risk_scale, fill_rate / max(cfg.min_fill_rate, 1e-9))

    edge_divergence = normalized["edge_divergence_bps"]
    if edge_divergence >= cfg.disable_edge_divergence_bps:
        disable = True
        reasons.append("edge_divergence_disable")
    elif edge_divergence > cfg.max_edge_divergence_bps:
        reasons.append("edge_divergence")
        risk_scale = min(risk_scale, cfg.max_edge_divergence_bps / max(edge_divergence, 1e-9))

    stale_frequency = normalized["stale_data_frequency"]
    if stale_frequency >= cfg.disable_stale_data_frequency:
        disable = True
        reasons.append("stale_data_frequency_disable")
    elif stale_frequency > cfg.max_stale_data_frequency:
        reasons.append("stale_data_frequency")
        risk_scale = min(risk_scale, cfg.max_stale_data_frequency / max(stale_frequency, 1e-9))

    decay_rate = normalized["thesis_decay_rate"]
    if decay_rate >= cfg.disable_thesis_decay_rate:
        disable = True
        reasons.append("thesis_decay_disable")
    elif decay_rate > cfg.max_thesis_decay_rate:
        reasons.append("thesis_decay")
        risk_scale = min(risk_scale, cfg.max_thesis_decay_rate / max(decay_rate, 1e-9))

    if disable:
        return LiveQualityGateResult(
            thesis_id=str(thesis_id),
            action="disable",
            risk_scale=0.0,
            reason_codes=tuple(reasons),
            metrics=normalized,
        )
    if reasons:
        return LiveQualityGateResult(
            thesis_id=str(thesis_id),
            action="downscale",
            risk_scale=max(cfg.min_risk_scale, min(1.0, risk_scale)),
            reason_codes=tuple(reasons),
            metrics=normalized,
        )
    return LiveQualityGateResult(
        thesis_id=str(thesis_id),
        action="allow",
        risk_scale=1.0,
        metrics=normalized,
    )
