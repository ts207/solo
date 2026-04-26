from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import numpy as np

_LOG = logging.getLogger(__name__)


class DataHealthMonitor:
    """
    Supervises data feed freshness and connection heartbeats.
    """

    def __init__(
        self,
        stale_threshold_sec: float = 10.0,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ):
        self.stale_threshold_sec = stale_threshold_sec
        self._now_fn = now_fn or (lambda: datetime.now(UTC))
        self.last_update_times: dict[str, datetime] = {}
        self.registered_stream_times: dict[str, datetime] = {}
        self.status: dict[str, str] = {}  # "HEALTHY" | "STALE" | "DISCONNECTED"

    def register_stream(self, symbol: str, stream: str) -> None:
        key = f"{str(symbol).upper()}:{stream}"
        if key not in self.registered_stream_times:
            self.registered_stream_times[key] = self._now_fn()
            self.status.setdefault(key, "HEALTHY")

    def register_streams(self, streams: list[tuple[str, str]]) -> None:
        for symbol, stream in streams:
            self.register_stream(symbol, stream)

    def on_event(self, symbol: str, stream: str):
        """Record the arrival of a new data event."""
        now = self._now_fn()
        key = f"{str(symbol).upper()}:{stream}"
        self.registered_stream_times.setdefault(key, now)
        self.last_update_times[key] = now
        self.status[key] = "HEALTHY"

    def check_health(self) -> dict[str, Any]:
        """
        Scan all monitored streams and identify stale feeds.
        """
        now = self._now_fn()
        stale_streams = []

        monitored_keys = set(self.registered_stream_times) | set(self.last_update_times)
        for key in monitored_keys:
            last_time = self.last_update_times.get(key)
            baseline = last_time or self.registered_stream_times.get(key)
            if baseline is None:
                continue
            diff = (now - baseline).total_seconds()
            if diff > self.stale_threshold_sec:
                self.status[key] = "STALE" if last_time is not None else "DISCONNECTED"
                stale_streams.append(
                    {
                        "stream": key,
                        "last_seen_sec_ago": float(diff),
                    }
                )
            else:
                self.status[key] = "HEALTHY"

        is_healthy = len(stale_streams) == 0
        max_last_seen_sec_ago = max(
            [float(item["last_seen_sec_ago"]) for item in stale_streams],
            default=0.0,
        )

        if not is_healthy:
            _LOG.warning(f"Data Health Degradation: {len(stale_streams)} stale streams detected.")

        return {
            "is_healthy": bool(is_healthy),
            "freshness_status": "healthy" if is_healthy else "stale",
            "stale_count": len(stale_streams),
            "stale_streams": stale_streams,
            "max_last_seen_sec_ago": float(max_last_seen_sec_ago),
            "timestamp": now.isoformat(),
        }


def check_kill_switch_triggers(
    live_performance_expectancy: float,
    research_mean_expectancy: float,
    max_drawdown_limit: float,
    current_drawdown: float,
    recent_invalidation_streak: int,
    streak_threshold: int = 5,
) -> dict[str, Any]:
    """
    Evaluate kill-switch triggers based on live performance vs research.
    """
    # 1. Expectancy Breach
    baseline_abs = max(abs(research_mean_expectancy), 1e-6)
    if research_mean_expectancy > 0:
        expectancy_ratio = live_performance_expectancy / baseline_abs
        expectancy_kill = expectancy_ratio < 0.5
    elif research_mean_expectancy < 0:
        expectancy_ratio = abs(live_performance_expectancy) / baseline_abs
        expectancy_kill = (
            live_performance_expectancy < research_mean_expectancy and expectancy_ratio > 1.5
        )
    else:
        expectancy_ratio = (
            0.0
            if live_performance_expectancy == 0
            else (float("inf") if live_performance_expectancy > 0 else float("-inf"))
        )
        expectancy_kill = live_performance_expectancy < 0

    # 2. Drawdown Breach
    drawdown_kill = current_drawdown > max_drawdown_limit

    # 3. Invalidation Streak (Repeated failures)
    streak_kill = recent_invalidation_streak >= streak_threshold

    should_kill = expectancy_kill or drawdown_kill or streak_kill

    return {
        "should_kill": bool(should_kill),
        "reasons": [
            "low_expectancy" if expectancy_kill else None,
            "max_drawdown" if drawdown_kill else None,
            "invalidation_streak" if streak_kill else None,
        ],
        "expectancy_ratio": float(expectancy_ratio),
    }


def build_runtime_certification_manifest(
    *,
    postflight_audit: dict[str, Any],
    health_report: dict[str, Any],
    kill_switch_status: dict[str, Any] | None = None,
    oms_lineage: dict[str, Any] | None = None,
    replay_status: dict[str, Any] | None = None,
    live_state_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stale_count = int(health_report.get("stale_count", 0) or 0)
    postflight_status = str(postflight_audit.get("status", "unknown")).strip().lower()
    kill_state = dict(kill_switch_status or {})
    replay_state = dict(replay_status or {})
    oms_state = dict(oms_lineage or {})
    live_state = dict(live_state_status or {})

    certification_checks = {
        "postflight_passed": postflight_status == "pass",
        "feeds_healthy": bool(health_report.get("is_healthy", False)),
        "kill_switch_inactive": not bool(kill_state.get("is_active", False)),
        "oms_lineage_present": bool(oms_state.get("order_source") or oms_state.get("session_id")),
        "live_state_snapshot_present": bool(live_state.get("snapshot_path")),
        "replay_digest_present": bool(
            replay_state.get("replay_digest") or postflight_audit.get("replay_digest")
        ),
    }
    status = "pass" if all(certification_checks.values()) else "failed"
    return {
        "manifest_type": "runtime_certification_manifest",
        "manifest_version": "runtime_certification_manifest_v1",
        "status": status,
        "watermark_status": {
            "status": postflight_status,
            "violation_count": int(postflight_audit.get("watermark_violation_count", 0) or 0),
            "violations_by_type": dict(postflight_audit.get("watermark_violations_by_type", {})),
        },
        "freshness_status": {
            "is_healthy": bool(health_report.get("is_healthy", False)),
            "stale_count": stale_count,
            "stale_streams": list(health_report.get("stale_streams", [])),
        },
        "kill_switch_status": {
            "is_active": bool(kill_state.get("is_active", False)),
            "reason": kill_state.get("reason"),
            "message": str(kill_state.get("message", "")),
        },
        "oms_lineage": oms_state,
        "live_state": {
            "snapshot_path": str(live_state.get("snapshot_path", "")),
            "auto_persist_enabled": bool(live_state.get("auto_persist_enabled", False)),
        },
        "replay_status": {
            "status": str(replay_state.get("status", postflight_status)),
            "replay_digest": str(
                replay_state.get("replay_digest", postflight_audit.get("replay_digest", ""))
            ),
        },
        "certification_checks": certification_checks,
    }


def validate_market_microstructure(
    spread_bps: float,
    max_spread_bps: float,
    liquidity_available: float,
    min_liquidity_usd: float,
) -> dict[str, Any]:
    """
    Validate if current market conditions allow safe trading.
    """
    spread_ok = spread_bps <= max_spread_bps
    liquidity_ok = liquidity_available >= min_liquidity_usd

    return {
        "is_safe": bool(spread_ok and liquidity_ok),
        "spread_ok": bool(spread_ok),
        "liquidity_ok": bool(liquidity_ok),
        "spread_bps": float(spread_bps),
    }


def evaluate_pretrade_microstructure_gate(
    *,
    spread_bps: float | None,
    depth_usd: float | None,
    tob_coverage: float | None,
    max_spread_bps: float,
    min_depth_usd: float,
    min_tob_coverage: float,
) -> dict[str, Any]:
    """
    Hard pre-trade gate for live deployment.

    Trading is blocked when current microstructure is outside the envelope that
    the execution model expects: spread blowouts, depth collapse, or invalid /
    insufficient ToB coverage.
    """
    resolved_spread = float(spread_bps) if spread_bps is not None else float("nan")
    resolved_depth = float(depth_usd) if depth_usd is not None else float("nan")
    resolved_coverage = float(tob_coverage) if tob_coverage is not None else float("nan")

    spread_ok = np.isfinite(resolved_spread) and resolved_spread <= float(max_spread_bps)
    depth_ok = np.isfinite(resolved_depth) and resolved_depth >= float(min_depth_usd)
    coverage_ok = np.isfinite(resolved_coverage) and resolved_coverage >= float(min_tob_coverage)

    reasons: list[str] = []
    if not spread_ok:
        reasons.append("spread_blowout")
    if not depth_ok:
        reasons.append("depth_collapse")
    if not coverage_ok:
        reasons.append("cost_model_invalid")

    return {
        "is_tradable": bool(spread_ok and depth_ok and coverage_ok),
        "reasons": reasons,
        "spread_ok": bool(spread_ok),
        "depth_ok": bool(depth_ok),
        "coverage_ok": bool(coverage_ok),
        "spread_bps": resolved_spread,
        "depth_usd": resolved_depth,
        "tob_coverage": resolved_coverage,
        "max_spread_bps": float(max_spread_bps),
        "min_depth_usd": float(min_depth_usd),
        "min_tob_coverage": float(min_tob_coverage),
    }


def evaluate_market_state_components(
    market_state: dict[str, Any],
    *,
    max_ticker_stale_seconds: float,
    runtime_feature_stale_after_seconds: float,
) -> dict[str, Any]:
    stale_components: list[dict[str, Any]] = []
    missing_components: list[str] = []

    if not bool(market_state.get("ticker_fresh", False)):
        stale_components.append(
            {
                "component": "ticker",
                "age_seconds": market_state.get("ticker_age_seconds"),
                "max_age_seconds": float(max_ticker_stale_seconds),
            }
        )
    for component, fresh_key, age_key in (
        ("funding", "funding_fresh", "funding_age_seconds"),
        ("open_interest", "open_interest_fresh", "open_interest_age_seconds"),
    ):
        source = str(market_state.get(f"{component}_source", "") or "")
        if source == "missing":
            missing_components.append(component)
            continue
        if fresh_key in market_state and not bool(market_state.get(fresh_key, False)):
            stale_components.append(
                {
                    "component": component,
                    "age_seconds": market_state.get(age_key),
                    "max_age_seconds": float(runtime_feature_stale_after_seconds),
                }
            )

    is_healthy = not stale_components and not missing_components
    return {
        "is_healthy": bool(is_healthy),
        "freshness_status": "healthy" if is_healthy else "degraded",
        "stale_components": stale_components,
        "missing_components": missing_components,
    }


def evaluate_live_quality_degradation(
    live_quality_result: dict[str, Any],
) -> dict[str, Any]:
    action = str(live_quality_result.get("action", "allow"))
    degraded = action in {"downscale", "disable"}
    return {
        "is_healthy": not degraded,
        "freshness_status": "degraded" if degraded else "healthy",
        "quality_action": action,
        "risk_scale": float(live_quality_result.get("risk_scale", 1.0)),
        "reason_codes": list(live_quality_result.get("reason_codes", [])),
    }
