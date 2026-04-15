from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from project.spec_registry import load_yaml_path

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeAlert:
    key: str
    category: str
    severity: str
    message: str
    payload: Dict[str, Any]

    def as_dict(self, *, status: str) -> Dict[str, Any]:
        return {
            "key": self.key,
            "category": self.category,
            "severity": self.severity,
            "status": status,
            "message": self.message,
            "payload": dict(self.payload),
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_runtime_alert_settings(config_path: Path) -> Dict[str, Any]:
    payload = load_yaml_path(config_path) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Live engine config must be a mapping: {config_path}")
    runtime_alerts = payload.get("runtime_alerts", {}) or {}
    if not isinstance(runtime_alerts, dict):
        raise ValueError(f"runtime_alerts must be a mapping: {config_path}")
    strategy_runtime = payload.get("strategy_runtime", {}) or {}
    metrics_path = runtime_alerts.get(
        "metrics_path",
        payload.get("runtime_metrics_snapshot_path") or strategy_runtime.get("runtime_metrics_snapshot_path") or "",
    )
    if not metrics_path:
        raise ValueError(
            f"Live config does not define runtime_metrics_snapshot_path/runtime_alerts.metrics_path: {config_path}"
        )
    return {
        "metrics_path": str(metrics_path),
        "poll_interval_seconds": float(runtime_alerts.get("poll_interval_seconds", 15.0) or 15.0),
        "alert_log_path": str(runtime_alerts.get("alert_log_path", "") or ""),
        "snapshot_max_age_seconds": float(runtime_alerts.get("snapshot_max_age_seconds", 180.0) or 180.0),
        "decision_drought_seconds": float(runtime_alerts.get("decision_drought_seconds", 3600.0) or 3600.0),
        "funding_elevated_abs": float(runtime_alerts.get("funding_elevated_abs", 0.0003) or 0.0003),
        "funding_stretched_abs": float(runtime_alerts.get("funding_stretched_abs", 0.0005) or 0.0005),
        "oi_stable_abs": float(runtime_alerts.get("oi_stable_abs", 0.01) or 0.01),
        "oi_flush_abs": float(runtime_alerts.get("oi_flush_abs", 0.03) or 0.03),
        "ratio_min_total": int(runtime_alerts.get("ratio_min_total", 8) or 8),
        "trade_small_probe_ratio_baseline": float(runtime_alerts.get("trade_small_probe_ratio_baseline", 1.0) or 1.0),
        "trade_small_probe_ratio_tolerance_fraction": float(runtime_alerts.get("trade_small_probe_ratio_tolerance_fraction", 0.5) or 0.5),
    }


def load_runtime_metrics_snapshot(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _symbol_regime(
    state: Mapping[str, Any],
    *,
    funding_elevated_abs: float,
    funding_stretched_abs: float,
    oi_stable_abs: float,
    oi_flush_abs: float,
) -> str:
    funding = float(state.get("funding_rate", 0.0) or 0.0)
    oi_delta = float(state.get("open_interest_delta_fraction", 0.0) or 0.0)

    if abs(funding) >= funding_stretched_abs:
        funding_bucket = "funding_stretched"
    elif abs(funding) >= funding_elevated_abs:
        funding_bucket = "funding_elevated"
    else:
        funding_bucket = "funding_neutral"

    if funding > 0:
        funding_dir = "pos"
    elif funding < 0:
        funding_dir = "neg"
    else:
        funding_dir = "flat"

    if oi_delta <= -oi_flush_abs:
        oi_bucket = "oi_flush"
    elif oi_delta >= oi_flush_abs:
        oi_bucket = "oi_build"
    elif abs(oi_delta) <= oi_stable_abs:
        oi_bucket = "oi_stable"
    else:
        oi_bucket = "oi_mixed"

    return f"{funding_dir}:{funding_bucket}:{oi_bucket}"


def evaluate_runtime_alerts(
    snapshot: Mapping[str, Any],
    *,
    previous_snapshot: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    snapshot_max_age_seconds: float = 180.0,
    decision_drought_seconds: float = 3600.0,
    funding_elevated_abs: float = 0.0003,
    funding_stretched_abs: float = 0.0005,
    oi_stable_abs: float = 0.01,
    oi_flush_abs: float = 0.03,
    ratio_min_total: int = 8,
    trade_small_probe_ratio_baseline: float = 1.0,
    trade_small_probe_ratio_tolerance_fraction: float = 0.5,
) -> List[RuntimeAlert]:
    now = now or _utc_now()
    alerts: List[RuntimeAlert] = []

    generated_at = _parse_timestamp(snapshot.get("generated_at"))
    if generated_at is None:
        alerts.append(
            RuntimeAlert(
                key="snapshot_missing_timestamp",
                category="snapshot",
                severity="critical",
                message="Runtime metrics snapshot is missing a valid generated_at timestamp.",
                payload={"generated_at": snapshot.get("generated_at", "")},
            )
        )
    else:
        age_seconds = max(0.0, (now - generated_at).total_seconds())
        if age_seconds > snapshot_max_age_seconds:
            alerts.append(
                RuntimeAlert(
                    key="snapshot_stale",
                    category="snapshot",
                    severity="critical",
                    message="Runtime metrics snapshot age exceeds the configured threshold.",
                    payload={
                        "snapshot_age_seconds": age_seconds,
                        "snapshot_max_age_seconds": snapshot_max_age_seconds,
                    },
                )
            )

    health = snapshot.get("health", {}) or {}
    stale_count = int(health.get("stale_count", 0) or 0)
    if stale_count > 0 or str(health.get("freshness_status", "healthy")) != "healthy":
        alerts.append(
            RuntimeAlert(
                key="stale_feeds",
                category="health",
                severity="critical",
                message="One or more live data feeds are stale.",
                payload={
                    "stale_count": stale_count,
                    "stale_streams": list(health.get("stale_streams", [])),
                    "max_last_seen_sec_ago": health.get("max_last_seen_sec_ago"),
                },
            )
        )

    kill_switch = snapshot.get("kill_switch", {}) or {}
    if bool(kill_switch.get("is_active", False)):
        alerts.append(
            RuntimeAlert(
                key="kill_switch_active",
                category="kill_switch",
                severity="critical",
                message="Kill-switch is active.",
                payload={
                    "reason": kill_switch.get("reason", ""),
                    "triggered_at": kill_switch.get("triggered_at", ""),
                    "message": kill_switch.get("message", ""),
                },
            )
        )

    strategy_runtime_enabled = bool(snapshot.get("strategy_runtime_enabled", False))
    recent_decisions = list(snapshot.get("recent_decisions", []) or [])
    if strategy_runtime_enabled:
        latest_decision_ts = None
        for item in recent_decisions:
            parsed = _parse_timestamp(item.get("timestamp"))
            if parsed is not None and (latest_decision_ts is None or parsed > latest_decision_ts):
                latest_decision_ts = parsed
        if latest_decision_ts is None:
            alerts.append(
                RuntimeAlert(
                    key="decision_drought",
                    category="decisioning",
                    severity="warning",
                    message="No recent decisions are present in the runtime metrics snapshot.",
                    payload={"decision_drought_seconds": decision_drought_seconds},
                )
            )
        else:
            drought_seconds = max(0.0, (now - latest_decision_ts).total_seconds())
            if drought_seconds > decision_drought_seconds:
                alerts.append(
                    RuntimeAlert(
                        key="decision_drought",
                        category="decisioning",
                        severity="warning",
                        message="Decision generation has gone quiet beyond the configured threshold.",
                        payload={
                            "drought_seconds": drought_seconds,
                            "decision_drought_seconds": decision_drought_seconds,
                            "latest_decision_timestamp": latest_decision_ts.isoformat(),
                        },
                    )
                )

    decision_counts = (snapshot.get("decision_counts", {}) or {}).get("by_action", {}) or {}
    probe_count = int(decision_counts.get("probe", 0) or 0)
    trade_small_count = int(decision_counts.get("trade_small", 0) or 0)
    if (probe_count + trade_small_count) >= int(ratio_min_total):
        if probe_count == 0:
            alerts.append(
                RuntimeAlert(
                    key="trade_small_probe_ratio_drift",
                    category="decision_mix",
                    severity="warning",
                    message="trade_small/probe ratio is undefined because no probe decisions were observed.",
                    payload={
                        "probe_count": probe_count,
                        "trade_small_count": trade_small_count,
                    },
                )
            )
        else:
            observed_ratio = float(trade_small_count) / float(probe_count)
            baseline = max(1e-9, float(trade_small_probe_ratio_baseline))
            tolerance = max(0.0, float(trade_small_probe_ratio_tolerance_fraction))
            lower = baseline * max(0.0, 1.0 - tolerance)
            upper = baseline * (1.0 + tolerance)
            if observed_ratio < lower or observed_ratio > upper:
                alerts.append(
                    RuntimeAlert(
                        key="trade_small_probe_ratio_drift",
                        category="decision_mix",
                        severity="warning",
                        message="trade_small/probe ratio drifted outside the configured band.",
                        payload={
                            "probe_count": probe_count,
                            "trade_small_count": trade_small_count,
                            "observed_ratio": observed_ratio,
                            "baseline_ratio": baseline,
                            "allowed_lower": lower,
                            "allowed_upper": upper,
                        },
                    )
                )

    current_market = snapshot.get("latest_market_state_by_symbol", {}) or {}
    previous_market = (previous_snapshot or {}).get("latest_market_state_by_symbol", {}) or {}
    for symbol, state in sorted(current_market.items()):
        current_regime = _symbol_regime(
            state,
            funding_elevated_abs=funding_elevated_abs,
            funding_stretched_abs=funding_stretched_abs,
            oi_stable_abs=oi_stable_abs,
            oi_flush_abs=oi_flush_abs,
        )
        previous_state = previous_market.get(symbol)
        if not previous_state:
            continue
        previous_regime = _symbol_regime(
            previous_state,
            funding_elevated_abs=funding_elevated_abs,
            funding_stretched_abs=funding_stretched_abs,
            oi_stable_abs=oi_stable_abs,
            oi_flush_abs=oi_flush_abs,
        )
        if previous_regime != current_regime:
            alerts.append(
                RuntimeAlert(
                    key=f"regime_shift::{symbol}",
                    category="regime_shift",
                    severity="info",
                    message=f"Runtime market regime shifted for {symbol}.",
                    payload={
                        "symbol": symbol,
                        "previous_regime": previous_regime,
                        "current_regime": current_regime,
                        "funding_rate": float(state.get("funding_rate", 0.0) or 0.0),
                        "open_interest_delta_fraction": float(state.get("open_interest_delta_fraction", 0.0) or 0.0),
                    },
                )
            )

    return alerts


def _emit_alert_events(
    alerts: Iterable[RuntimeAlert],
    *,
    active_alerts: Dict[str, RuntimeAlert],
    alert_log_path: Path | None,
) -> Dict[str, RuntimeAlert]:
    current = {alert.key: alert for alert in alerts}
    emitted: List[Dict[str, Any]] = []

    for key, alert in current.items():
        if key not in active_alerts:
            emitted.append(alert.as_dict(status="triggered"))
    for key, alert in active_alerts.items():
        if key not in current:
            emitted.append(alert.as_dict(status="resolved"))

    if emitted:
        stamp = _utc_now().isoformat()
        if alert_log_path is not None:
            alert_log_path.parent.mkdir(parents=True, exist_ok=True)
            with alert_log_path.open("a", encoding="utf-8") as handle:
                for item in emitted:
                    handle.write(json.dumps({"generated_at": stamp, **item}, sort_keys=True) + "\n")
        for item in emitted:
            level = logging.INFO
            if item["severity"] == "warning":
                level = logging.WARNING
            elif item["severity"] == "critical":
                level = logging.ERROR
            _LOG.log(level, "%s %s: %s", item["status"].upper(), item["key"], item["message"])
            print(json.dumps({"generated_at": stamp, **item}, sort_keys=True))

    return current


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch live runtime metrics and emit operational alerts.")
    parser.add_argument("--config", type=Path, default=Path("project/configs/live_paper_btc_thesis_v1.yaml"))
    parser.add_argument("--metrics-path", type=Path, default=None)
    parser.add_argument("--alert-log-path", type=Path, default=None)
    parser.add_argument("--poll-interval-seconds", type=float, default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--fail-on-alert", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser


def _resolve_settings(args: argparse.Namespace) -> Dict[str, Any]:
    settings = load_runtime_alert_settings(args.config)
    if args.metrics_path is not None:
        settings["metrics_path"] = str(args.metrics_path)
    if args.alert_log_path is not None:
        settings["alert_log_path"] = str(args.alert_log_path)
    if args.poll_interval_seconds is not None:
        settings["poll_interval_seconds"] = float(args.poll_interval_seconds)
    return settings


def main(argv: List[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    settings = _resolve_settings(args)
    metrics_path = Path(settings["metrics_path"])
    alert_log_path = Path(settings["alert_log_path"]) if settings.get("alert_log_path") else None
    poll_interval = max(1.0, float(settings["poll_interval_seconds"]))

    active_alerts: Dict[str, RuntimeAlert] = {}
    previous_snapshot: Dict[str, Any] | None = None

    while True:
        snapshot = load_runtime_metrics_snapshot(metrics_path)
        alerts = evaluate_runtime_alerts(
            snapshot,
            previous_snapshot=previous_snapshot,
            snapshot_max_age_seconds=float(settings["snapshot_max_age_seconds"]),
            decision_drought_seconds=float(settings["decision_drought_seconds"]),
            funding_elevated_abs=float(settings["funding_elevated_abs"]),
            funding_stretched_abs=float(settings["funding_stretched_abs"]),
            oi_stable_abs=float(settings["oi_stable_abs"]),
            oi_flush_abs=float(settings["oi_flush_abs"]),
            ratio_min_total=int(settings["ratio_min_total"]),
            trade_small_probe_ratio_baseline=float(settings["trade_small_probe_ratio_baseline"]),
            trade_small_probe_ratio_tolerance_fraction=float(settings["trade_small_probe_ratio_tolerance_fraction"]),
        )
        active_alerts = _emit_alert_events(
            alerts,
            active_alerts=active_alerts,
            alert_log_path=alert_log_path,
        )
        previous_snapshot = snapshot
        if args.once:
            if args.fail_on_alert and active_alerts:
                return 1
            return 0
        time.sleep(poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
