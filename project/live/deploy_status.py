from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT
from project.artifacts import promoted_theses_path
from project.core.config import get_data_root

SYNTHETIC_MICROSTRUCTURE_DEFAULT_KEYS: frozenset[str] = frozenset(
    {
        "default_depth_usd",
        "default_tob_coverage",
        "default_expected_cost_bps",
    }
)


def synthetic_microstructure_default_keys(strategy_runtime: Mapping[str, Any]) -> list[str]:
    return sorted(key for key in SYNTHETIC_MICROSTRUCTURE_DEFAULT_KEYS if key in strategy_runtime)


def runtime_allows_synthetic_microstructure_defaults(runtime_mode: str) -> bool:
    return str(runtime_mode or "").strip().lower() in {"monitor_only", "simulation"}


def reject_synthetic_microstructure_defaults(
    *,
    runtime_mode: str,
    strategy_runtime: Mapping[str, Any],
    config_path: Path,
) -> None:
    keys = synthetic_microstructure_default_keys(strategy_runtime)
    if not keys or runtime_allows_synthetic_microstructure_defaults(runtime_mode):
        return
    joined = ", ".join(keys)
    raise ValueError(
        "Trading live engine configs cannot carry synthetic microstructure defaults "
        f"({joined}); use measured venue state or runtime_mode='simulation': {config_path}"
    )


def inspect_deployment(
    run_id: str,
    *,
    data_root: Path | None = None,
    config_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    resolved_data_root = data_root or get_data_root()
    thesis_path = promoted_theses_path(run_id, resolved_data_root)
    thesis_payload = _load_json_object(thesis_path)
    theses = _theses(thesis_payload)
    config = _load_config_for_run(run_id, config_path=config_path)
    strategy_runtime = _mapping(config.get("strategy_runtime"))
    execution_model = _mapping(strategy_runtime.get("execution_model"))
    live_quality_gate = _mapping(strategy_runtime.get("live_quality_gate"))

    thesis_hash = _sha256_file(thesis_path)
    synthetic_default_keys = synthetic_microstructure_default_keys(strategy_runtime)
    runtime_mode = str(config.get("runtime_mode", "") or "")

    return {
        "kind": "deploy_inspect",
        "run_id": str(run_id),
        "data_root": str(resolved_data_root),
        "thesis_batch": {
            "path": str(thesis_path),
            "exists": thesis_path.exists(),
            "schema_version": str(thesis_payload.get("schema_version", "") or ""),
            "run_id": str(thesis_payload.get("run_id", "") or ""),
            "generated_at_utc": str(thesis_payload.get("generated_at_utc", "") or ""),
            "thesis_count": len(theses),
            "active_thesis_count": _count_by(theses, "status", "active"),
            "hash": thesis_hash,
            "hash_short": thesis_hash[:12] if thesis_hash else "",
        },
        "symbols": _symbols(theses),
        "event_families": _event_families(theses),
        "runtime": {
            "config_path": str(config.get("_path", "")),
            "config_exists": bool(config.get("_exists", False)),
            "runtime_mode": runtime_mode,
            "execution_mode": str(config.get("execution_mode", "") or ""),
            "venue": _venue(config, environ=environ),
            "oms_order_source": str(_mapping(config.get("oms_lineage")).get("order_source", "")),
        },
        "strategy_runtime": {
            "implemented": bool(strategy_runtime.get("implemented", False)),
            "auto_submit": bool(strategy_runtime.get("auto_submit", False)),
            "include_pending_theses": bool(strategy_runtime.get("include_pending_theses", False)),
            "thesis_run_id": str(strategy_runtime.get("thesis_run_id", "") or ""),
            "thesis_path": str(strategy_runtime.get("thesis_path", "") or ""),
            "supported_event_families": list(
                strategy_runtime.get(
                    "supported_event_families",
                    strategy_runtime.get("supported_event_ids", []),
                )
                or []
            ),
            "allowed_actions": list(strategy_runtime.get("allowed_actions", []) or []),
            "max_notional_fraction": strategy_runtime.get("max_notional_fraction"),
            "max_spread_bps": strategy_runtime.get("max_spread_bps"),
            "min_depth_usd": strategy_runtime.get("min_depth_usd"),
            "min_tob_coverage": strategy_runtime.get("min_tob_coverage"),
            "execution_model": execution_model,
            "execution_model_family": _execution_model_family(
                execution_model,
                implemented=bool(strategy_runtime.get("implemented", False)),
            ),
            "live_quality_gate": live_quality_gate,
            "kill_on_live_quality_disable": bool(
                strategy_runtime.get("kill_on_live_quality_disable", False)
            ),
            "live_quality_kill_on_disable": bool(
                live_quality_gate.get(
                    "kill_on_disable",
                    strategy_runtime.get("kill_on_live_quality_disable", False),
                )
            ),
            "portfolio_candidate_batch_size": strategy_runtime.get("portfolio_candidate_batch_size"),
            "synthetic_microstructure_defaults_present": synthetic_default_keys,
            "synthetic_microstructure_defaults_allowed": (
                runtime_allows_synthetic_microstructure_defaults(runtime_mode)
            ),
        },
        "risk_caps": _risk_caps(theses),
        "approval_state": _approval_state(theses),
    }


def deployment_status(
    run_id: str,
    *,
    data_root: Path | None = None,
    config_path: Path | None = None,
    snapshot_path: Path | None = None,
    metrics_path: Path | None = None,
) -> dict[str, Any]:
    resolved_data_root = data_root or get_data_root()
    thesis_path = promoted_theses_path(run_id, resolved_data_root)
    thesis_hash = _sha256_file(thesis_path)
    config = _load_config_for_run(run_id, config_path=config_path)

    resolved_snapshot_path = snapshot_path or _config_path_value(
        config.get("live_state_snapshot_path")
    )
    resolved_metrics_path = metrics_path or _config_path_value(
        config.get("runtime_metrics_snapshot_path")
    )
    snapshot = _load_json_object(resolved_snapshot_path) if resolved_snapshot_path else {}
    metrics = _load_json_object(resolved_metrics_path) if resolved_metrics_path else {}
    account = _mapping(snapshot.get("account") or metrics.get("account"))
    positions = list(account.get("positions", []) or [])

    return {
        "kind": "deploy_status",
        "run_id": str(run_id),
        "runtime": {
            "config_path": str(config.get("_path", "")),
            "runtime_mode": str(config.get("runtime_mode", "") or ""),
            "venue": _venue(config),
        },
        "engine_heartbeat": _heartbeat(metrics, resolved_metrics_path),
        "feed_freshness": _feed_freshness(metrics),
        "active_thesis_batch": {
            "path": str(thesis_path),
            "exists": thesis_path.exists(),
            "hash": thesis_hash,
            "hash_short": thesis_hash[:12] if thesis_hash else "",
        },
        "state_sources": {
            "snapshot_path": str(resolved_snapshot_path or ""),
            "snapshot_exists": bool(resolved_snapshot_path and resolved_snapshot_path.exists()),
            "metrics_path": str(resolved_metrics_path or ""),
            "metrics_exists": bool(resolved_metrics_path and resolved_metrics_path.exists()),
        },
        "active_positions": positions,
        "active_position_count": len(positions),
        "active_orders": list(metrics.get("active_orders", []) or []),
        "active_order_count": len(list(metrics.get("active_orders", []) or [])),
        "account": {
            "wallet_balance": account.get("wallet_balance"),
            "margin_balance": account.get("margin_balance"),
            "available_balance": account.get("available_balance"),
            "total_unrealized_pnl": account.get("total_unrealized_pnl"),
            "exchange_status": account.get("exchange_status"),
            "update_time": account.get("update_time"),
        },
        "kill_switch": _mapping(snapshot.get("kill_switch") or metrics.get("kill_switch")),
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {"_read_error": str(path)}
    return payload if isinstance(payload, dict) else {"_schema_error": "not_object"}


def _load_config_for_run(run_id: str, *, config_path: Path | None) -> dict[str, Any]:
    resolved = config_path or _discover_config_path(run_id)
    if resolved is None:
        return {"_path": "", "_exists": False}
    payload = _load_yaml_object(resolved)
    config = dict(payload) if isinstance(payload, dict) else {}
    config["_path"] = str(resolved)
    config["_exists"] = resolved.exists()
    return config


def _discover_config_path(run_id: str) -> Path | None:
    configs_dir = PROJECT_ROOT / "configs"
    preferred = configs_dir / f"live_paper_{run_id}.yaml"
    if preferred.exists():
        return preferred
    for path in sorted(configs_dir.glob("live*.yaml")):
        payload = _load_yaml_object(path)
        if not isinstance(payload, dict):
            continue
        strategy_runtime = _mapping(payload.get("strategy_runtime"))
        if str(strategy_runtime.get("thesis_run_id", "") or "").strip() == str(run_id):
            return path
    return None


def _load_yaml_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError, TypeError, ValueError):
        return {"_read_error": str(path)}
    return payload if isinstance(payload, dict) else {"_schema_error": "not_object"}


def _config_path_value(value: Any) -> Path | None:
    token = str(value or "").strip()
    if not token:
        return None
    path = Path(token)
    return path if path.is_absolute() else PROJECT_ROOT.parent / path


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _execution_model_family(
    execution_model: Mapping[str, Any],
    *,
    implemented: bool,
) -> str:
    configured = str(execution_model.get("cost_model", "") or "").strip().lower()
    if configured in {"execution_simulator_v2", "fill_model_v2"}:
        return "execution_simulator_v2"
    if implemented:
        return "execution_simulator_v2"
    if configured:
        return configured
    return ""


def _theses(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("theses", [])
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _symbols(theses: list[dict[str, Any]]) -> list[str]:
    symbols: set[str] = set()
    for thesis in theses:
        scope = _mapping(thesis.get("symbol_scope"))
        for raw in list(scope.get("symbols", []) or []):
            token = str(raw or "").strip().upper()
            if token:
                symbols.add(token)
        candidate = str(scope.get("candidate_symbol", "") or "").strip().upper()
        if candidate:
            symbols.add(candidate)
        parts = str(thesis.get("thesis_id", "") or "").split("::")
        if len(parts) >= 3 and parts[2].strip():
            symbols.add(parts[2].strip().upper())
    return sorted(symbols)


def _event_families(theses: list[dict[str, Any]]) -> list[str]:
    values: set[str] = set()
    for thesis in theses:
        for key in ("primary_event_id", "event_family"):
            token = str(thesis.get(key, "") or "").strip().upper()
            if token:
                values.add(token)
        requirements = _mapping(thesis.get("requirements"))
        for key in ("trigger_events", "confirmation_events"):
            for raw in list(requirements.get(key, []) or []):
                token = str(raw or "").strip().upper()
                if token:
                    values.add(token)
    return sorted(values)


def _venue(config: Mapping[str, Any], *, environ: Mapping[str, str] | None = None) -> str:
    explicit = str(config.get("venue", "") or "").strip().lower()
    if explicit:
        return explicit
    env = environ or os.environ
    token = str(env.get("EDGE_VENUE", "") or "").strip().lower()
    return token or "unresolved"


def _count_by(theses: list[dict[str, Any]], key: str, expected: str) -> int:
    expected_token = str(expected).strip().lower()
    return sum(1 for thesis in theses if str(thesis.get(key, "")).strip().lower() == expected_token)


def _approval_state(theses: list[dict[str, Any]]) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    approval_required = 0
    approved = 0
    violations: list[str] = []
    for thesis in theses:
        thesis_id = str(thesis.get("thesis_id", "") or "")
        deployment_state = str(thesis.get("deployment_state", "") or "").strip().lower()
        live_approval = _mapping(thesis.get("live_approval"))
        approval_status = str(live_approval.get("live_approval_status", "") or "").strip().lower()
        by_status[approval_status or "missing"] = by_status.get(approval_status or "missing", 0) + 1
        if deployment_state in {"live_eligible", "live_enabled"}:
            approval_required += 1
            if approval_status == "approved":
                approved += 1
            else:
                violations.append(thesis_id)
    return {
        "by_live_approval_status": by_status,
        "approval_required_count": approval_required,
        "approved_required_count": approved,
        "approval_violations": violations,
    }


def _risk_caps(theses: list[dict[str, Any]]) -> dict[str, Any]:
    configured = 0
    missing: list[str] = []
    by_thesis: dict[str, dict[str, Any]] = {}
    for thesis in theses:
        thesis_id = str(thesis.get("thesis_id", "") or "")
        cap = _mapping(thesis.get("cap_profile"))
        has_cap = any(
            float(cap.get(key, 0.0) or 0.0) > 0.0
            for key in ("max_notional", "max_position_notional", "max_daily_loss")
        )
        if has_cap:
            configured += 1
        else:
            missing.append(thesis_id)
        by_thesis[thesis_id] = {
            "configured": has_cap,
            "max_notional": cap.get("max_notional"),
            "max_position_notional": cap.get("max_position_notional"),
            "max_daily_loss": cap.get("max_daily_loss"),
            "max_active_orders": cap.get("max_active_orders"),
            "kill_switch_scope": cap.get("kill_switch_scope"),
        }
    return {
        "configured_count": configured,
        "missing_count": len(missing),
        "missing_thesis_ids": missing,
        "by_thesis": by_thesis,
    }


def _heartbeat(metrics: Mapping[str, Any], metrics_path: Path | None) -> dict[str, Any]:
    generated_at = str(metrics.get("generated_at", "") or "")
    age_seconds = _age_seconds(generated_at)
    if generated_at:
        state = "fresh" if age_seconds is not None and age_seconds <= 180.0 else "stale"
    elif metrics_path and metrics_path.exists():
        state = "missing_generated_at"
    else:
        state = "unavailable"
    return {
        "state": state,
        "generated_at": generated_at,
        "age_seconds": age_seconds,
    }


def _feed_freshness(metrics: Mapping[str, Any]) -> dict[str, Any]:
    market_state = _mapping(metrics.get("latest_market_state_by_symbol"))
    out: dict[str, Any] = {}
    for symbol, raw in market_state.items():
        item = _mapping(raw)
        timestamps = {
            "ticker": str(item.get("ticker_timestamp", "") or ""),
            "funding": str(item.get("funding_timestamp", "") or ""),
            "open_interest": str(item.get("open_interest_timestamp", "") or ""),
        }
        ages = {key: _age_seconds(value) for key, value in timestamps.items() if value}
        stale = [key for key, age in ages.items() if age is None or age > 180.0]
        out[str(symbol).upper()] = {
            "state": "fresh" if ages and not stale else "stale" if ages else "unavailable",
            "timestamps": timestamps,
            "age_seconds": ages,
            "stale_components": stale,
        }
    return out


def _age_seconds(value: str) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds())
