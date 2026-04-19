from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlencode

from project import PROJECT_ROOT
from project.live.deploy_status import reject_synthetic_microstructure_defaults
from project.live.policy import normalize_live_event_detector_config
from project.spec_registry import load_yaml_path


class LiveRuntimeConfigError(ValueError):
    pass


class VenueConnectivityError(RuntimeError):
    pass


def _normalize_runtime_mode(config: Dict[str, Any], *, config_path: Path) -> str:
    runtime_mode = str(config.get("runtime_mode", "")).strip().lower()
    if runtime_mode not in {"monitor_only", "trading", "simulation"}:
        raise LiveRuntimeConfigError(
            "Live engine config must set runtime_mode to 'monitor_only', 'simulation', "
            f"or 'trading': {config_path}"
        )
    return runtime_mode


def _normalize_strategy_runtime(config: Dict[str, Any], *, config_path: Path) -> Dict[str, Any]:
    strategy_runtime = config.get("strategy_runtime", {})
    if strategy_runtime in (None, ""):
        return {}
    if not isinstance(strategy_runtime, dict):
        raise LiveRuntimeConfigError(f"strategy_runtime must be a mapping: {config_path}")
    normalized = dict(strategy_runtime)
    implemented = bool(normalized.get("implemented", False))
    thesis_path = str(normalized.get("thesis_path", "") or "").strip()
    thesis_run_id = str(normalized.get("thesis_run_id", "") or "").strip()
    load_latest_theses = bool(normalized.pop("load_latest_theses", False))

    if load_latest_theses:
        raise LiveRuntimeConfigError(
            "strategy_runtime.load_latest_theses is no longer supported; "
            "set exactly one of strategy_runtime.thesis_path or strategy_runtime.thesis_run_id "
            f"instead: {config_path}"
        )
    if thesis_path and thesis_run_id:
        raise LiveRuntimeConfigError(
            f"strategy_runtime must set only one of thesis_path or thesis_run_id: {config_path}"
        )
    if implemented and not (thesis_path or thesis_run_id):
        raise LiveRuntimeConfigError(
            "strategy_runtime.implemented=true requires explicit thesis input via "
            f"strategy_runtime.thesis_path or strategy_runtime.thesis_run_id: {config_path}"
        )
    if thesis_path:
        normalized["thesis_path"] = thesis_path
    else:
        normalized.pop("thesis_path", None)
    if thesis_run_id:
        normalized["thesis_run_id"] = thesis_run_id
    else:
        normalized.pop("thesis_run_id", None)
    try:
        normalized["event_detector"] = normalize_live_event_detector_config(
            normalized.get("event_detector", {})
        )
    except ValueError as exc:
        raise LiveRuntimeConfigError(f"{exc}: {config_path}") from exc
    return normalized


def load_live_engine_config(path: Path) -> Dict[str, Any]:
    payload = load_yaml_path(path) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Live engine config must be a mapping: {path}")
    config = dict(payload)
    config["runtime_mode"] = _normalize_runtime_mode(config, config_path=path)
    config["strategy_runtime"] = _normalize_strategy_runtime(config, config_path=path)
    try:
        reject_synthetic_microstructure_defaults(
            runtime_mode=str(config["runtime_mode"]),
            strategy_runtime=config["strategy_runtime"],
            config_path=path,
        )
    except ValueError as exc:
        raise LiveRuntimeConfigError(str(exc)) from exc
    return config


def _resolve_live_engine_config(
    *, config_path: Path, config: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    if config is None:
        return load_live_engine_config(config_path)
    if not isinstance(config, dict):
        raise ValueError(f"Live engine config must be a mapping: {config_path}")
    normalized = dict(config)
    normalized["runtime_mode"] = _normalize_runtime_mode(normalized, config_path=config_path)
    normalized["strategy_runtime"] = _normalize_strategy_runtime(
        normalized, config_path=config_path
    )
    try:
        reject_synthetic_microstructure_defaults(
            runtime_mode=str(normalized["runtime_mode"]),
            strategy_runtime=normalized["strategy_runtime"],
            config_path=config_path,
        )
    except ValueError as exc:
        raise LiveRuntimeConfigError(str(exc)) from exc
    return normalized


def resolve_live_engine_session_metadata(
    *,
    config_path: Path,
    config: Dict[str, Any] | None = None,
    symbols: list[str] | None = None,
    snapshot_path: str | None = None,
) -> Dict[str, Any]:
    config = _resolve_live_engine_config(config_path=config_path, config=config)
    resolved_symbols = symbols or [
        str(item.get("symbol", "")).strip().lower()
        for item in list(config.get("freshness_streams", []))
        if str(item.get("symbol", "")).strip()
    ]
    if not resolved_symbols:
        resolved_symbols = ["btcusdt", "ethusdt"]

    resolved_snapshot_path = snapshot_path or str(
        config.get(
            "live_state_snapshot_path", PROJECT_ROOT.parent / "artifacts" / "live_state.json"
        )
    )
    runtime_mode = str(config.get("runtime_mode", "monitor_only")).strip().lower()
    strategy_runtime = config.get("strategy_runtime", {})
    recovery_streak = int(config.get("microstructure_recovery_streak", 3) or 3)
    account_sync_interval_seconds = float(config.get("account_sync_interval_seconds", 30.0) or 30.0)
    account_sync_failure_threshold = int(config.get("account_sync_failure_threshold", 3) or 3)
    execution_degradation_min_samples = int(config.get("execution_degradation_min_samples", 3) or 3)
    execution_degradation_warn_edge_bps = float(
        config.get("execution_degradation_warn_edge_bps", 0.0) or 0.0
    )
    execution_degradation_block_edge_bps = float(
        config.get("execution_degradation_block_edge_bps", -5.0) or -5.0
    )
    execution_degradation_throttle_scale = float(
        config.get("execution_degradation_throttle_scale", 0.5) or 0.5
    )
    stale_threshold_sec = float(config.get("stale_threshold_sec", 60.0) or 60.0)
    return {
        "symbols": list(resolved_symbols),
        "live_state_snapshot_path": str(resolved_snapshot_path),
        "live_state_auto_persist_enabled": bool(resolved_snapshot_path),
        "kill_switch_recovery_streak": recovery_streak,
        "account_sync_interval_seconds": account_sync_interval_seconds,
        "account_sync_failure_threshold": account_sync_failure_threshold,
        "execution_degradation_min_samples": execution_degradation_min_samples,
        "execution_degradation_warn_edge_bps": execution_degradation_warn_edge_bps,
        "execution_degradation_block_edge_bps": execution_degradation_block_edge_bps,
        "execution_degradation_throttle_scale": execution_degradation_throttle_scale,
        "stale_threshold_sec": stale_threshold_sec,
        "runtime_mode": runtime_mode,
        "strategy_runtime_implemented": bool(
            isinstance(strategy_runtime, dict) and strategy_runtime.get("implemented", False)
        ),
        "event_detection_adapter": str(
            (
                strategy_runtime.get("event_detector", {})
                if isinstance(strategy_runtime, dict)
                else {}
            ).get("adapter", "governed_runtime_core")
        ),
    }


def _resolve_runtime_environment(config: Dict[str, Any], *, config_path: Path) -> str:
    order_source = str(config.get("oms_lineage", {}).get("order_source", "")).strip().lower()
    workflow_id = str(config.get("workflow_id", "")).strip().lower()
    stem = config_path.stem.strip().lower()
    if "paper" in order_source or "paper" in workflow_id or "paper" in stem:
        return "paper"
    if "production" in order_source or "production" in workflow_id or "production" in stem:
        return "production"
    return ""


def _normalize_path_for_match(path: str | Path) -> str:
    return str(path).strip().replace("\\", "/")


def _path_matches_expected(actual: str, expected: str | Path) -> bool:
    normalized_actual = _normalize_path_for_match(actual)
    normalized_expected = _normalize_path_for_match(expected)
    return normalized_actual == normalized_expected or normalized_actual.endswith(
        normalized_expected
    )


def validate_live_runtime_environment(
    *,
    config_path: Path,
    config: Dict[str, Any] | None = None,
    snapshot_path: str | None = None,
    environ: Dict[str, str] | None = None,
) -> Dict[str, str]:
    config = _resolve_live_engine_config(config_path=config_path, config=config)
    runtime_mode = str(config.get("runtime_mode", "monitor_only")).strip().lower()
    strategy_runtime = config.get("strategy_runtime", {})
    if runtime_mode == "trading" and not bool(strategy_runtime.get("implemented", False)):
        raise LiveRuntimeConfigError(
            "runtime_mode 'trading' requires strategy_runtime.implemented=true"
        )
    env = dict(environ or os.environ)
    environment_name = _resolve_runtime_environment(config, config_path=config_path)
    if runtime_mode in {"monitor_only", "simulation"}:
        return {
            "environment": environment_name,
            "venue": str(env.get("EDGE_VENUE", "")).strip().lower(),
        }
    if not environment_name:
        raise LiveRuntimeConfigError(
            f"runtime_mode 'trading' requires a resolvable environment name: {config_path}"
        )

    errors: list[str] = []
    edge_environment = str(env.get("EDGE_ENVIRONMENT", "")).strip().lower()
    edge_venue = str(env.get("EDGE_VENUE", "")).strip().lower()
    edge_live_config = str(env.get("EDGE_LIVE_CONFIG", "")).strip()
    resolved_snapshot_path = str(snapshot_path or "").strip()
    edge_live_snapshot_path = str(env.get("EDGE_LIVE_SNAPSHOT_PATH", "")).strip()

    if edge_environment != environment_name:
        errors.append(f"EDGE_ENVIRONMENT must be '{environment_name}'")
    if edge_venue not in {"binance", "bybit"}:
        errors.append("EDGE_VENUE must be 'binance' or 'bybit'")
    if not edge_live_config:
        errors.append("EDGE_LIVE_CONFIG must be set")
    elif not _path_matches_expected(edge_live_config, config_path):
        errors.append(f"EDGE_LIVE_CONFIG must point to {config_path}")
    if not edge_live_snapshot_path:
        errors.append("EDGE_LIVE_SNAPSHOT_PATH must be set")
    elif resolved_snapshot_path and not _path_matches_expected(
        edge_live_snapshot_path, resolved_snapshot_path
    ):
        errors.append(f"EDGE_LIVE_SNAPSHOT_PATH must point to {resolved_snapshot_path}")
    if environment_name == "paper":
        if edge_venue == "bybit":
            if not (
                str(env.get("EDGE_BYBIT_PAPER_API_KEY", "")).strip()
                or str(env.get("EDGE_API_KEY", "")).strip()
            ):
                errors.append("EDGE_BYBIT_PAPER_API_KEY must be set")
            if not (
                str(env.get("EDGE_BYBIT_PAPER_API_SECRET", "")).strip()
                or str(env.get("EDGE_API_SECRET", "")).strip()
            ):
                errors.append("EDGE_BYBIT_PAPER_API_SECRET must be set")
        else:
            if not (
                str(env.get("EDGE_BINANCE_PAPER_API_KEY", "")).strip()
                or str(env.get("EDGE_API_KEY", "")).strip()
            ):
                errors.append("EDGE_BINANCE_PAPER_API_KEY must be set")
            if not (
                str(env.get("EDGE_BINANCE_PAPER_API_SECRET", "")).strip()
                or str(env.get("EDGE_API_SECRET", "")).strip()
            ):
                errors.append("EDGE_BINANCE_PAPER_API_SECRET must be set")
    if environment_name == "production":
        if edge_venue == "bybit":
            if not (
                str(env.get("EDGE_BYBIT_API_KEY", "")).strip()
                or str(env.get("EDGE_API_KEY", "")).strip()
            ):
                errors.append("EDGE_BYBIT_API_KEY must be set")
            if not (
                str(env.get("EDGE_BYBIT_API_SECRET", "")).strip()
                or str(env.get("EDGE_API_SECRET", "")).strip()
            ):
                errors.append("EDGE_BYBIT_API_SECRET must be set")
        else:
            if not (
                str(env.get("EDGE_BINANCE_API_KEY", "")).strip()
                or str(env.get("EDGE_API_KEY", "")).strip()
            ):
                errors.append("EDGE_BINANCE_API_KEY must be set")
            if not (
                str(env.get("EDGE_BINANCE_API_SECRET", "")).strip()
                or str(env.get("EDGE_API_SECRET", "")).strip()
            ):
                errors.append("EDGE_BINANCE_API_SECRET must be set")

    if errors:
        raise LiveRuntimeConfigError("; ".join(errors))

    return {
        "environment": environment_name,
        "venue": edge_venue,
        "config_path": edge_live_config,
        "snapshot_path": edge_live_snapshot_path,
    }


def _build_binance_signed_query(secret: str, params: Dict[str, Any]) -> str:
    query = urlencode([(str(key), str(value)) for key, value in params.items()])
    signature = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def _resolve_binance_api_credentials(environment: Dict[str, str]) -> Dict[str, str]:
    runtime_environment = str(environment.get("environment", "")).strip().lower()
    if runtime_environment == "paper":
        return {
            "base_url": str(os.environ.get("EDGE_BINANCE_PAPER_API_BASE", "")).strip(),
            "api_key": str(os.environ.get("EDGE_BINANCE_PAPER_API_KEY", "")).strip(),
            "api_secret": str(os.environ.get("EDGE_BINANCE_PAPER_API_SECRET", "")).strip(),
            "expected_host": "testnet.binancefuture.com",
        }
    return {
        "base_url": str(os.environ.get("EDGE_BINANCE_API_BASE", "")).strip(),
        "api_key": str(os.environ.get("EDGE_BINANCE_API_KEY", "")).strip(),
        "api_secret": str(os.environ.get("EDGE_BINANCE_API_SECRET", "")).strip(),
        "expected_host": "fapi.binance.com",
    }


def normalize_binance_futures_account_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Binance futures account payload must be a mapping")

    positions: list[Dict[str, Any]] = []
    for raw in list(payload.get("positions", [])):
        quantity = float(raw.get("positionAmt", 0.0) or 0.0)
        if quantity == 0.0:
            continue
        entry_price = float(raw.get("entryPrice", 0.0) or 0.0)
        mark_price = float(raw.get("markPrice", entry_price) or entry_price)
        positions.append(
            {
                "symbol": str(raw.get("symbol", "")).upper(),
                "quantity": quantity,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "unrealized_pnl": float(raw.get("unrealizedProfit", 0.0) or 0.0),
                "liquidation_price": (
                    float(raw["liquidationPrice"])
                    if raw.get("liquidationPrice") not in (None, "")
                    else None
                ),
                "leverage": float(raw.get("leverage", 1.0) or 1.0),
                "margin_type": str(raw.get("marginType", "ISOLATED")).upper(),
            }
        )

    return {
        "wallet_balance": float(payload.get("totalWalletBalance", 0.0) or 0.0),
        "margin_balance": float(payload.get("totalMarginBalance", 0.0) or 0.0),
        "available_balance": float(payload.get("availableBalance", 0.0) or 0.0),
        "exchange_status": "NORMAL",
        "positions": positions,
    }


async def preflight_binance_venue_connectivity(
    *,
    environment: Dict[str, str],
    timeout_seconds: float = 5.0,
    session_factory: Any | None = None,
) -> Dict[str, Any]:
    venue = str(environment.get("venue", "")).strip().lower()
    runtime_environment = str(environment.get("environment", "")).strip().lower()
    if venue != "binance":
        raise VenueConnectivityError(f"Unsupported venue preflight: {venue}")

    credentials = _resolve_binance_api_credentials(environment)
    base_url = credentials["base_url"]
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]
    expected_host = credentials["expected_host"]

    if not base_url:
        raise VenueConnectivityError("Binance API base URL must be set")
    if expected_host not in base_url:
        raise VenueConnectivityError(f"Binance API base URL must target {expected_host}")
    if not api_key or not api_secret:
        raise VenueConnectivityError("Binance API credentials must be set for venue preflight")

    _sf = session_factory if session_factory is not None else _default_aiohttp_session_factory
    params = {"timestamp": int(time.time() * 1000)}
    signed_query = _build_binance_signed_query(api_secret, params)
    ping_url = f"{base_url.rstrip('/')}/fapi/v1/ping"
    account_url = f"{base_url.rstrip('/')}/fapi/v2/account?{signed_query}"
    headers = {"X-MBX-APIKEY": api_key}

    async with _sf(timeout_seconds=timeout_seconds) as session:
        async with session.get(ping_url) as ping_response:
            if int(getattr(ping_response, "status", 0)) != 200:
                raise VenueConnectivityError(
                    f"Binance ping failed with status {getattr(ping_response, 'status', 'unknown')}"
                )
        async with session.get(account_url, headers=headers) as account_response:
            if int(getattr(account_response, "status", 0)) != 200:
                detail = ""
                if hasattr(account_response, "text"):
                    try:
                        detail = await account_response.text()
                    except Exception:
                        detail = ""
                status = getattr(account_response, "status", "unknown")
                raise VenueConnectivityError(
                    f"Binance account preflight failed with status {status}: {detail}".strip()
                )
            payload = await account_response.json()

    return {
        "venue": venue,
        "environment": runtime_environment,
        "api_base": base_url,
        "account_can_trade": bool(payload.get("canTrade", True)),
        "account_type": str(payload.get("accountType", "")),
    }


def validate_binance_account_preflight(preflight: Dict[str, Any]) -> Dict[str, Any]:
    if not bool(preflight.get("account_can_trade", False)):
        raise VenueConnectivityError("Binance account preflight failed: account cannot trade")

    account_type = str(preflight.get("account_type", "")).strip().upper()
    if account_type and "FUTURE" not in account_type:
        raise VenueConnectivityError(
            f"Binance account preflight failed: unexpected account type '{account_type}'"
        )
    return preflight


async def fetch_binance_futures_account_snapshot(
    *,
    environment: Dict[str, str],
    timeout_seconds: float = 5.0,
    session_factory: Any | None = None,
) -> Dict[str, Any]:
    venue = str(environment.get("venue", "")).strip().lower()
    if venue != "binance":
        raise VenueConnectivityError(f"Unsupported venue snapshot fetch: {venue}")

    credentials = _resolve_binance_api_credentials(environment)
    base_url = credentials["base_url"]
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]
    if not base_url or not api_key or not api_secret:
        raise VenueConnectivityError(
            "Binance API credentials must be set for account snapshot fetch"
        )

    _sf = session_factory if session_factory is not None else _default_aiohttp_session_factory
    params = {"timestamp": int(time.time() * 1000)}
    signed_query = _build_binance_signed_query(api_secret, params)
    account_url = f"{base_url.rstrip('/')}/fapi/v2/account?{signed_query}"
    headers = {"X-MBX-APIKEY": api_key}

    async with _sf(timeout_seconds=timeout_seconds) as session:
        async with session.get(account_url, headers=headers) as account_response:
            if int(getattr(account_response, "status", 0)) != 200:
                detail = ""
                if hasattr(account_response, "text"):
                    try:
                        detail = await account_response.text()
                    except Exception:
                        detail = ""
                status = getattr(account_response, "status", "unknown")
                raise VenueConnectivityError(
                    f"Binance account snapshot fetch failed with status {status}: {detail}".strip()
                )
            payload = await account_response.json()

    return normalize_binance_futures_account_snapshot(payload)


def _resolve_bybit_api_credentials(environment: Dict[str, str]) -> Dict[str, str]:
    runtime_environment = str(environment.get("environment", "")).strip().lower()
    if runtime_environment == "paper":
        return {
            "base_url": str(
                os.environ.get("EDGE_BYBIT_PAPER_API_BASE", "https://api-testnet.bybit.com")
            ).strip(),
            "api_key": str(
                os.environ.get("EDGE_BYBIT_PAPER_API_KEY", "") or os.environ.get("EDGE_API_KEY", "")
            ).strip(),
            "api_secret": str(
                os.environ.get("EDGE_BYBIT_PAPER_API_SECRET", "")
                or os.environ.get("EDGE_API_SECRET", "")
            ).strip(),
            "expected_host": "api-testnet.bybit.com",
        }
    return {
        "base_url": str(os.environ.get("EDGE_BYBIT_API_BASE", "https://api.bybit.com")).strip(),
        "api_key": str(
            os.environ.get("EDGE_BYBIT_API_KEY", "") or os.environ.get("EDGE_API_KEY", "")
        ).strip(),
        "api_secret": str(
            os.environ.get("EDGE_BYBIT_API_SECRET", "") or os.environ.get("EDGE_API_SECRET", "")
        ).strip(),
        "expected_host": "api.bybit.com",
    }


def normalize_bybit_account_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Bybit account payload must be a mapping")

    result_list = payload.get("result", {}).get("list", [])
    wallet_balance = 0.0
    available_balance = 0.0
    for account in result_list:
        wallet_balance += float(account.get("totalWalletBalance", 0.0) or 0.0)
        available_balance += float(account.get("totalAvailableBalance", 0.0) or 0.0)

    positions: list[Dict[str, Any]] = []
    for account in result_list:
        for raw in list(account.get("coin", [])):
            qty = float(raw.get("totalPositionMM", 0.0) or 0.0)
            if qty == 0.0:
                continue
            positions.append(
                {
                    "symbol": str(raw.get("coin", "")).upper(),
                    "quantity": qty,
                    "entry_price": 0.0,
                    "mark_price": 0.0,
                    "unrealized_pnl": float(raw.get("unrealisedPnl", 0.0) or 0.0),
                    "liquidation_price": None,
                    "leverage": 1.0,
                    "margin_type": "UNIFIED",
                }
            )

    return {
        "wallet_balance": wallet_balance,
        "margin_balance": wallet_balance,
        "available_balance": available_balance,
        "exchange_status": "NORMAL",
        "positions": positions,
    }


async def preflight_bybit_venue_connectivity(
    *,
    environment: Dict[str, str],
    timeout_seconds: float = 5.0,
    session_factory: Any | None = None,
) -> Dict[str, Any]:
    runtime_environment = str(environment.get("environment", "")).strip().lower()
    credentials = _resolve_bybit_api_credentials(environment)
    base_url = credentials["base_url"]
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]
    expected_host = credentials["expected_host"]

    if not base_url:
        raise VenueConnectivityError("Bybit API base URL must be set")
    if expected_host not in base_url:
        raise VenueConnectivityError(f"Bybit API base URL must target {expected_host}")
    if not api_key or not api_secret:
        raise VenueConnectivityError("Bybit API credentials must be set for venue preflight")

    _sf = session_factory if session_factory is not None else _default_aiohttp_session_factory
    server_time_url = f"{base_url.rstrip('/')}/v5/market/time"
    wallet_url = f"{base_url.rstrip('/')}/v5/account/wallet-balance?accountType=UNIFIED"

    import hashlib
    import hmac as hmac_mod

    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    sign_str = timestamp + api_key + recv_window + "accountType=UNIFIED"
    signature = hmac_mod.new(
        api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }

    async with _sf(timeout_seconds=timeout_seconds) as session:
        async with session.get(server_time_url) as resp:
            if int(getattr(resp, "status", 0)) != 200:
                raise VenueConnectivityError(
                    f"Bybit server time check failed: {getattr(resp, 'status', 'unknown')}"
                )
        async with session.get(wallet_url, headers=headers) as resp:
            if int(getattr(resp, "status", 0)) != 200:
                raise VenueConnectivityError(
                    f"Bybit wallet preflight failed: {getattr(resp, 'status', 'unknown')}"
                )
            payload = await resp.json()

    ret_code = payload.get("retCode", -1)
    if ret_code != 0:
        raise VenueConnectivityError(
            f"Bybit wallet API error: retCode={ret_code} msg={payload.get('retMsg', '')}"
        )

    return {
        "venue": "bybit",
        "environment": runtime_environment,
        "api_base": base_url,
        "account_can_trade": True,
        "account_type": "UNIFIED",
    }


async def fetch_bybit_account_snapshot(
    *,
    environment: Dict[str, str],
    timeout_seconds: float = 5.0,
    session_factory: Any | None = None,
) -> Dict[str, Any]:
    credentials = _resolve_bybit_api_credentials(environment)
    base_url = credentials["base_url"]
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]

    if not base_url or not api_key or not api_secret:
        raise VenueConnectivityError("Bybit API credentials must be set for account snapshot")

    import hashlib
    import hmac as hmac_mod

    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    sign_str = timestamp + api_key + recv_window + "accountType=UNIFIED"
    signature = hmac_mod.new(
        api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }
    wallet_url = f"{base_url.rstrip('/')}/v5/account/wallet-balance?accountType=UNIFIED"

    _sf = session_factory if session_factory is not None else _default_aiohttp_session_factory
    async with _sf(timeout_seconds=timeout_seconds) as session:
        async with session.get(wallet_url, headers=headers) as resp:
            if int(getattr(resp, "status", 0)) != 200:
                raise VenueConnectivityError(
                    f"Bybit account snapshot failed: {getattr(resp, 'status', 'unknown')}"
                )
            payload = await resp.json()

    return normalize_bybit_account_snapshot(payload)


def _default_aiohttp_session_factory(*, timeout_seconds: float):
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    return aiohttp.ClientSession(timeout=timeout)


def _build_trading_order_manager(environment: Dict[str, str]) -> tuple[str, Any | None]:
    from project.live.oms import OrderManager

    venue = str(environment.get("venue", "binance")).strip().lower()
    if venue == "bybit":
        from project.live.bybit_client import BybitDerivativesClient

        credentials = _resolve_bybit_api_credentials(environment)
        if credentials["base_url"] and credentials["api_key"] and credentials["api_secret"]:
            return (
                venue,
                OrderManager(
                    exchange_client=BybitDerivativesClient(
                        credentials["api_key"],
                        credentials["api_secret"],
                        base_url=credentials["base_url"],
                    )
                ),
            )
        return venue, None

    from project.live.binance_client import BinanceFuturesClient

    credentials = _resolve_binance_api_credentials(environment)
    if credentials["base_url"] and credentials["api_key"] and credentials["api_secret"]:
        return (
            "binance",
            OrderManager(
                exchange_client=BinanceFuturesClient(
                    credentials["api_key"],
                    credentials["api_secret"],
                    base_url=credentials["base_url"],
                )
            ),
        )
    return "binance", None


def build_live_runner(
    *,
    config_path: Path,
    config: Dict[str, Any] | None = None,
    symbols: list[str] | None = None,
    snapshot_path: str | None = None,
    environment: Dict[str, str] | None = None,
):
    from project.live.runner import LiveEngineRunner

    config = _resolve_live_engine_config(config_path=config_path, config=config)
    session_metadata = resolve_live_engine_session_metadata(
        config_path=config_path,
        config=config,
        symbols=symbols,
        snapshot_path=snapshot_path,
    )
    order_manager = None
    venue = "binance"
    if environment is not None and session_metadata["runtime_mode"] == "trading":
        venue, order_manager = _build_trading_order_manager(environment)
    return LiveEngineRunner(
        session_metadata["symbols"],
        exchange=venue,
        snapshot_path=session_metadata["live_state_snapshot_path"],
        microstructure_recovery_streak=session_metadata["kill_switch_recovery_streak"],
        account_sync_interval_seconds=session_metadata["account_sync_interval_seconds"],
        account_sync_failure_threshold=session_metadata["account_sync_failure_threshold"],
        execution_degradation_min_samples=session_metadata["execution_degradation_min_samples"],
        execution_degradation_warn_edge_bps=session_metadata["execution_degradation_warn_edge_bps"],
        execution_degradation_block_edge_bps=session_metadata[
            "execution_degradation_block_edge_bps"
        ],
        execution_degradation_throttle_scale=session_metadata[
            "execution_degradation_throttle_scale"
        ],
        stale_threshold_sec=session_metadata["stale_threshold_sec"],
        order_manager=order_manager,
        runtime_mode=session_metadata["runtime_mode"],
        strategy_runtime=dict(config.get("strategy_runtime", {})),
    )


async def _fetch_trading_start_snapshot(environment: Dict[str, str]) -> Dict[str, Any]:
    venue = str(environment.get("venue", "binance")).strip().lower()
    if venue == "bybit":
        await preflight_bybit_venue_connectivity(environment=environment)
        return await fetch_bybit_account_snapshot(environment=environment)

    preflight = await preflight_binance_venue_connectivity(environment=environment)
    validate_binance_account_preflight(preflight)
    return await fetch_binance_futures_account_snapshot(environment=environment)


def configure_runner_for_trading_start(*, runner: Any, environment: Dict[str, str]) -> None:
    venue = str(environment.get("venue", "binance")).strip().lower()
    if venue == "bybit":
        runner.account_snapshot_fetcher = lambda: fetch_bybit_account_snapshot(
            environment=environment
        )
    else:
        runner.account_snapshot_fetcher = lambda: fetch_binance_futures_account_snapshot(
            environment=environment
        )
    initial_account_snapshot = asyncio.run(_fetch_trading_start_snapshot(environment))
    runner.state_store.update_from_exchange_snapshot(initial_account_snapshot)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the live engine with persistent state.")
    parser.add_argument("--config", required=True, help="Live engine config YAML path.")
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols. Defaults to config freshness streams.",
    )
    parser.add_argument(
        "--snapshot_path", default="", help="Path for durable live state snapshot JSON."
    )
    parser.add_argument(
        "--print_session_metadata",
        action="store_true",
        help="Print resolved session metadata as JSON and exit.",
    )
    parser.add_argument(
        "--run_id",
        default="",
        help="Override thesis_run_id in strategy_runtime and enable thesis-driven mode.",
    )
    args = parser.parse_args(argv)

    symbols = [s.strip().lower() for s in str(args.symbols).split(",") if s.strip()]
    resolved_config_path = Path(args.config)
    resolved_snapshot_path = str(args.snapshot_path).strip() or None
    config = load_live_engine_config(resolved_config_path)
    if args.run_id:
        config.setdefault("strategy_runtime", {})["thesis_run_id"] = args.run_id.strip()
        sr = config.setdefault("strategy_runtime", {})
        if sr.get("implemented") is False:
            sr["implemented"] = True
    runtime_mode = str(config.get("runtime_mode", "monitor_only")).strip().lower()

    if args.print_session_metadata:
        print(
            json.dumps(
                resolve_live_engine_session_metadata(
                    config_path=resolved_config_path,
                    config=config,
                    symbols=symbols or None,
                    snapshot_path=resolved_snapshot_path,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    runtime_environment: Dict[str, str] = {}
    if runtime_mode == "trading":
        runtime_environment = validate_live_runtime_environment(
            config_path=resolved_config_path,
            config=config,
            snapshot_path=resolved_snapshot_path,
        )
    runner = build_live_runner(
        config_path=resolved_config_path,
        config=config,
        symbols=symbols or None,
        snapshot_path=resolved_snapshot_path,
        environment=runtime_environment if runtime_mode == "trading" else None,
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        if runtime_mode == "trading":
            configure_runner_for_trading_start(runner=runner, environment=runtime_environment)
        asyncio.run(runner.start())
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
