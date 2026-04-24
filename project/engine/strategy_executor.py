from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from project.engine.exchange_constraints import apply_constraints, load_symbol_constraints
from project.core.execution_costs import estimate_transaction_cost_bps
from project.engine.execution_model import (
    load_calibration_config,
)
from project.engine.pnl import compute_pnl_ledger
from project.engine.schema import validate_strategy_frame_schema, validate_trace_schema
from project.features.funding_persistence import FP_DEF_VERSION
from project.portfolio import calculate_execution_aware_target_notional
from project.strategy.models.executable_strategy_spec import ExecutableStrategySpec
from project.strategy.runtime import get_strategy

LOGGER = logging.getLogger(__name__)


@dataclass
class StrategyResult:
    name: str
    data: pd.DataFrame
    diagnostics: Dict[str, Any]
    strategy_metadata: Dict[str, Any]
    trace: pd.DataFrame


def build_live_order_metadata(
    result: StrategyResult,
    *,
    timestamp: pd.Timestamp | None = None,
    realized_fee_bps: float = 0.0,
) -> Dict[str, float]:
    frame = result.data
    if frame.empty:
        return {}

    working = frame.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True)
    if timestamp is None:
        row = working.iloc[-1]
    else:
        ts = pd.Timestamp(timestamp)
        ts = ts.tz_convert("UTC") if ts.tz is not None else ts.tz_localize("UTC")
        matched = working.loc[working["timestamp"] == ts]
        if matched.empty:
            raise KeyError(f"timestamp not found in strategy result: {ts}")
        row = matched.iloc[-1]

    expected_entry_price = row.get("fill_price")
    if pd.isna(expected_entry_price):
        expected_entry_price = row.get("close")

    payload = {
        "strategy": str(row.get("strategy", result.name)),
        "signal_timestamp": str(pd.Timestamp(row["timestamp"]).isoformat()),
        "volatility_regime": str(row.get("volatility_regime", "")),
        "microstructure_regime": str(row.get("microstructure_regime", "")),
        "expected_entry_price": float(expected_entry_price or 0.0),
        "expected_return_bps": float(row.get("expected_return_bps", 0.0) or 0.0),
        "expected_adverse_bps": float(row.get("expected_adverse_bps", 0.0) or 0.0),
        "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
        "expected_net_edge_bps": float(row.get("expected_net_edge_bps", 0.0) or 0.0),
        "realized_fee_bps": float(realized_fee_bps),
        "cluster_id": row.get("cluster_id") if not pd.isna(row.get("cluster_id")) else None,
    }
    if payload["expected_entry_price"] <= 0.0:
        return {}
    return payload


def _validate_positions(series: pd.Series, *, allow_continuous: bool = False) -> None:
    if series.index.tz is None:
        raise ValueError("Positions index must be tz-aware UTC timestamps.")
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        raise ValueError("Positions must be numeric.")
    if allow_continuous:
        return
    invalid = ~numeric.isin([-1, 0, 1])
    if invalid.any():
        bad_vals = numeric[invalid].unique()
        raise ValueError(
            f"Positions must be in {{-1,0,1}} unless allow_continuous_position=1. Found: {bad_vals}"
        )


def _is_carry_strategy(strategy_name: str, strategy_metadata: Dict[str, Any]) -> bool:
    family = str(strategy_metadata.get("family", "")).strip().lower()
    if family == "carry":
        return True
    name = str(strategy_name).strip().lower()
    return "carry" in name or "funding_extreme_reversal" in name


def _validated_executable_spec_provenance(params: Dict[str, Any]) -> Dict[str, Any]:
    raw_spec = params.get("executable_strategy_spec")
    if raw_spec is None:
        return {}

    spec = (
        raw_spec
        if isinstance(raw_spec, ExecutableStrategySpec)
        else ExecutableStrategySpec.model_validate(dict(raw_spec))
    )
    return {
        "runtime_provenance_validated": True,
        "runtime_provenance_source": "executable_strategy_spec",
        "proposal_id": str(spec.metadata.proposal_id).strip(),
        "run_id": str(spec.metadata.run_id).strip(),
        "hypothesis_id": str(spec.metadata.hypothesis_id).strip(),
        "candidate_id": str(spec.metadata.candidate_id).strip(),
        "blueprint_id": str(spec.metadata.blueprint_id).strip(),
        "canonical_event_type": str(spec.metadata.canonical_event_type).strip(),
        "canonical_regime": str(spec.metadata.canonical_regime).strip(),
        "routing_profile_id": str(spec.metadata.routing_profile_id).strip(),
        "event_type": str(spec.metadata.event_type).strip(),
        "direction": str(spec.metadata.direction).strip(),
        "retail_profile": str(spec.metadata.retail_profile).strip(),
        "source_path": str(spec.research_origin.source_path).strip(),
        "compiler_version": str(spec.research_origin.compiler_version).strip(),
        "generated_at_utc": str(spec.research_origin.generated_at_utc).strip(),
        "ontology_spec_hash": str(spec.research_origin.ontology_spec_hash).strip(),
        "promotion_track": str(spec.research_origin.promotion_track).strip(),
        "wf_status": str(spec.research_origin.wf_status).strip(),
        "wf_evidence_hash": str(spec.research_origin.wf_evidence_hash).strip(),
        "template_verb": str(spec.research_origin.template_verb).strip(),
    }


def _classify_volatility_regime(vol_bps: float) -> str:
    value = abs(float(vol_bps or 0.0))
    if value >= 50.0:
        return "stressed"
    if value >= 20.0:
        return "elevated"
    return "calm"


def _classify_microstructure_regime(
    *, spread_bps: float, depth_usd: float, tob_coverage: float
) -> str:
    spread = float(spread_bps or 0.0)
    depth = float(depth_usd or 0.0)
    coverage = float(tob_coverage or 0.0)
    if coverage < 0.8 or spread > 10.0 or depth < 25_000.0:
        return "stressed"
    if coverage < 0.95 or spread > 5.0 or depth < 50_000.0:
        return "fragile"
    return "healthy"


def _resolve_execution_aware_scale(
    *,
    params: Dict[str, Any],
    features_aligned: pd.DataFrame,
    bars_indexed: pd.DataFrame,
    close_aligned: pd.Series,
    symbol: str,
    requested_position_scale: float,
) -> tuple[float, Dict[str, float]]:
    if not bool(int(params.get("execution_aware_sizing", 0) or 0)):
        return requested_position_scale, {}

    required = ("event_score", "expected_return_bps", "expected_adverse_bps")
    if any(key not in params for key in required):
        return requested_position_scale, {}

    latest_close = float(close_aligned.dropna().iloc[-1]) if close_aligned.notna().any() else 0.0
    latest_feature_row = (
        features_aligned.loc[features_aligned.notna().any(axis=1)].iloc[-1]
        if not features_aligned.empty and features_aligned.notna().any(axis=1).any()
        else pd.Series(dtype=float)
    )
    latest_bar_row = (
        bars_indexed.loc[bars_indexed.notna().any(axis=1)].iloc[-1]
        if not bars_indexed.empty and bars_indexed.notna().any(axis=1).any()
        else pd.Series(dtype=float)
    )

    liquidity_usd = float(
        latest_feature_row.get(
            "depth_usd",
            latest_feature_row.get("quote_volume", params.get("liquidity_usd", 0.0)),
        )
        or 0.0
    )
    vol_regime = float(
        params.get(
            "vol_regime",
            latest_feature_row.get("vol_regime_bps", latest_feature_row.get("atr_14", 0.0)),
        )
        or 0.0
    )
    market_data = {
        "spread_bps": float(latest_feature_row.get("spread_bps", 0.0) or 0.0),
        "atr_14": latest_feature_row.get("atr_14"),
        "close": latest_close,
        "high": latest_bar_row.get("high", latest_close),
        "low": latest_bar_row.get("low", latest_close),
        "quote_volume": latest_feature_row.get("quote_volume", liquidity_usd),
        "depth_usd": latest_feature_row.get("depth_usd", liquidity_usd),
        "tob_coverage": latest_feature_row.get("tob_coverage", 0.0),
    }
    portfolio_state = {
        "portfolio_value": float(params.get("portfolio_value", 1_000_000.0)),
        "gross_exposure": float(params.get("gross_exposure", 0.0)),
        "max_gross_leverage": float(params.get("max_gross_leverage", 1.0)),
        "target_vol": float(params.get("target_vol", 0.1)),
        "current_vol": float(params.get("current_vol", params.get("target_vol", 0.1))),
        "bucket_exposures": dict(params.get("bucket_exposures", {})),
        "active_cluster_counts": dict(params.get("active_cluster_counts", {})),
    }
    sizing = calculate_execution_aware_target_notional(
        event_score=float(params["event_score"]),
        expected_return_bps=float(params["expected_return_bps"]),
        expected_adverse_bps=float(params["expected_adverse_bps"]),
        vol_regime=vol_regime,
        liquidity_usd=max(liquidity_usd, abs(requested_position_scale) * max(latest_close, 1.0)),
        portfolio_state=portfolio_state,
        symbol=symbol,
        asset_bucket=str(params.get("asset_bucket", "default")),
        cluster_id=params.get("cluster_id"),
        market_data=market_data,
        execution_cost_config=dict(params.get("execution_model", {})),
    )
    resolved_scale = requested_position_scale
    if latest_close > 0.0:
        resolved_scale = float(
            np.sign(requested_position_scale)
            * min(abs(requested_position_scale), sizing["target_notional"] / latest_close)
        )
    diagnostics = {
        "execution_aware_scale": float(resolved_scale),
        "execution_aware_target_notional": float(sizing["target_notional"]),
        "execution_aware_estimated_cost_bps": float(sizing["estimated_execution_cost_bps"]),
        "execution_aware_net_expected_return": float(sizing["net_expected_return"]),
    }
    return resolved_scale, diagnostics


def calculate_strategy_returns(
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    strategy_name: str,
    params: Dict[str, Any],
    cost_bps: float,
    data_root: Path,
    eligibility_mask: pd.Series | None = None,
    calibration_dir: Optional[Path] = None,
) -> StrategyResult:
    strategy = get_strategy(strategy_name)

    positions = strategy.generate_positions(bars, features, params)
    signal_events = positions.attrs.get("signal_events", []) if hasattr(positions, "attrs") else []
    strategy_metadata = (
        positions.attrs.get("strategy_metadata", {}) if hasattr(positions, "attrs") else {}
    )
    strategy_metadata = dict(strategy_metadata)
    strategy_metadata.update(_validated_executable_spec_provenance(params))

    timestamp_index = pd.DatetimeIndex(pd.to_datetime(bars["timestamp"], utc=True))
    allow_continuous_position = bool(int(params.get("allow_continuous_position", 0) or 0))
    positions = pd.to_numeric(positions.reindex(timestamp_index).fillna(0), errors="coerce").fillna(
        0.0
    )
    _validate_positions(positions, allow_continuous=allow_continuous_position)
    if not allow_continuous_position:
        positions = positions.astype(int)

    requested_position_scale = float(params.get("position_scale", 1.0))

    blueprint_delay = int(params.get("delay_bars", 0))
    execution_lag = int(
        params.get("execution_lag_bars", 1) if "execution_lag_bars" in params else 1
    )

    if blueprint_delay > 0 and execution_lag > 0 and not int(params.get("allow_double_lag", 0)):
        raise ValueError(f"Double lag detected for {strategy_name}")

    if execution_lag > 0:
        positions = positions.shift(execution_lag).fillna(0)
        if not allow_continuous_position:
            positions = positions.astype(int)

    strategy_metadata["engine_execution_lag_bars_used"] = execution_lag
    strategy_metadata["strategy_effective_lag_bars"] = blueprint_delay + execution_lag
    strategy_metadata["fp_def_version"] = FP_DEF_VERSION

    entry_reason_map: dict[pd.Timestamp, str] = {}
    exit_reason_map: dict[pd.Timestamp, str] = {}
    for evt in signal_events:
        try:
            ts = pd.Timestamp(evt.get("timestamp"))
            ts = ts.tz_convert("UTC") if ts.tz is not None else ts.tz_localize("UTC")
            reason = str(evt.get("reason", ""))
            if evt.get("event") == "entry":
                entry_reason_map[ts] = reason
            elif evt.get("event") == "exit":
                exit_reason_map[ts] = reason
        except Exception as exc:
            LOGGER.warning("Failed to parse signal event: %s", exc)
            continue

    bars_indexed = bars.copy()
    bars_indexed["timestamp"] = pd.to_datetime(bars_indexed["timestamp"], utc=True)
    bars_indexed = bars_indexed.set_index("timestamp")
    close = pd.to_numeric(bars_indexed["close"], errors="coerce").astype(float)
    open_series = (
        pd.to_numeric(bars_indexed["open"], errors="coerce").astype(float)
        if "open" in bars_indexed.columns
        else None
    )

    if eligibility_mask is not None:
        eligibility_mask = eligibility_mask.reindex(timestamp_index).fillna(False).astype(bool)
        positions = positions.where(eligibility_mask, 0)
        if not allow_continuous_position:
            positions = positions.astype(int)

    execution_cfg = dict(params.get("execution_model", {}))
    if calibration_dir:
        execution_cfg = load_calibration_config(
            symbol, calibration_dir=calibration_dir, base_config=execution_cfg
        )
    exec_mode = str(execution_cfg.get("exec_mode", "close")).strip().lower()

    signal_position = positions.astype(float)

    features_indexed = features.copy()
    features_indexed["timestamp"] = pd.to_datetime(features_indexed["timestamp"], utc=True)
    features_indexed = features_indexed.set_index("timestamp")
    features_aligned = features_indexed.reindex(timestamp_index)

    close_aligned = close.reindex(timestamp_index)
    resolved_position_scale, execution_aware_diag = _resolve_execution_aware_scale(
        params={**params, "execution_model": execution_cfg},
        features_aligned=features_aligned,
        bars_indexed=bars_indexed,
        close_aligned=close_aligned,
        symbol=symbol,
        requested_position_scale=requested_position_scale,
    )
    if execution_aware_diag:
        requested_position_scale = resolved_position_scale
        strategy_metadata.update(execution_aware_diag)

    requested_target_position = signal_position * requested_position_scale
    target_qty = requested_target_position / close_aligned.replace(0.0, np.nan)

    constraints = load_symbol_constraints(
        symbol, meta_dir=data_root / "lake" / "raw" / "binance" / "meta"
    )
    clipped_trades = 0
    if constraints.step_size is not None or constraints.min_notional is not None:
        prior_qty = target_qty.shift(1).fillna(0.0)
        raw_change = target_qty - prior_qty
        mask = (raw_change != 0.0) & close_aligned.notna()
        if mask.any():
            adj_change = raw_change.copy()
            adj_change[mask] = apply_constraints(
                requested_qty=raw_change[mask],
                price=close_aligned[mask],
                constraints=constraints,
            )
            clipped_trades = int((mask & (adj_change == 0.0)).sum())
            executed_qty = prior_qty.copy()
            executed_qty[mask] = prior_qty[mask] + adj_change[mask]
            target_position = (executed_qty * close_aligned).fillna(0.0)
        else:
            target_position = requested_target_position.fillna(0.0)
    else:
        target_position = requested_target_position.fillna(0.0)

    if "base_fee_bps" not in execution_cfg:
        execution_cfg["base_fee_bps"] = float(cost_bps) / 2.0
    if "base_slippage_bps" not in execution_cfg:
        execution_cfg["base_slippage_bps"] = float(cost_bps) / 2.0

    turnover_proxy = (target_position - target_position.shift(1).fillna(0.0)).abs()

    def _to_series(val, idx) -> pd.Series:
        if isinstance(val, pd.Series):
            return pd.to_numeric(val, errors="coerce").reindex(idx).fillna(0.0)
        return pd.Series(float(val) if val is not None else 0.0, index=idx)

    dynamic_cost_bps = estimate_transaction_cost_bps(
        frame=pd.DataFrame(
            {
                "spread_bps": _to_series(features_aligned.get("spread_bps", 0.0), timestamp_index),
                "close": close.reindex(timestamp_index).astype(float),
                "high": _to_series(bars_indexed.get("high", close), timestamp_index),
                "low": _to_series(bars_indexed.get("low", close), timestamp_index),
                "quote_volume": _to_series(
                    features_aligned.get("quote_volume", 0.0), timestamp_index
                ),
                "tob_coverage": _to_series(
                    features_aligned.get("tob_coverage", 0.0), timestamp_index
                ),
                "depth_usd": _to_series(features_aligned.get("depth_usd", 0.0), timestamp_index),
            },
            index=timestamp_index,
        ),
        turnover=turnover_proxy,
        config=execution_cfg,
    )

    use_carry = _is_carry_strategy(strategy_name, strategy_metadata)
    expected_return_bps = float(params.get("expected_return_bps", 0.0) or 0.0)
    expected_adverse_bps = float(params.get("expected_adverse_bps", 0.0) or 0.0)
    expected_cost_series = (
        pd.to_numeric(dynamic_cost_bps, errors="coerce").reindex(timestamp_index).fillna(0.0)
    )
    expected_net_edge_bps = expected_return_bps - expected_adverse_bps - expected_cost_series
    vol_bps_series = pd.to_numeric(
        features_aligned.get("vol_regime_bps", pd.Series(np.nan, index=timestamp_index)),
        errors="coerce",
    ).reindex(timestamp_index)
    atr_series = _to_series(features_aligned.get("atr_14"), timestamp_index)
    close_safe = close.reindex(timestamp_index).replace(0.0, np.nan)
    derived_vol_bps = ((atr_series / close_safe) * 10000.0).replace([np.inf, -np.inf], np.nan)
    vol_bps_series = vol_bps_series.fillna(derived_vol_bps).fillna(0.0)
    spread_series = _to_series(features_aligned.get("spread_bps"), timestamp_index)
    depth_series = _to_series(features_aligned.get("depth_usd"), timestamp_index)
    coverage_series = _to_series(features_aligned.get("tob_coverage"), timestamp_index)
    volatility_regime_series = vol_bps_series.map(_classify_volatility_regime)
    microstructure_regime_series = pd.Series(
        [
            _classify_microstructure_regime(
                spread_bps=float(spread_series.iloc[i]),
                depth_usd=float(depth_series.iloc[i]),
                tob_coverage=float(coverage_series.iloc[i]),
            )
            for i in range(len(timestamp_index))
        ],
        index=timestamp_index,
    )
    funding_rate = (
        _to_series(features_indexed.get("funding_rate_realized", 0.0), timestamp_index)
        if use_carry
        else None
    )

    ledger = compute_pnl_ledger(
        target_position=target_position,
        close=close.reindex(timestamp_index),
        open_=open_series.reindex(timestamp_index) if open_series is not None else None,
        execution_mode=exec_mode,
        cost_bps=dynamic_cost_bps,
        funding_rate=funding_rate,
        capital_base=1.0,
    )

    df = pd.DataFrame(
        {
            "timestamp": timestamp_index,
            "symbol": symbol,
            "strategy": strategy_name,
            "signal_position": signal_position.values,
            "requested_position_scale": float(requested_position_scale),
            "target_position": target_position.values,
            "executed_position": ledger["executed_position"].values,
            "prior_executed_position": ledger["prior_executed_position"].values,
            "fill_mode": ledger["fill_mode"].values,
            "fill_price": ledger["fill_price"].values,
            "mark_price": ledger["mark_price"].values,
            "open": ledger["open"].values,
            "close": ledger["close"].values,
            "bar_return_close_to_close": ledger["bar_return_close_to_close"].values,
            "entry_return_next_open": ledger["entry_return_next_open"].values,
            "holding_return": ledger["holding_return"].values,
            "expected_return_bps": float(expected_return_bps),
            "expected_adverse_bps": float(expected_adverse_bps),
            "expected_cost_bps": expected_cost_series.values,
            "expected_net_edge_bps": expected_net_edge_bps.values,
            "volatility_regime": volatility_regime_series.values,
            "microstructure_regime": microstructure_regime_series.values,
            "turnover": ledger["turnover"].values,
            "gross_pnl": ledger["gross_pnl"].values,
            "transaction_cost": ledger["transaction_cost"].values,
            "slippage_cost": ledger["slippage_cost"].values,
            "funding_pnl": ledger["funding_pnl"].values,
            "borrow_cost": ledger["borrow_cost"].values,
            "net_pnl": ledger["net_pnl"].values,
            "gross_exposure": ledger["gross_exposure"].values,
            "net_exposure": ledger["net_exposure"].values,
            "capital_base": ledger["capital_base"].values,
            "equity_return": ledger["equity_return"].values,
            "entry_reason": [entry_reason_map.get(t, "") for t in timestamp_index],
            "exit_signal_reason": [exit_reason_map.get(t, "") for t in timestamp_index],
            "cluster_id": params.get("cluster_id"),
        }
    )

    validate_strategy_frame_schema(df)

    strategy_metadata["live_order_metadata_template"] = build_live_order_metadata(
        result=StrategyResult(
            strategy_name,
            df,
            {
                "total_bars": len(df),
                "clipped_trades": clipped_trades,
                "allow_continuous_position": allow_continuous_position,
            },
            dict(strategy_metadata),
            pd.DataFrame(),
        )
    )

    trace = df[
        [
            "timestamp",
            "symbol",
            "strategy",
            "signal_position",
            "target_position",
            "executed_position",
            "prior_executed_position",
            "fill_mode",
            "fill_price",
            "mark_price",
            "turnover",
            "gross_pnl",
            "transaction_cost",
            "slippage_cost",
            "funding_pnl",
            "borrow_cost",
            "net_pnl",
        ]
    ].copy()
    trace["state"] = np.where(trace["executed_position"] == 0, "flat", "in_position")
    validate_trace_schema(trace)

    return StrategyResult(
        strategy_name,
        df,
        {
            "total_bars": len(df),
            "clipped_trades": clipped_trades,
            "allow_continuous_position": allow_continuous_position,
        },
        strategy_metadata,
        trace,
    )
