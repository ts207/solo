from __future__ import annotations
from project.core.config import get_data_root

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from project.engine.data_loader import load_symbol_raw_data, load_universe_snapshots
from project.engine.context_assembler import assemble_symbol_context
from project.engine.strategy_executor import calculate_strategy_returns, StrategyResult
from project.engine.portfolio_aggregator import (
    aggregate_strategy_results,
    build_strategy_contributions,
    build_symbol_contributions,
    combine_strategy_symbols,
)
from project.engine.reporting_summarizer import (
    summarize_pnl,
    summarize_portfolio_ledger,
    entry_count,
)
from project.engine.schema import (
    PORTFOLIO_FRAME_SCHEMA_VERSION,
    STRATEGY_FRAME_SCHEMA_VERSION,
    TRACE_SCHEMA_VERSION,
    validate_portfolio_frame_schema,
    validate_strategy_frame_schema,
    validate_trace_schema,
)
from project.engine.pnl import compute_pnl_ledger
from project.engine.risk_allocator import (
    AllocationContract,
    allocate_position_details,
    build_allocation_contract,
)
from project.events.registry import load_registry_flags, build_event_feature_frame
from project.core.constants import BARS_PER_YEAR_BY_TIMEFRAME
from project.io.utils import ensure_dir
from project.engine.artifacts import (
    build_engine_run_manifest,
    write_engine_dataframe,
    write_engine_run_manifest,
)
from project.strategy.runtime import get_strategy, is_dsl_strategy

BARS_PER_YEAR = BARS_PER_YEAR_BY_TIMEFRAME
LOGGER = logging.getLogger(__name__)
_DEFAULT_TIMEFRAME = "5m"


def _spec_metadata_payload(raw_spec: Any) -> Dict[str, Any]:
    if raw_spec is None:
        return {}
    if hasattr(raw_spec, "model_dump"):
        payload = raw_spec.model_dump()
    elif isinstance(raw_spec, dict):
        payload = dict(raw_spec)
    else:
        return {}
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    research_origin = (
        payload.get("research_origin", {}) if isinstance(payload.get("research_origin"), dict) else {}
    )
    return {
        "proposal_id": str(metadata.get("proposal_id", "")).strip(),
        "run_id": str(metadata.get("run_id", "")).strip(),
        "hypothesis_id": str(metadata.get("hypothesis_id", "")).strip(),
        "candidate_id": str(metadata.get("candidate_id", "")).strip(),
        "blueprint_id": str(metadata.get("blueprint_id", "")).strip(),
        "canonical_event_type": str(metadata.get("canonical_event_type", "")).strip(),
        "canonical_regime": str(metadata.get("canonical_regime", "")).strip(),
        "routing_profile_id": str(metadata.get("routing_profile_id", "")).strip(),
        "ontology_spec_hash": str(research_origin.get("ontology_spec_hash", "")).strip(),
        "promotion_track": str(research_origin.get("promotion_track", "")).strip(),
        "wf_status": str(research_origin.get("wf_status", "")).strip(),
        "wf_evidence_hash": str(research_origin.get("wf_evidence_hash", "")).strip(),
        "source_path": str(research_origin.get("source_path", "")).strip(),
        "compiler_version": str(research_origin.get("compiler_version", "")).strip(),
    }


def run_engine_for_specs(
    run_id: str,
    symbols: List[str],
    strategy_specs: Iterable[Any],
    *,
    cost_bps: float,
    data_root: Path | None = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    timeframe: str = _DEFAULT_TIMEFRAME,
    memory_efficient: bool = True,
) -> Dict[str, object]:
    strategies: list[str] = []
    params_by_strategy: Dict[str, Dict[str, object]] = {}
    for idx, spec in enumerate(strategy_specs):
        metadata = _spec_metadata_payload(spec)
        suffix = str(metadata.get("blueprint_id") or idx).strip() or str(idx)
        strategy_name = f"dsl_interpreter_v1__{suffix}"
        strategies.append(strategy_name)
        params_by_strategy[strategy_name] = {"executable_strategy_spec": spec}
    return run_engine(
        run_id=run_id,
        symbols=symbols,
        strategies=strategies,
        params={},
        cost_bps=cost_bps,
        data_root=data_root,
        params_by_strategy=params_by_strategy,
        start_ts=start_ts,
        end_ts=end_ts,
        timeframe=timeframe,
        memory_efficient=memory_efficient,
    )


def _load_symbol_data(
    data_root: Path, symbol: str, run_id: str, timeframe: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return load_symbol_raw_data(data_root, symbol, run_id, timeframe)


def _strategy_returns(
    symbol: str,
    bars: pd.DataFrame,
    features: pd.DataFrame,
    strategy_name: str,
    strategy_params: dict,
    cost_bps: float,
    data_root: Path,
    **kwargs,
) -> StrategyResult:
    return calculate_strategy_returns(
        symbol, bars, features, strategy_name, strategy_params, cost_bps, data_root, **kwargs
    )


def _infer_bps_from_cost(turnover: pd.Series, cost: pd.Series) -> pd.Series:
    idx = turnover.index
    turnover_aligned = pd.to_numeric(turnover, errors="coerce").reindex(idx).fillna(0.0).abs()
    cost_aligned = pd.to_numeric(cost, errors="coerce").reindex(idx).fillna(0.0).abs()
    out = pd.Series(0.0, index=idx, dtype=float)
    mask = turnover_aligned > 0.0
    out.loc[mask] = (cost_aligned.loc[mask] / turnover_aligned.loc[mask]) * 10000.0
    return out


def _infer_funding_rate(executed_position: pd.Series, funding_pnl: pd.Series) -> pd.Series:
    idx = executed_position.index
    executed = pd.to_numeric(executed_position, errors="coerce").reindex(idx).fillna(0.0)
    funding = pd.to_numeric(funding_pnl, errors="coerce").reindex(idx).fillna(0.0)
    out = pd.Series(0.0, index=idx, dtype=float)
    mask = executed != 0.0
    out.loc[mask] = -(funding.loc[mask] / executed.loc[mask])
    return out


def _infer_borrow_rate(executed_position: pd.Series, borrow_cost: pd.Series) -> pd.Series:
    idx = executed_position.index
    executed = pd.to_numeric(executed_position, errors="coerce").reindex(idx).fillna(0.0)
    borrow = pd.to_numeric(borrow_cost, errors="coerce").reindex(idx).fillna(0.0).abs()
    short_notional = executed.clip(upper=0.0).abs()
    out = pd.Series(0.0, index=idx, dtype=float)
    mask = short_notional > 0.0
    out.loc[mask] = borrow.loc[mask] / short_notional.loc[mask]
    return out


def _rebuild_frame_after_allocation(
    frame: pd.DataFrame,
    *,
    allocation_scale: pd.Series,
    clip_reason: pd.Series,
) -> pd.DataFrame:
    rebuilt = (
        frame.copy()
        .sort_values([col for col in ["timestamp", "symbol", "strategy"] if col in frame.columns])
        .reset_index(drop=True)
    )
    ts = pd.to_datetime(rebuilt["timestamp"], utc=True)
    scale_aligned = ts.map(allocation_scale).fillna(1.0).astype(float)
    clip_aligned = ts.map(clip_reason).fillna("").astype(str)

    idx = pd.DatetimeIndex(ts)
    target_position = (
        pd.to_numeric(rebuilt["target_position"], errors="coerce").fillna(0.0).astype(float)
        * scale_aligned.values
    )
    close = pd.to_numeric(rebuilt["close"], errors="coerce").set_axis(idx)
    open_series = pd.to_numeric(
        rebuilt.get("open", pd.Series(np.nan, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)

    fill_mode = str(rebuilt.get("fill_mode", pd.Series(["close"])).iloc[0]).strip().lower()
    turnover = pd.to_numeric(
        rebuilt.get("turnover", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    transaction_cost = pd.to_numeric(
        rebuilt.get("transaction_cost", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    slippage_cost = pd.to_numeric(
        rebuilt.get("slippage_cost", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    funding_pnl = pd.to_numeric(
        rebuilt.get("funding_pnl", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    borrow_cost = pd.to_numeric(
        rebuilt.get("borrow_cost", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    executed_position_old = pd.to_numeric(
        rebuilt.get("executed_position", pd.Series(0.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)
    capital_base = pd.to_numeric(
        rebuilt.get("capital_base", pd.Series(1.0, index=rebuilt.index)), errors="coerce"
    ).set_axis(idx)

    cost_bps = _infer_bps_from_cost(turnover, transaction_cost)
    slippage_bps = _infer_bps_from_cost(turnover, slippage_cost)
    funding_rate = _infer_funding_rate(executed_position_old, funding_pnl)
    borrow_rate = _infer_borrow_rate(executed_position_old, borrow_cost)

    ledger = compute_pnl_ledger(
        target_position=target_position.set_axis(idx),
        close=close,
        open_=open_series,
        execution_mode=fill_mode,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        funding_rate=funding_rate,
        borrow_rate=borrow_rate,
        capital_base=capital_base,
    )

    rebuilt["target_position"] = target_position.values
    rebuilt["executed_position"] = ledger["executed_position"].values
    rebuilt["prior_executed_position"] = ledger["prior_executed_position"].values
    rebuilt["fill_mode"] = ledger["fill_mode"].values
    rebuilt["fill_price"] = ledger["fill_price"].values
    rebuilt["mark_price"] = ledger["mark_price"].values
    rebuilt["bar_return_close_to_close"] = ledger["bar_return_close_to_close"].values
    rebuilt["entry_return_next_open"] = ledger["entry_return_next_open"].values
    rebuilt["holding_return"] = ledger["holding_return"].values
    rebuilt["turnover"] = ledger["turnover"].values
    rebuilt["gross_pnl"] = ledger["gross_pnl"].values
    rebuilt["transaction_cost"] = ledger["transaction_cost"].values
    rebuilt["slippage_cost"] = ledger["slippage_cost"].values
    rebuilt["funding_pnl"] = ledger["funding_pnl"].values
    rebuilt["borrow_cost"] = ledger["borrow_cost"].values
    rebuilt["net_pnl"] = ledger["net_pnl"].values
    rebuilt["gross_exposure"] = ledger["gross_exposure"].values
    rebuilt["net_exposure"] = ledger["net_exposure"].values
    rebuilt["capital_base"] = ledger["capital_base"].values
    rebuilt["equity_return"] = ledger["equity_return"].values
    rebuilt["allocation_scale"] = scale_aligned.values
    rebuilt["risk_scale"] = scale_aligned.values
    rebuilt["portfolio_clip_scale"] = scale_aligned.values
    rebuilt["allocation_clip_reason"] = clip_aligned.values
    return rebuilt


def _trace_from_frame(frame: pd.DataFrame) -> pd.DataFrame:
    cols = [
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
    trace = frame[[c for c in cols if c in frame.columns]].copy()
    trace["state"] = np.where(trace.get("executed_position", 0.0) == 0, "flat", "in_position")
    if "allocation_scale" in frame.columns:
        trace["allocation_scale"] = frame["allocation_scale"].values
    if "allocation_clip_reason" in frame.columns:
        trace["allocation_clip_reason"] = frame["allocation_clip_reason"].values
    validate_trace_schema(trace)
    return trace


def run_engine(
    run_id: str,
    symbols: List[str],
    strategies: List[str],
    params: Dict[str, object],
    cost_bps: float,
    data_root: Path | None = None,
    params_by_strategy: Optional[Dict[str, Dict[str, object]]] = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    timeframe: str = _DEFAULT_TIMEFRAME,
    memory_efficient: bool = True,
) -> Dict[str, object]:
    data_root = Path(data_root) if data_root is not None else get_data_root()
    engine_dir = data_root / "runs" / run_id / "engine"
    ensure_dir(engine_dir)

    strategy_frames: Dict[str, pd.DataFrame] = {}
    metrics: Dict[str, object] = {"strategies": {}, "strategy_metadata": {}}
    event_flags_cache: Dict[Tuple[str, str], pd.DataFrame] = {}
    event_features_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    universe_snapshots = load_universe_snapshots(data_root, run_id)

    for strategy_name in strategies:
        strategy_params = (
            params_by_strategy.get(strategy_name, params) if params_by_strategy else params
        )
        strategy_obj = get_strategy(strategy_name)
        required_features = getattr(strategy_obj, "required_features", []) or []
        feature_cols = sorted(
            set(
                required_features
                + ["timestamp", "funding_rate_scaled", "funding_rate_realized", "spread_bps"]
            )
        )
        bars_cols = ["timestamp", "open", "high", "low", "close", "volume", "quote_volume"]

        symbol_results: List[StrategyResult] = []
        for symbol in symbols:
            event_flags = pd.DataFrame()
            event_features = pd.DataFrame()

            is_dsl, _ = is_dsl_strategy(strategy_name)
            if is_dsl:
                blueprint_run_id = str(
                    strategy_params.get("dsl_blueprint", {}).get(
                        "run_id",
                        strategy_params.get("executable_strategy_spec", {})
                        .get("metadata", {})
                        .get("run_id", run_id),
                    )
                )
                cache_key = (blueprint_run_id, str(symbol).upper())
                if cache_key not in event_flags_cache:
                    event_flags_cache[cache_key] = load_registry_flags(
                        data_root=data_root, run_id=blueprint_run_id, symbol=symbol
                    )
                event_flags = event_flags_cache[cache_key]
                if cache_key not in event_features_cache:
                    event_features_cache[cache_key] = build_event_feature_frame(
                        data_root=data_root, run_id=blueprint_run_id, symbol=symbol
                    )
                event_features = event_features_cache[cache_key]

            bars, features_raw = load_symbol_raw_data(
                data_root,
                symbol,
                run_id,
                timeframe,
                bars_columns=bars_cols,
                feature_columns=feature_cols,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            features = assemble_symbol_context(
                bars,
                features_raw,
                data_root,
                symbol,
                run_id,
                timeframe,
                start_ts,
                end_ts,
                event_flags,
                event_features,
                event_feature_ffill_bars=int(strategy_params.get("event_feature_ffill_bars", 12)),
            )

            ts_index = pd.DatetimeIndex(pd.to_datetime(bars["timestamp"], utc=True))
            eligibility_mask = pd.Series(True, index=ts_index, dtype=bool)
            if not universe_snapshots.empty:
                rows = universe_snapshots[
                    universe_snapshots["symbol"].astype(str).str.upper() == str(symbol).upper()
                ]
                if rows.empty:
                    eligibility_mask[:] = False
                else:
                    ts_values = ts_index.values[:, np.newaxis]
                    starts = pd.to_datetime(rows["listing_start"], utc=True).values
                    ends = pd.to_datetime(rows["listing_end"], utc=True).values
                    mask = ((ts_values >= starts) & (ts_values <= ends)).any(axis=1)
                    eligibility_mask = pd.Series(mask, index=ts_index, dtype=bool)

            result = calculate_strategy_returns(
                symbol,
                bars,
                features,
                strategy_name,
                strategy_params,
                cost_bps,
                data_root,
                eligibility_mask=eligibility_mask,
                calibration_dir=data_root / "reports" / "cost_calibration" / run_id,
            )
            symbol_results.append(result)

        combined = combine_strategy_symbols([res.data for res in symbol_results])
        validate_strategy_frame_schema(combined, frame_name=f"strategy frame [{strategy_name}]")
        strategy_frames[strategy_name] = combined

        merged_meta: dict = {}
        for res in symbol_results:
            if getattr(res, "strategy_metadata", None):
                merged_meta.update(res.strategy_metadata)
        merged_meta.update(
            {
                key: value
                for key, value in _spec_metadata_payload(
                    strategy_params.get("executable_strategy_spec")
                ).items()
                if value
            }
        )
        metrics["strategy_metadata"][strategy_name] = merged_meta

    raw_positions_by_strategy = {}
    requested_scale_by_strategy = {}
    strategy_returns = {}
    for name, frame in strategy_frames.items():
        validate_strategy_frame_schema(frame, frame_name=f"strategy frame [{name}]")
        raw_positions_by_strategy[name] = frame.groupby("timestamp", sort=True)[
            "signal_position"
        ].sum()
        requested_scale_by_strategy[name] = frame.groupby("timestamp", sort=True)[
            "requested_position_scale"
        ].mean()
        strategy_returns[name] = frame.groupby("timestamp", sort=True)["gross_pnl"].sum()

    allocation_diagnostics = pd.DataFrame()
    allocation_contract: AllocationContract | None = None
    if raw_positions_by_strategy:
        allocation_params = dict(params)
        if "allocation_spec" not in allocation_params and params_by_strategy:
            for strategy_name in strategies:
                per_strategy = dict(params_by_strategy.get(strategy_name, {}) or {})
                if "allocation_spec" in per_strategy:
                    allocation_params["allocation_spec"] = per_strategy["allocation_spec"]
                    break
        allocation_contract = build_allocation_contract(allocation_params)
        allocation = allocate_position_details(
            raw_positions_by_strategy,
            requested_scale_by_strategy,
            allocation_contract.limits,
            contract=allocation_contract,
            strategy_returns=strategy_returns,
        )
        allocation_contract = allocation.contract
        allocation_diagnostics = allocation.diagnostics.copy()
        clip_reason_series = (
            allocation_diagnostics.set_index("timestamp")["clip_reason"]
            if not allocation_diagnostics.empty
            else pd.Series(dtype=object)
        )
        for name, frame in list(strategy_frames.items()):
            strategy_frames[name] = _rebuild_frame_after_allocation(
                frame,
                allocation_scale=allocation.scale_by_strategy.get(
                    name, pd.Series(1.0, index=raw_positions_by_strategy[name].index)
                ),
                clip_reason=clip_reason_series,
            )
            validate_strategy_frame_schema(
                strategy_frames[name], frame_name=f"strategy frame [{name}] post-allocation"
            )
        metrics["allocation"] = allocation.summary

    for name, frame in strategy_frames.items():
        metrics["strategies"][name] = {
            **summarize_pnl(frame.groupby("timestamp")["net_pnl"].sum()),
            "entries": entry_count(frame),
        }

    portfolio = aggregate_strategy_results(strategy_frames)
    if not portfolio.empty:
        validate_portfolio_frame_schema(portfolio)
    strategy_contributions = build_strategy_contributions(strategy_frames, portfolio)
    symbol_contributions = build_symbol_contributions(strategy_frames, portfolio)
    metrics["portfolio"] = (
        summarize_portfolio_ledger(portfolio)
        if not portfolio.empty
        else summarize_portfolio_ledger(pd.DataFrame())
    )

    artifact_inventory = []
    for name, frame in strategy_frames.items():
        artifact_inventory.append(
            write_engine_dataframe(
                frame,
                engine_dir=engine_dir,
                artifact_name=f"strategy_returns_{name}",
                schema_name="strategy_frame",
                schema_version=STRATEGY_FRAME_SCHEMA_VERSION,
            )
        )
        trace = _trace_from_frame(frame)
        artifact_inventory.append(
            write_engine_dataframe(
                trace,
                engine_dir=engine_dir,
                artifact_name=f"strategy_trace_{name}",
                schema_name="strategy_trace",
                schema_version=TRACE_SCHEMA_VERSION,
            )
        )
    artifact_inventory.append(
        write_engine_dataframe(
            portfolio,
            engine_dir=engine_dir,
            artifact_name="portfolio_returns",
            schema_name="portfolio_frame",
            schema_version=PORTFOLIO_FRAME_SCHEMA_VERSION,
        )
    )
    if not strategy_contributions.empty:
        artifact_inventory.append(
            write_engine_dataframe(
                strategy_contributions,
                engine_dir=engine_dir,
                artifact_name="strategy_contributions",
                schema_name="strategy_contributions",
                schema_version="strategy_contributions_v1",
            )
        )
    if not symbol_contributions.empty:
        artifact_inventory.append(
            write_engine_dataframe(
                symbol_contributions,
                engine_dir=engine_dir,
                artifact_name="symbol_contributions",
                schema_name="symbol_contributions",
                schema_version="symbol_contributions_v1",
            )
        )
    if not allocation_diagnostics.empty:
        artifact_inventory.append(
            write_engine_dataframe(
                allocation_diagnostics,
                engine_dir=engine_dir,
                artifact_name="allocation_diagnostics",
                schema_name="allocation_diagnostics",
                schema_version="allocation_diagnostics_v1",
            )
        )
    metrics_path = engine_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    manifest = build_engine_run_manifest(
        run_id=run_id,
        engine_dir=engine_dir,
        symbols=symbols,
        strategies=strategies,
        params=params,
        metrics=metrics,
        artifact_inventory=artifact_inventory,
        timeframe=timeframe,
        cost_bps=cost_bps,
        start_ts=start_ts,
        end_ts=end_ts,
        strategy_frames=strategy_frames,
        allocation_contract=allocation_contract,
    )
    manifest_path = write_engine_run_manifest(manifest, engine_dir=engine_dir)

    return {
        "engine_dir": engine_dir,
        "strategy_frames": strategy_frames,
        "portfolio": portfolio,
        "strategy_contributions": strategy_contributions,
        "symbol_contributions": symbol_contributions,
        "metrics": metrics,
        "allocation_diagnostics": allocation_diagnostics,
        "artifact_inventory": manifest["artifacts"],
        "manifest": manifest,
        "manifest_path": manifest_path,
        "metrics_path": metrics_path,
    }
