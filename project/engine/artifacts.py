from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from project.engine.schema import (
    ENGINE_ARTIFACT_SCHEMA_VERSION,
    PORTFOLIO_FRAME_SCHEMA_VERSION,
    STRATEGY_FRAME_SCHEMA_VERSION,
    TRACE_SCHEMA_VERSION,
)
from project.engine.risk_allocator import AllocationContract
from project.io.utils import write_parquet
from project.portfolio import AllocationSpec


ENGINE_RUN_MANIFEST_VERSION = "engine_run_manifest_v1"
CAPITAL_MODEL_NAME = "equity_curve_from_net_pnl"


@dataclass(frozen=True)
class ArtifactRecord:
    artifact_name: str
    schema_name: str
    schema_version: str
    storage_format: str
    path: str
    rows: int
    columns: list[str]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        ts = (
            value.tz_convert("UTC")
            if isinstance(value, pd.Timestamp) and value.tzinfo is not None
            else value
        )
        if isinstance(ts, pd.Timestamp):
            return ts.isoformat()
        return ts.astimezone(timezone.utc).isoformat() if ts.tzinfo else ts.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if (
        pd.isna(value)
        if not isinstance(value, (str, bytes, dict, list, tuple, set, Path))
        else False
    ):
        return None
    return value


def write_engine_dataframe(
    df: pd.DataFrame,
    *,
    engine_dir: Path,
    artifact_name: str,
    schema_name: str,
    schema_version: str,
) -> ArtifactRecord:
    target = engine_dir / f"{artifact_name}.parquet"
    actual_path, storage_format = write_parquet(df, target)
    return ArtifactRecord(
        artifact_name=artifact_name,
        schema_name=schema_name,
        schema_version=schema_version,
        storage_format=storage_format,
        path=str(actual_path),
        rows=int(len(df)),
        columns=[str(c) for c in df.columns],
    )


def build_engine_run_manifest(
    *,
    run_id: str,
    engine_dir: Path,
    symbols: Iterable[str],
    strategies: Iterable[str],
    params: dict[str, Any],
    metrics: dict[str, Any],
    artifact_inventory: list[ArtifactRecord],
    timeframe: str,
    cost_bps: float,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    strategy_frames: dict[str, pd.DataFrame],
    allocation_contract: AllocationContract | None = None,
) -> dict[str, Any]:
    fill_modes = sorted(
        {
            str(mode).strip().lower()
            for frame in strategy_frames.values()
            if not frame.empty and "fill_mode" in frame.columns
            for mode in frame["fill_mode"].dropna().unique().tolist()
        }
    )
    allocator_keys = [
        "allocator_mode",
        "allocator_deterministic",
        "allocator_turnover_penalty",
        "strategy_risk_budgets",
        "family_risk_budgets",
        "strategy_family_map",
        "portfolio_max_exposure",
        "max_portfolio_gross",
        "max_strategy_gross",
        "max_symbol_gross",
        "max_new_exposure_per_bar",
        "target_annual_volatility",
        "max_pairwise_correlation",
        "drawdown_limit",
        "portfolio_max_drawdown",
        "max_symbol_exposure",
        "enable_correlation_allocation",
    ]
    allocator_config = {key: params[key] for key in allocator_keys if key in params}
    raw_allocation_spec = params.get("allocation_spec")
    if raw_allocation_spec is not None and not allocator_config:
        allocation_spec = (
            raw_allocation_spec
            if isinstance(raw_allocation_spec, AllocationSpec)
            else AllocationSpec.model_validate(dict(raw_allocation_spec))
        )
        allocator_config = allocation_spec.to_allocator_params()

    manifest = {
        "manifest_type": "engine_run_manifest",
        "manifest_version": ENGINE_RUN_MANIFEST_VERSION,
        "engine_artifact_schema_version": ENGINE_ARTIFACT_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "engine_dir": str(engine_dir),
        "timeframe": str(timeframe),
        "symbols": sorted({str(s) for s in symbols}),
        "strategies": [str(s) for s in strategies],
        "capital_model": {
            "name": CAPITAL_MODEL_NAME,
            "portfolio_schema_version": PORTFOLIO_FRAME_SCHEMA_VERSION,
            "starting_equity": _json_safe(metrics.get("portfolio", {}).get("starting_equity")),
            "ending_equity": _json_safe(metrics.get("portfolio", {}).get("ending_equity")),
        },
        "execution": {
            "fill_modes": fill_modes,
            "input_cost_bps": float(cost_bps),
            "start_ts": _json_safe(start_ts),
            "end_ts": _json_safe(end_ts),
        },
        "allocator": {
            "enabled": bool(strategy_frames),
            "mode": (
                allocation_contract.policy.mode
                if allocation_contract is not None
                else str(params.get("allocator_mode", "heuristic"))
            ),
            "config": _json_safe(allocator_config),
            "contract": (
                _json_safe(allocation_contract.to_manifest_payload())
                if allocation_contract is not None
                else None
            ),
            "summary": _json_safe(metrics.get("allocation", {})),
        },
        "schemas": {
            "strategy_frame": STRATEGY_FRAME_SCHEMA_VERSION,
            "strategy_trace": TRACE_SCHEMA_VERSION,
            "portfolio_frame": PORTFOLIO_FRAME_SCHEMA_VERSION,
        },
        "artifacts": [_json_safe(asdict(record)) for record in artifact_inventory],
        "metrics": _json_safe(metrics),
    }
    return manifest


def write_engine_run_manifest(manifest: dict[str, Any], *, engine_dir: Path) -> Path:
    path = engine_dir / "engine_run_manifest.json"
    path.write_text(json.dumps(_json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8")
    return path
