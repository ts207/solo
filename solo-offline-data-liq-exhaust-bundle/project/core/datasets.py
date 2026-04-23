from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from project.core.exceptions import ContractViolationError
from project.core.timeframes import (
    bars_dataset_name,
    funding_dataset_name,
    normalize_timeframe,
    ohlcv_dataset_name,
)
from project.io.utils import run_scoped_lake_path


@dataclass(frozen=True)
class DatasetResolution:
    dataset_id: str
    market: str
    kind: str
    timeframe: str | None
    artifact_token: str
    storage_scope: str
    dataset_name: str


def resolve_dataset_id(dataset_id: str) -> DatasetResolution:
    raw = str(dataset_id or "").strip().lower()
    if not raw:
        raise ContractViolationError("Dataset identifier cannot be empty")

    parts = raw.split("_")
    if len(parts) >= 3 and parts[0] in {"perp", "spot"} and parts[1] == "ohlcv":
        tf = normalize_timeframe(parts[2])
        return DatasetResolution(
            dataset_id=raw,
            market=parts[0],
            kind="ohlcv",
            timeframe=tf,
            artifact_token=f"raw.{parts[0]}.{ohlcv_dataset_name(tf)}",
            storage_scope="cleaned",
            dataset_name=bars_dataset_name(tf),
        )

    if len(parts) >= 2 and parts[0] == "funding":
        tf = normalize_timeframe(parts[1])
        return DatasetResolution(
            dataset_id=raw,
            market="perp",
            kind="funding",
            timeframe=tf,
            artifact_token=f"raw.perp.{funding_dataset_name(tf)}",
            storage_scope="raw",
            dataset_name="funding",
        )

    if len(parts) >= 3 and parts[0] == "perp" and parts[1] == "funding":
        tf = normalize_timeframe(parts[2])
        return DatasetResolution(
            dataset_id=raw,
            market="perp",
            kind="funding",
            timeframe=tf,
            artifact_token=f"raw.perp.{funding_dataset_name(tf)}",
            storage_scope="raw",
            dataset_name="funding",
        )

    raise ContractViolationError(f"Unsupported dataset identifier: '{dataset_id}'")


def dataset_path_candidates(
    *, data_root: Path, run_id: str, symbol: str, dataset_id: str
) -> list[Path]:
    resolved = resolve_dataset_id(dataset_id)
    if resolved.kind == "ohlcv":
        return [
            run_scoped_lake_path(
                data_root, run_id, "cleaned", resolved.market, symbol, resolved.dataset_name
            ),
            data_root / "lake" / "cleaned" / resolved.market / symbol / resolved.dataset_name,
        ]
    if resolved.kind == "funding":
        return [
            run_scoped_lake_path(
                data_root, run_id, "raw", "bybit", resolved.market, symbol, resolved.dataset_name
            ),
            data_root
            / "lake"
            / "raw"
            / "bybit"
            / resolved.market
            / symbol
            / resolved.dataset_name,
        ]
    raise ContractViolationError(f"No path mapping for dataset identifier: '{dataset_id}'")
