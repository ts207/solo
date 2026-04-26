from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import atomic_write_json, ensure_dir, read_parquet


def _parse_window(window: str) -> tuple[str, str]:
    parts = str(window or "").split("/", 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise ValueError("window must be formatted as <ISO8601-start>/<ISO8601-end>")
    return parts[0].strip(), parts[1].strip()


def _phase2_candidate_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / str(run_id) / "phase2_candidates.parquet"


def _read_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"phase2 candidate artifact not found: {path}")
    return read_parquet(path)


def _series_numeric(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    for column in columns:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(dtype="float64")


def _representative_metric(series: pd.Series, *, prefer: str = "median") -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return 0.0
    if prefer == "max_abs":
        idx = values.abs().idxmax()
        return float(values.loc[idx])
    return float(values.median())


def build_forward_confirmation_payload(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    start, end = _parse_window(window)
    candidates = _read_candidates(_phase2_candidate_path(root, run_id))
    if candidates.empty:
        raise ValueError(f"phase2 candidate artifact for {run_id} is empty")

    t_net = _series_numeric(candidates, ["t_stat_net", "t_stat", "net_t_stat"])
    mean_net = _series_numeric(
        candidates,
        ["mean_return_net_bps", "after_cost_expectancy_bps", "cost_adjusted_return_bps"],
    )
    sharpe = _series_numeric(candidates, ["sharpe_net", "sharpe", "risk_adjusted_score"])
    metrics = {
        "t_stat_net": _representative_metric(t_net, prefer="max_abs"),
        "mean_return_net_bps": _representative_metric(mean_net),
        "sharpe_net": _representative_metric(sharpe),
    }
    out_dir = root / "reports" / "validation" / str(run_id)
    return {
        "run_id": str(run_id),
        "confirmed_at": datetime.now(UTC).isoformat(),
        "oos_window_start": start,
        "oos_window_end": end,
        "metrics": metrics,
        "evidence_bundle_path": str(out_dir / "forward_confirmation.json"),
        "source_candidate_rows": len(candidates),
        "method": "phase2_candidate_metric_snapshot",
    }


def forward_confirm(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    payload = build_forward_confirmation_payload(run_id=run_id, window=window, data_root=root)
    out_dir = root / "reports" / "validation" / str(run_id)
    ensure_dir(out_dir)
    out_path = out_dir / "forward_confirmation.json"
    atomic_write_json(out_path, payload)
    payload["path"] = str(out_path)
    return payload


__all__ = ["build_forward_confirmation_payload", "forward_confirm"]
