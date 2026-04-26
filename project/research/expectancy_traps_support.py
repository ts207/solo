from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd


def load_expectancy_payload(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_empty_robustness_payload(
    *,
    out_dir: Path,
    run_id: str,
    symbols: list[str],
    horizons: list[int],
    skip_reason: str,
) -> int:
    payload = {
        "run_id": run_id,
        "symbols": symbols,
        "horizons": horizons,
        "stability_diagnostics": {
            "pass": False,
            "rank_consistency": 0.0,
            "performance_decay": 1.0,
            "neighborhood_supported": False,
            "scenarios": [],
        },
        "capacity_diagnostics": {
            "pass": False,
            "events_per_day": 0.0,
            "min_events_per_day": 0.0,
            "symbol_coverage": {symbol: 0.0 for symbol in symbols},
        },
        "survivors": [],
        "skipped": True,
        "skip_reason": skip_reason,
    }
    json_path = out_dir / "conditional_expectancy_robustness.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {json_path}")
    return 0


def parse_horizons(value: str) -> list[int]:
    parts = [x.strip() for x in value.split(",") if x.strip()]
    horizons = sorted({int(x) for x in parts if int(x) > 0})
    if not horizons:
        raise ValueError("At least one positive horizon is required")
    return horizons


def pick_window_column(columns: Iterable[str], prefix: str) -> str:
    candidates: list[tuple[int, str]] = []
    for col in columns:
        if not col.startswith(prefix):
            continue
        try:
            window = int(col.split("_")[-1])
        except ValueError:
            continue
        candidates.append((window, col))
    if not candidates:
        raise ValueError(f"Missing required feature prefix: {prefix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        valid = values[~np.isnan(values)]
        if len(valid) == 0:
            return np.nan
        last = values[-1]
        if np.isnan(last):
            return np.nan
        return float(np.sum(valid <= last) / len(valid) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def stable_row_seed(condition: str, horizon: int, base_seed: int) -> int:
    acc = (int(base_seed) + int(horizon) * 1009) % (2**32 - 1)
    for ch in str(condition):
        acc = (acc * 131 + ord(ch)) % (2**32 - 1)
    return int(acc)
