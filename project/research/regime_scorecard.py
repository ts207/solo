from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "regime_scorecard_v1"
DEFAULT_DATA_ROOT = Path(__file__).resolve().parents[2] / "data"
DEFAULT_OUTPUT_DIR = DEFAULT_DATA_ROOT / "reports" / "regime_baselines"
LOGGER = logging.getLogger(__name__)

CLASSIFICATION_PRIORITY = {
    "stable_positive": 5,
    "year_conditional": 4,
    "unstable": 3,
    "insufficient_support": 2,
    "negative": 1,
}

NEXT_ACTION = {
    "stable_positive": "run_event_lift_for_best_tuple",
    "year_conditional": "define_ex_ante_regime_variant_before_events",
    "unstable": "monitor_only_no_event_search",
    "negative": "reject_directional_event_search_for_regime",
    "insufficient_support": "repair_context_or_price_cost_data",
}

DECISION = {
    "stable_positive": "allow_event_lift",
    "year_conditional": "require_variant",
    "unstable": "monitor_only",
    "negative": "reject_directional",
    "insufficient_support": "data_repair",
}

ROW_COLUMNS = [
    "schema_version",
    "source_run_ids",
    "matrix_id",
    "regime_id",
    "candidate_baseline_count",
    "stable_positive_count",
    "year_conditional_count",
    "unstable_count",
    "negative_count",
    "insufficient_support_count",
    "best_symbol",
    "best_direction",
    "best_horizon_bars",
    "best_mean_net_bps",
    "best_t_stat_net",
    "best_max_year_pnl_share",
    "best_effective_n",
    "classification",
    "decision",
    "next_action",
]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _safe_int(value: Any) -> int:
    try:
        if pd.isna(value):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def discover_baseline_result_paths(
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> list[Path]:
    base = data_root / "reports" / "regime_baselines"
    if run_id:
        path = base / run_id / "regime_baselines.parquet"
        return [path] if path.exists() else []
    if not base.exists():
        return []
    return sorted(base.glob("*/regime_baselines.parquet"))


def _latest_run_per_matrix(paths: list[Path]) -> list[Path]:
    selected: dict[str, tuple[str, Path]] = {}
    for path in paths:
        try:
            frame = pd.read_parquet(path, columns=["run_id", "matrix_id"])
        except Exception as exc:
            LOGGER.warning("Could not inspect baseline result %s: %s", path, exc)
            continue
        if frame.empty:
            continue
        matrix_id = str(frame["matrix_id"].dropna().iloc[0])
        run_id = str(frame["run_id"].dropna().iloc[0])
        current = selected.get(matrix_id)
        if current is None or run_id > current[0]:
            selected[matrix_id] = (run_id, path)
    return [item[1] for item in sorted(selected.values(), key=lambda item: item[0])]


def select_baseline_result_paths(
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
    matrix_id: str | None = None,
    all_runs: bool = False,
) -> list[Path]:
    paths = discover_baseline_result_paths(data_root=data_root, run_id=run_id)
    if matrix_id:
        filtered: list[Path] = []
        for path in paths:
            try:
                frame = pd.read_parquet(path, columns=["matrix_id"])
            except Exception as exc:
                LOGGER.warning("Could not inspect baseline matrix id %s: %s", path, exc)
                continue
            if not frame.empty and str(frame["matrix_id"].dropna().iloc[0]) == matrix_id:
                filtered.append(path)
        paths = filtered
    if run_id or all_runs:
        return paths
    return _latest_run_per_matrix(paths)


def load_baseline_results(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            LOGGER.warning("Could not read baseline result %s: %s", path, exc)
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _numeric_rank(value: Any, *, lower_is_better: bool = False) -> float:
    numeric = _safe_float(value)
    if numeric is None:
        return float("-inf")
    return -numeric if lower_is_better else numeric


def best_baseline_row(group: pd.DataFrame) -> pd.Series:
    if group.empty:
        raise ValueError("Cannot rank an empty baseline group")

    def key(item: tuple[int, pd.Series]) -> tuple[Any, ...]:
        _idx, row = item
        return (
            CLASSIFICATION_PRIORITY.get(str(row.get("classification") or ""), 0),
            _numeric_rank(row.get("mean_net_bps")),
            _numeric_rank(row.get("t_stat_net")),
            _numeric_rank(row.get("max_year_pnl_share"), lower_is_better=True),
            _safe_int(row.get("effective_n")),
            str(row.get("symbol") or ""),
            str(row.get("direction") or ""),
            -_safe_int(row.get("horizon_bars")),
        )

    return max(group.iterrows(), key=key)[1]


def classify_regime_scorecard(counts: dict[str, int]) -> tuple[str, str, str]:
    if counts.get("stable_positive", 0) > 0:
        classification = "stable_positive"
    elif counts.get("year_conditional", 0) > 0:
        classification = "year_conditional"
    elif counts.get("unstable", 0) > 0:
        classification = "unstable"
    elif counts.get("negative", 0) > 0 and counts.get("insufficient_support", 0) == 0:
        classification = "negative"
    elif counts.get("insufficient_support", 0) > 0:
        classification = "insufficient_support"
    else:
        classification = "insufficient_support"
    return classification, DECISION[classification], NEXT_ACTION[classification]


def build_regime_scorecard(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=ROW_COLUMNS)
    rows: list[dict[str, Any]] = []
    for (matrix_id, regime_id), group in df.groupby(["matrix_id", "regime_id"], dropna=False):
        counts = {
            "stable_positive": int((group["classification"] == "stable_positive").sum()),
            "year_conditional": int((group["classification"] == "year_conditional").sum()),
            "unstable": int((group["classification"] == "unstable").sum()),
            "negative": int((group["classification"] == "negative").sum()),
            "insufficient_support": int((group["classification"] == "insufficient_support").sum()),
        }
        classification, decision, next_action = classify_regime_scorecard(counts)
        best = best_baseline_row(group)
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "source_run_ids": sorted({str(item) for item in group["run_id"].dropna().unique()}),
                "matrix_id": str(matrix_id),
                "regime_id": str(regime_id),
                "candidate_baseline_count": len(group),
                "stable_positive_count": counts["stable_positive"],
                "year_conditional_count": counts["year_conditional"],
                "unstable_count": counts["unstable"],
                "negative_count": counts["negative"],
                "insufficient_support_count": counts["insufficient_support"],
                "best_symbol": str(best.get("symbol") or ""),
                "best_direction": str(best.get("direction") or ""),
                "best_horizon_bars": _safe_int(best.get("horizon_bars")),
                "best_mean_net_bps": _safe_float(best.get("mean_net_bps")),
                "best_t_stat_net": _safe_float(best.get("t_stat_net")),
                "best_max_year_pnl_share": _safe_float(best.get("max_year_pnl_share")),
                "best_effective_n": _safe_int(best.get("effective_n")),
                "classification": classification,
                "decision": decision,
                "next_action": next_action,
            }
        )
    return pd.DataFrame(rows, columns=ROW_COLUMNS).sort_values(["matrix_id", "regime_id"])


def _records_for_json(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        item: dict[str, Any] = {}
        for column in ROW_COLUMNS:
            value = row.get(column)
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                value = None
            item[column] = value
        records.append(item)
    return records


def write_regime_scorecard(df: pd.DataFrame, *, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "row_count": len(df),
        "rows": _records_for_json(df),
    }
    (output_dir / "regime_scorecard.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    df.to_parquet(output_dir / "regime_scorecard.parquet", index=False)
    write_regime_scorecard_markdown(df, output_dir=output_dir)


def write_regime_scorecard_markdown(df: pd.DataFrame, *, output_dir: Path) -> None:
    counts = df["classification"].value_counts().to_dict() if not df.empty else {}
    lines = [
        "# Regime Scorecard",
        "",
        f"- rows: `{len(df)}`",
        "",
        "## Classification Counts",
        "",
    ]
    lines.extend(f"- {key}: {int(counts[key])}" for key in sorted(counts))
    lines.extend(["", "## Regimes", ""])
    for _, row in df.iterrows():
        lines.append(
            "- "
            f"{row['matrix_id']} {row['regime_id']}: {row['classification']} "
            f"decision={row['decision']} best={row['best_symbol']}/"
            f"{row['best_direction']}/h{row['best_horizon_bars']}"
        )
    (output_dir / "regime_scorecard.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def update_regime_scorecard(
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
    matrix_id: str | None = None,
    all_runs: bool = False,
    output_dir: Path | None = None,
) -> pd.DataFrame:
    paths = select_baseline_result_paths(
        data_root=data_root,
        run_id=run_id,
        matrix_id=matrix_id,
        all_runs=all_runs,
    )
    baseline_df = load_baseline_results(paths)
    scorecard = build_regime_scorecard(baseline_df)
    out_dir = output_dir or data_root / "reports" / "regime_baselines"
    write_regime_scorecard(scorecard, output_dir=out_dir)
    return scorecard
