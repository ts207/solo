from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.io.utils import ensure_dir, read_parquet
from project.research.services.pathing import negative_control_out_dir
from project.specs.manifest import finalize_manifest, start_manifest


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate negative-control summary artifacts.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--out_dir", default="")
    return parser


def _read_csv_or_parquet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            return read_parquet(path)
        except (ImportError, OSError, ValueError):
            csv_path = path.with_suffix(".csv")
            if csv_path.exists():
                return pd.read_csv(csv_path)
            raise
    return pd.read_csv(path)


def _load_edge_candidates(run_id: str, data_root: Path) -> pd.DataFrame:
    base = data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
    if base.exists():
        return _read_csv_or_parquet(base)
    csv_path = base.with_suffix(".csv")
    if csv_path.exists():
        return _read_csv_or_parquet(csv_path)
    return pd.DataFrame()


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    raw = df[column]
    truthy = {"1", "true", "t", "yes", "y", "on", "pass"}
    return raw.map(
        lambda value: (
            bool(value)
            if isinstance(value, bool)
            else (str(value).strip().lower() in truthy if value is not None else False)
        )
    ).astype(bool)


def _placebo_failure_rate(df: pd.DataFrame) -> tuple[float | None, str]:
    control_rate = _numeric_series(df, "control_pass_rate")
    if control_rate.notna().any():
        return float(control_rate.dropna().mean()), "candidate.control_pass_rate"

    placebo_cols = [
        "pass_shift_placebo",
        "pass_random_entry_placebo",
        "pass_direction_reversal_placebo",
    ]
    present = [col for col in placebo_cols if col in df.columns]
    if not present:
        return None, "missing"

    passes = pd.Series(True, index=df.index, dtype=bool)
    for col in present:
        passes &= _bool_series(df, col)
    # Lower is better for promotion; this measures failure/exceedance rate.
    return float((~passes).mean()), "candidate.placebo_failure_rate"


def _event_column(df: pd.DataFrame) -> str | None:
    for col in ("event_type", "event"):
        if col in df.columns:
            return col
    return None


def _build_summary(df: pd.DataFrame, *, run_id: str) -> Dict[str, Any]:
    event_col = _event_column(df)
    by_event: Dict[str, Any] = {}

    rate, source = _placebo_failure_rate(df)
    global_summary: Dict[str, Any] = {
        "candidate_count": int(len(df)),
        "has_control_evidence": bool(rate is not None),
        "control_rate_source": source,
    }
    if rate is not None:
        global_summary["pass_rate_after_bh"] = float(rate)
        global_summary["control_pass_rate"] = float(rate)

    if event_col is not None and not df.empty:
        for event_type, sub in df.groupby(event_col, sort=True):
            event_rate, event_source = _placebo_failure_rate(sub)
            row: Dict[str, Any] = {
                "candidate_count": int(len(sub)),
                "has_control_evidence": bool(event_rate is not None),
                "control_rate_source": event_source,
            }
            if event_rate is not None:
                row["pass_rate_after_bh"] = float(event_rate)
                row["control_pass_rate"] = float(event_rate)
            by_event[str(event_type)] = row

    return {
        "schema_version": "negative_control_summary_v1",
        "run_id": str(run_id),
        "candidate_count": int(len(df)),
        "global": global_summary,
        "by_event": by_event,
    }


def main(argv: List[str] | None = None) -> int:
    parser = _make_parser()
    args = parser.parse_args(argv)

    data_root = get_data_root()
    out_dir = (
        Path(args.out_dir)
        if str(args.out_dir).strip()
        else negative_control_out_dir(data_root=data_root, run_id=args.run_id)
    )
    ensure_dir(out_dir)

    manifest = start_manifest(
        "generate_negative_control_summary",
        args.run_id,
        vars(args),
        [],
        [{"path": str(out_dir / "negative_control_summary.json")}],
    )

    try:
        candidates_df = _load_edge_candidates(args.run_id, data_root)
        summary = _build_summary(candidates_df, run_id=args.run_id)
        (out_dir / "negative_control_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        finalize_manifest(manifest, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
