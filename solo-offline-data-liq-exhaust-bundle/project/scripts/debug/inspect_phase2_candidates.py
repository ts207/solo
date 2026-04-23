from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_SAMPLE_COLUMNS = [
    "symbol",
    "event_type",
    "candidate_id",
    "condition",
    "condition_raw",
    "horizon",
    "effect_raw",
    "effect_shrunk_state",
    "cost_bps_resolved",
]


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect phase2_candidates.csv files and summarize blocked conditions, "
            "raw conditions, and key discovery statistics."
        )
    )
    parser.add_argument(
        "--csv",
        help="Inspect a single phase2_candidates.csv file.",
    )
    parser.add_argument(
        "--base-dir",
        default="data/reports/phase2",
        help="Directory scanned recursively for phase2_candidates.csv files when --csv is not set.",
    )
    parser.add_argument(
        "--show-sample",
        type=int,
        default=5,
        help="Number of sample rows to print per file (0 disables samples).",
    )
    parser.add_argument(
        "--list-raw",
        action="store_true",
        help="Print unique condition_raw values seen across scanned files.",
    )
    parser.add_argument(
        "--list-blocked",
        action="store_true",
        help="Print unique blocked condition_raw values (condition == '__BLOCKED__').",
    )
    return parser


def _resolve_targets(csv_path: str | None, base_dir: str) -> list[Path]:
    if csv_path:
        path = Path(csv_path)
        return [path]
    root = Path(base_dir)
    if not root.exists():
        return []
    return sorted(root.rglob("phase2_candidates.csv"))


def _series_mean(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce")
    if series.dropna().empty:
        return None
    return float(series.mean())


def _print_file_summary(path: Path, df: pd.DataFrame, sample_rows: int) -> None:
    total_rows = len(df)
    discovery_rows = (
        int(df["is_discovery"].astype(bool).sum()) if "is_discovery" in df.columns else 0
    )
    blocked_rows = int((df["condition"] == "__BLOCKED__").sum()) if "condition" in df.columns else 0

    print(f"\n=== {path} ===")
    print(f"rows={total_rows} discovery_rows={discovery_rows} blocked_rows={blocked_rows}")

    mean_effect_shrunk = _series_mean(df, "effect_shrunk_state")
    mean_effect_raw = _series_mean(df, "effect_raw")
    mean_cost_bps = _series_mean(df, "cost_bps_resolved")

    if mean_effect_shrunk is not None:
        print(f"mean_effect_shrunk_state={mean_effect_shrunk:.6f}")
    if mean_effect_raw is not None:
        print(f"mean_effect_raw={mean_effect_raw:.6f}")
    if mean_cost_bps is not None:
        print(f"mean_cost_bps_resolved={mean_cost_bps:.6f}")

    if sample_rows > 0 and total_rows > 0:
        sample_cols = [c for c in DEFAULT_SAMPLE_COLUMNS if c in df.columns]
        if sample_cols:
            print("sample:")
            print(df[sample_cols].head(sample_rows).to_string(index=False))


def main() -> int:
    args = _make_parser().parse_args()

    targets = _resolve_targets(args.csv, args.base_dir)
    if not targets:
        print("No phase2 candidate files found.")
        return 0

    blocked_raw_values: set[str] = set()
    all_raw_values: set[str] = set()

    scanned = 0
    for path in targets:
        if not path.exists():
            print(f"Missing file: {path}")
            continue

        try:
            df = pd.read_csv(path)
        except Exception as exc:
            print(f"Failed to read {path}: {exc}")
            continue

        scanned += 1
        _print_file_summary(path, df, max(0, int(args.show_sample)))

        if "condition_raw" in df.columns:
            values = df["condition_raw"].dropna().astype(str)
            all_raw_values.update(values.unique().tolist())

        if {"condition", "condition_raw"}.issubset(df.columns):
            blocked = (
                df[df["condition"].astype(str) == "__BLOCKED__"]["condition_raw"]
                .dropna()
                .astype(str)
                .unique()
            )
            blocked_raw_values.update(blocked.tolist())

    print(f"\nScanned files: {scanned}/{len(targets)}")

    if args.list_raw:
        print("\nAll condition_raw values:")
        for value in sorted(all_raw_values):
            print(value)

    if args.list_blocked:
        print("\nBlocked condition_raw values:")
        for value in sorted(blocked_raw_values):
            print(value)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
