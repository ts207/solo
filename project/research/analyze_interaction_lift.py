from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from project.core.config import get_data_root

try:
    from scipy import stats
except ModuleNotFoundError:  # pragma: no cover - environment-specific fallback
    from project.core.stats import stats

from project.io.utils import ensure_dir, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

AGGREGATED_HYPOTHESIS_ID = "AGGREGATED"


def _load_phase2_data(run_id: str) -> pd.DataFrame:
    DATA_ROOT = get_data_root()
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        logging.warning("Phase 2 root not found: %s", phase2_root)
        return pd.DataFrame()

    csv_files = list(phase2_root.glob("**/phase2_candidates.parquet"))
    if not csv_files:
        return pd.DataFrame()

    dfs = []
    for f in csv_files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            logging.error("Failed to read %s: %s", f, e)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def _resolve_hypothesis_id(row: pd.Series) -> str:
    raw = str(row.get("hypothesis_id", "")).strip()
    if raw:
        return raw
    for fallback_col in ("template_id", "template_verb", "candidate_id"):
        fallback = str(row.get(fallback_col, "")).strip()
        if fallback:
            return fallback
    return AGGREGATED_HYPOTHESIS_ID


def calculate_lift(df: pd.DataFrame, min_trades: int = 30) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = {
        "event_type",
        "symbol",
        "horizon",
        "condition_signature",
        "expectancy",
        "std_return",
        "sample_size",
    }
    if not required.issubset(df.columns):
        return pd.DataFrame()

    # Filter for candidates with enough data.
    df = df[df["sample_size"] >= min_trades].copy()
    if df.empty:
        return pd.DataFrame()

    # Normalize hypothesis identity to avoid empty identifiers in artifacts.
    df["hypothesis_id"] = df.apply(_resolve_hypothesis_id, axis=1)

    # Group by (Event Type, Hypothesis, Symbol, Horizon) to find base vs conditioned.
    # condition_signature "all" is the baseline.
    group_cols = ["event_type", "hypothesis_id", "symbol", "horizon"]

    lift_rows = []

    for keys, sub in df.groupby(group_cols):
        event_type, hyp_id, symbol, horizon = keys

        # Find baseline row
        base_row = sub[sub["condition_signature"] == "all"]
        if base_row.empty:
            continue

        base_exp = float(base_row["expectancy"].iloc[0])
        base_std = float(base_row["std_return"].iloc[0])
        base_n = int(base_row["sample_size"].iloc[0])

        # Compare every other condition to baseline
        conditioned = sub[sub["condition_signature"] != "all"]

        for _, row in conditioned.iterrows():
            cond_sig = str(row["condition_signature"])
            cond_exp = float(row["expectancy"])
            cond_std = float(row["std_return"])
            cond_n = int(row["sample_size"])

            expectancy_lift = (cond_exp - base_exp) * 10000  # bps

            # Welch's t-test for difference in means (unequal variance/sample size)
            # t = (m1 - m2) / sqrt(s1^2/n1 + s2^2/n2)
            denom = np.sqrt((base_std**2 / base_n) + (cond_std**2 / cond_n))
            if denom > 0:
                t_stat = (cond_exp - base_exp) / denom
                # Degrees of freedom approximation
                df_welch = (
                    ((base_std**2 / base_n + cond_std**2 / cond_n) ** 2)
                    / (
                        (base_std**2 / base_n) ** 2 / (base_n - 1)
                        + (cond_std**2 / cond_n) ** 2 / (cond_n - 1)
                    )
                    if base_n > 1 and cond_n > 1
                    else 1
                )
                # Route through project.core.stats consistency helper (Finding 94)
                # stats here is already mapped to project.core.stats if scipy is missing
                p_val = 2 * (1 - stats.t.cdf(abs(t_stat), df_welch))
            else:
                t_stat = 0.0
                p_val = 1.0

            lift_rows.append(
                {
                    "event_type": event_type,
                    "hypothesis_id": hyp_id,
                    "symbol": symbol,
                    "horizon": horizon,
                    "condition": cond_sig,
                    "base_expectancy_bps": base_exp * 10000,
                    "cond_expectancy_bps": cond_exp * 10000,
                    "expectancy_lift_bps": expectancy_lift,
                    "base_n": base_n,
                    "cond_n": cond_n,
                    "t_stat": t_stat,
                    "p_value": p_val,
                    "is_significant": bool(p_val <= 0.05),
                }
            )

    out = pd.DataFrame(lift_rows)
    if out.empty:
        return out
    # Guard contract drift when upstream emits duplicate candidate rows.
    return out.drop_duplicates(ignore_index=True)


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(description="Analyze marginal lift of context states.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--min_trades", type=int, default=30)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    out_dir = (
        Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "interactions" / run_id
    )
    ensure_dir(out_dir)

    manifest = start_manifest("analyze_interaction_lift", run_id, vars(args), [], [])

    try:
        df = _load_phase2_data(run_id)
        if df.empty:
            logging.error("No Phase 2 data found for run %s", run_id)
            finalize_manifest(manifest, "failed", error="No Phase 2 data")
            return 1

        lift_df = calculate_lift(df, min_trades=args.min_trades)

        if lift_df.empty:
            logging.warning("No lift calculated (insufficient samples or missing baselines)")
            finalize_manifest(manifest, "success", stats={"rows": 0})
            return 0

        parquet_path = out_dir / "lift_analysis.parquet"
        write_parquet(lift_df, parquet_path)

        csv_path = out_dir / "lift_analysis.csv"
        lift_df.to_csv(csv_path, index=False)

        # Generate Markdown summary
        md_path = out_dir / "top_lifts.md"
        top_lifts = lift_df.sort_values("expectancy_lift_bps", ascending=False).head(20)

        with md_path.open("w", encoding="utf-8") as f:
            f.write(f"# Top Interaction Lifts - Run: {run_id}\n\n")
            f.write("| Event | Hypothesis | State | Symbol | Lift (bps) | P-Val | Sig |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for _, row in top_lifts.iterrows():
                f.write(
                    f"| {row['event_type']} | {row['hypothesis_id']} | {row['condition']} | "
                    f"{row['symbol']} | {row['expectancy_lift_bps']:.2f} | {row['p_value']:.4f} | "
                    f"{'Y' if row['is_significant'] else 'N'} |\n"
                )

        finalize_manifest(
            manifest,
            "success",
            stats={"rows": len(lift_df), "significant_lifts": int(lift_df["is_significant"].sum())},
        )
        return 0
    except Exception as exc:
        logging.exception("Lift analysis failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
