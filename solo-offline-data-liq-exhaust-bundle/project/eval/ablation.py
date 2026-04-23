import argparse
import logging

import numpy as np
import pandas as pd

from project.core.config import get_data_root

DATA_ROOT = get_data_root()

from project.eval.multiplicity import benjamini_hochberg
from project.io.utils import ensure_dir, write_parquet

LOGGER = logging.getLogger(__name__)


def calculate_lift(group_df: pd.DataFrame) -> pd.DataFrame:
    """Compute descriptive per-condition lift vs baseline ('all').

    Requires columns: candidate_id, condition_key, expectancy, n_events.
    Optional column: p_value (inherited per-condition significance metric; defaults to 1.0 if absent).
    Returns descriptive lift columns plus inherited condition-significance columns.
    This does not perform a lift-vs-baseline test, so lift-significance fields are
    emitted as unavailable rather than aliased to condition significance.
    """
    rows = []
    for _, row in group_df.iterrows():
        condition = str(row.get("condition_key", "unknown")).strip()
        rows.append(
            {
                "candidate_id": row["candidate_id"],
                "condition": condition,
                "expectancy": row.get("expectancy", 0.0),
                "n_events": row.get("n_events", 0),
                "p_value": float(row.get("p_value", 1.0)),
            }
        )

    df = pd.DataFrame(rows)
    baseline = df[df["condition"] == "all"]
    if baseline.empty:
        return pd.DataFrame()

    base_exp = baseline["expectancy"].mean()
    out_rows = []
    for _, row in df.iterrows():
        if row["condition"] == "all":
            continue
        lift = row["expectancy"] - base_exp
        lift_pct = (lift / abs(base_exp)) if base_exp != 0 else 0.0
        out_rows.append(
            {
                "candidate_id": row["candidate_id"],
                "condition": row["condition"],
                "baseline_expectancy": base_exp,
                "conditioned_expectancy": row["expectancy"],
                "lift_bps": lift * 10000.0,
                "lift_pct": lift_pct,
                "n_events": row["n_events"],
                "p_value": row["p_value"],
            }
        )

    if not out_rows:
        return pd.DataFrame()

    result = pd.DataFrame(out_rows)
    _BH_ALPHA = 0.10
    _, q_values = benjamini_hochberg(result["p_value"].tolist(), alpha=_BH_ALPHA)
    result["condition_p_value_raw"] = result["p_value"]
    result["condition_q_value"] = q_values
    result["is_condition_discovery"] = result["condition_q_value"] <= _BH_ALPHA
    result["condition_significance_basis"] = "inherited_condition_p_value"
    result["lift_significance_available"] = False
    result["lift_q_value"] = np.nan
    result["is_lift_discovery"] = False
    return result.drop(columns=["p_value"])


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    phase2_root = DATA_ROOT / "reports" / "phase2" / args.run_id
    all_results = []

    if not phase2_root.exists():
        LOGGER.error("No Phase 2 results found at %s", phase2_root)
        return

    for event_dir in phase2_root.iterdir():
        if event_dir.is_dir():
            res_file_parquet = event_dir / "phase2_candidates.parquet"
            res_file_csv = event_dir / "phase2_candidates.csv"
            df = pd.DataFrame()
            if res_file_parquet.exists():
                try:
                    df = pd.read_parquet(res_file_parquet)
                except Exception:
                    df = pd.DataFrame()
            if df.empty and res_file_csv.exists():
                try:
                    df = pd.read_csv(res_file_csv)
                except Exception:
                    df = pd.DataFrame()
            if not df.empty:
                all_results.append(df)

    if not all_results:
        LOGGER.warning("No results loaded.")
        return

    full_df = pd.concat(all_results)
    results = []
    required_cols = [
        "event_type",
        "rule_template",
        "horizon",
        "symbol",
        "conditioning",
        "expectancy",
        "n_events",
    ]
    if not all(col in full_df.columns for col in required_cols):
        LOGGER.error(
            "Missing required columns in Phase 2 results. Available: %s", full_df.columns.tolist()
        )
        return

    full_df["group_key"] = list(
        zip(full_df["event_type"], full_df["rule_template"], full_df["horizon"], full_df["symbol"])
    )

    for key, group in full_df.groupby("group_key"):
        LOGGER.debug(
            "Processing ablation group %s with conditions %s",
            key,
            group["conditioning"].unique().tolist(),
        )
        group_renamed = group.rename(columns={"conditioning": "condition_key"})
        lift_df = calculate_lift(group_renamed)
        if not lift_df.empty:
            lift_df["event_type"] = key[0]
            lift_df["rule"] = key[1]
            lift_df["horizon"] = key[2]
            lift_df["symbol"] = key[3]
            lift_df["bh_group_id"] = "|".join(str(part) for part in key)
            results.append(lift_df)

    if results:
        final_df = pd.concat(results)
        out_dir = DATA_ROOT / "reports" / "ablation" / args.run_id
        ensure_dir(out_dir)
        write_parquet(final_df, out_dir / "ablation_report.parquet")
        final_df.to_csv(out_dir / "lift_summary.csv", index=False)
        LOGGER.info("Ablation report written to %s", out_dir)
        LOGGER.info(
            "Average lift by condition:\n%s",
            final_df.groupby("condition")["lift_bps"].mean().to_markdown(),
        )
    else:
        LOGGER.warning("No lift calculations possible (missing baselines?)")


if __name__ == "__main__":
    main()
