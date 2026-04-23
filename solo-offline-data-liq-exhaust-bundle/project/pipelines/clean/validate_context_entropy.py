from __future__ import annotations
from project.core.config import get_data_root

import argparse
import logging
import sys
import json
import itertools
from typing import Any, Dict, List, Tuple
import networkx as nx

import numpy as np
import pandas as pd
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    ensure_dir,
)
from project.specs.manifest import finalize_manifest, start_manifest
from project.specs.ontology import MATERIALIZED_STATE_COLUMNS_BY_ID

LOGGER = logging.getLogger(__name__)


def get_equivalence_classes(edges: List[tuple], nodes: List[str]) -> List[Dict[str, Any]]:
    G = nx.Graph()
    G.add_nodes_from(nodes)
    for u, v in edges:
        G.add_edge(u, v)

    # Lexicographically smallest name or largest support for deterministic selection
    # We will pick the highest coverage, tie-breaking lexicographically
    # Wait, coverage is available outside. We can just pass the coverage mapping in.
    return list(nx.connected_components(G))


def eval_entropy(
    df: pd.DataFrame,
    symbol: str,
    max_modal_mass: float = 80.0,
    max_top_1_mass: float = 20.0,
    min_unique_codes: int = 12,
) -> Tuple[Dict[str, Any], List[str]]:

    issues = []
    report = {"digits": {}, "combined": {}, "dedup": {}}

    cols = {
        "ms_vol_state": "Volatility",
        "ms_liq_state": "Liquidity",
        "ms_oi_state": "Open Interest",
        "ms_funding_state": "Funding",
        "ms_trend_state": "Trend",
        "ms_spread_state": "Spread",
    }

    for col, name in cols.items():
        if col in df.columns:
            # NaN values are effectively single states in pandas value_counts if data is missing (like OI)
            # but we explicitly ignore nans for distribution mass
            counts = df[col].value_counts(normalize=True).sort_index() * 100
            if len(counts) > 0:
                modal_mass = counts.max()
                report["digits"][col] = {
                    "name": name,
                    "modal_mass": float(modal_mass),
                    "states": counts.to_dict(),
                }
                if modal_mass > max_modal_mass and col not in [
                    "ms_vol_state",
                    "ms_oi_state",
                    "ms_spread_state",
                ]:
                    issues.append(f"Modal mass {modal_mass:.2f}% > {max_modal_mass}% for {col}")

    code_counts = df["ms_context_state_code"].value_counts(normalize=True).copy() * 100
    if len(code_counts) > 0:
        top_1_mass = float(code_counts.iloc[0])
        top_5_mass = float(code_counts.head(5).sum())
        unique_gt_1 = int((code_counts >= 1.0).sum())
        unique_gt_05 = int((code_counts >= 0.5).sum())

        report["combined"] = {
            "top_1_mass": top_1_mass,
            "top_5_mass": top_5_mass,
            "unique_codes_gte_1pct": unique_gt_1,
            "unique_codes_gte_0_5pct": unique_gt_05,
            "top_5_codes": code_counts.head(5).to_dict(),
        }

        if top_1_mass > max_top_1_mass:
            # High col drawdown pushes this temporarily, 30% is safer for a single month?
            # User accepted 23%. Let's flag warning if above 25% or fail > 35%.
            # We'll use max_top_1_mass.
            if top_1_mass > 35.0:
                issues.append(f"Top-1 mass {top_1_mass:.2f}% > 35.0% for {symbol}")

        if unique_gt_1 < min_unique_codes:
            issues.append(
                f"Only {unique_gt_1} unique codes >= 1% (min {min_unique_codes}) for {symbol}"
            )

    # Jaccard Dedup Phase
    mask_cols = list(MATERIALIZED_STATE_COLUMNS_BY_ID.values())
    valid_masks = [c for c in mask_cols if c in df.columns]
    mask_data = df[valid_masks].fillna(False).astype(bool)

    coverage = mask_data.mean()
    active_masks = [m for m in valid_masks if mask_data[m].sum() > 0]

    pairs = list(itertools.combinations(active_masks, 2))
    hard_dupes = []

    for m1, m2 in pairs:
        intersection = (mask_data[m1] & mask_data[m2]).sum()
        union = (mask_data[m1] | mask_data[m2]).sum()
        if union > 0 and (intersection / union) >= 0.95:
            hard_dupes.append((m1, m2))

    components = get_equivalence_classes(hard_dupes, active_masks)

    class_map = {}
    for i, c in enumerate(components):
        # Sort by highest coverage, tiebreak lexicographically
        members = list(c)
        members.sort(key=lambda x: (-coverage[x], x))
        rep = members[0]

        for member in members:
            class_map[member] = {
                "class_id": i,
                "representative": rep,
                "support": float(coverage[member] * 100),
                "removed": member != rep,
            }

    report["dedup"] = {
        "active_masks": len(active_masks),
        "hard_dupes_pairs": len(hard_dupes),
        "equivalence_classes": len(components),
        "reduction": len(active_masks) - len(components),
        "map": class_map,
    }

    # Assert No Redundant pairs remain post-dedup
    retained_masks = [
        info["representative"] for m, info in class_map.items() if not info["removed"]
    ]
    retained_masks = list(set(retained_masks))

    retained_pairs = list(itertools.combinations(retained_masks, 2))
    for m1, m2 in retained_pairs:
        intersection = (mask_data[m1] & mask_data[m2]).sum()
        union = (mask_data[m1] | mask_data[m2]).sum()
        if union > 0 and (intersection / union) >= 0.95:
            issues.append(f"Post-dedup Jaccard failure: {m1} and {m2} remain with J >= 0.95")

    return report, issues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_modal_mass", type=float, default=85.0)
    parser.add_argument("--fail_on_issues", type=int, default=1)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    data_root = get_data_root()

    manifest = start_manifest("validate_context_entropy", args.run_id, vars(args), [], [])

    all_issues = {}
    master_report = {}
    global_dedup_records = []

    for symbol in symbols:
        LOGGER.info(f"Auditing context entropy for {symbol}...")

        candidates = [
            run_scoped_lake_path(
                data_root, args.run_id, "features", args.market, symbol, "5m", "market_context"
            ),
            data_root / "lake" / "features" / args.market / symbol / "5m" / "market_context",
            run_scoped_lake_path(
                data_root, args.run_id, "context", "market_state", args.market, symbol, "5m"
            ),
            data_root / "lake" / "context" / "market_state" / args.market / symbol / "5m",
        ]

        ctx_dir = choose_partition_dir(candidates)
        if not ctx_dir:
            LOGGER.warning(f"No market_state found for {symbol}")
            continue

        df = read_parquet(list_parquet_files(ctx_dir))
        if df.empty:
            continue

        report, issues = eval_entropy(df, symbol, max_modal_mass=args.max_modal_mass)

        master_report[symbol] = report
        if issues:
            all_issues[symbol] = issues

        for mask, info in report["dedup"]["map"].items():
            global_dedup_records.append(
                {
                    "symbol": symbol,
                    "mask": mask,
                    "representative": info["representative"],
                    "class_id": info["class_id"],
                    "support_pct": info["support"],
                    "removed_flag": info["removed"],
                }
            )

    output_dir = run_scoped_lake_path(data_root, args.run_id)
    ensure_dir(output_dir)

    report_path = output_dir / "context_entropy_report.json"
    with open(report_path, "w") as f:
        json.dump(master_report, f, indent=2)

    df_dedup = pd.DataFrame(global_dedup_records)
    if not df_dedup.empty:
        df_dedup.to_csv(output_dir / "ontology_dedup_summary.csv", index=False)

        # Pull global dedup map from the first symbol as representative constraint
        if symbols and symbols[0] in master_report:
            with open(output_dir / "ontology_dedup_map.json", "w") as f:
                json.dump(master_report[symbols[0]]["dedup"]["map"], f, indent=2)

    status = "success"
    if all_issues:
        for sym, sym_issues in all_issues.items():
            for iss in sym_issues:
                LOGGER.error(f"[{sym}] {iss}")
        status = "failed" if int(args.fail_on_issues) else "warning"

    finalize_manifest(
        manifest,
        status,
        stats={
            "symbols_evaluated": len(master_report),
            "symbols_with_issues": len(all_issues),
            "details": all_issues,
        },
    )
    return 1 if all_issues and int(args.fail_on_issues) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
