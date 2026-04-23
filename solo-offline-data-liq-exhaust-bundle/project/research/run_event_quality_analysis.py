# project/research/run_event_quality_analysis.py
"""
Event Quality Analysis Pipeline Runner.

Runs all four event quality analyses against a features DataFrame and
writes results to an output directory.

Usage (CLI):
    python project/research/run_event_quality_analysis.py \
        --run_id <run_id> \
        --symbol BTCUSDT \
        --output_dir data/reports/event_quality/my_run

Usage (library):
    from project.research.run_event_quality_analysis import run_event_quality_analysis
    summary = run_event_quality_analysis(features_df, output_dir=Path("..."))
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from project.research.event_quality.firing_rate import compute_firing_rates
from project.research.event_quality.cooccurrence import compute_cooccurrence

from project.research.event_quality.information_gain import compute_information_gain
from project.research.event_quality.lead_lag import (
    compute_event_return_lead_lag,
    compute_event_event_lead_lag,
)

log = logging.getLogger(__name__)


def run_event_quality_analysis(
    features: pd.DataFrame,
    *,
    output_dir: Path,
    min_n: int = 100,
    cooccurrence_window: int = 5,
    redundancy_threshold: float = 0.5,
    ig_horizon_bars: int = 12,
    lead_lag_horizons: list[int] | None = None,
    lead_lag_max_lag: int = 24,
) -> Dict[str, Any]:
    """
    Run all event quality analyses on features DataFrame.

    Parameters
    ----------
    features : wide feature DataFrame with 'close', 'timestamp', event_* columns
    output_dir : directory to write CSV and JSON results
    min_n : minimum event fires for statistical reliability
    cooccurrence_window : bars window for co-occurrence (±window)
    redundancy_threshold : p_b_given_a threshold for flagging redundancy
    ig_horizon_bars : forward horizon for information gain computation
    lead_lag_horizons : forward horizons for event-return lead-lag
    lead_lag_max_lag : max lag for event-event lead-lag

    Returns
    -------
    Summary dict written to summary.json
    """
    if lead_lag_horizons is None:
        lead_lag_horizons = [3, 6, 12, 24, 48]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Running firing rate audit...")
    firing = compute_firing_rates(features, min_n=min_n)
    firing.to_csv(output_dir / "firing_rates.csv", index=False)

    log.info("Running co-occurrence analysis...")
    cooc = compute_cooccurrence(
        features, window_bars=cooccurrence_window, redundancy_threshold=redundancy_threshold
    )
    cooc.to_csv(output_dir / "cooccurrence.csv", index=False)

    log.info("Running information gain analysis...")
    ig = compute_information_gain(features, horizon_bars=ig_horizon_bars, min_fires=min_n)
    ig.to_csv(output_dir / "information_gain.csv", index=False)

    log.info("Running event-return lead-lag analysis...")
    ret_ll = compute_event_return_lead_lag(features, horizons=lead_lag_horizons)
    if not ret_ll.empty:
        ret_ll.to_csv(output_dir / "event_return_lead_lag.csv", index=False)

    log.info("Running event-event lead-lag analysis...")
    evt_ll = compute_event_event_lead_lag(features, max_lag=lead_lag_max_lag)
    if not evt_ll.empty:
        evt_ll.to_csv(output_dir / "event_event_lead_lag.csv", index=False)

    # Build summary
    below_min_n = []
    if not firing.empty:
        below = firing[firing["below_min_n"]]
        below_min_n = below[["event_id", "n_fires", "events_per_day"]].to_dict("records")

    top_redundancy = []
    if not cooc.empty:
        candidates = cooc[cooc["redundancy_candidate"]].head(20)
        top_redundancy = candidates[["event_a", "event_b", "p_b_given_a", "n_co_fires"]].to_dict(
            "records"
        )

    top_ig = []
    bottom_ig = []
    if not ig.empty:
        valid_ig = ig.dropna(subset=["ig_bits"])
        top_ig = valid_ig.head(10)[["event_id", "ig_bits", "n_fires"]].to_dict("records")
        bottom_ig = valid_ig.tail(10)[["event_id", "ig_bits", "n_fires"]].to_dict("records")

    summary: Dict[str, Any] = {
        "n_events_analyzed": len(firing) if not firing.empty else 0,
        "n_bars": len(features),
        "below_min_n_events": below_min_n,
        "top_redundancy_pairs": top_redundancy,
        "top_ig_events": top_ig,
        "bottom_ig_events": bottom_ig,
        "parameters": {
            "min_n": min_n,
            "cooccurrence_window": cooccurrence_window,
            "redundancy_threshold": redundancy_threshold,
            "ig_horizon_bars": ig_horizon_bars,
        },
    }

    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    log.info("Event quality analysis complete. Results written to %s", output_dir)
    return summary


def main() -> None:
    from project.research.helpers.loading import load_research_features

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Event quality analysis pipeline")
    parser.add_argument("--run_id", default="", help="Run ID for feature loading")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--min_n", type=int, default=100)
    parser.add_argument("--cooccurrence_window", type=int, default=5)
    parser.add_argument("--redundancy_threshold", type=float, default=0.5)
    parser.add_argument("--ig_horizon_bars", type=int, default=12)
    args = parser.parse_args()

    features = load_research_features(args.run_id, args.symbol, args.timeframe)
    if features.empty:
        logging.error("No features loaded for run_id=%s symbol=%s", args.run_id, args.symbol)
        sys.exit(1)

    run_event_quality_analysis(
        features,
        output_dir=Path(args.output_dir),
        min_n=args.min_n,
        cooccurrence_window=args.cooccurrence_window,
        redundancy_threshold=args.redundancy_threshold,
        ig_horizon_bars=args.ig_horizon_bars,
    )


if __name__ == "__main__":
    main()
