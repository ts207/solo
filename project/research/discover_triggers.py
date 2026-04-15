import argparse
import logging
from pathlib import Path

import pandas as pd

from project import PROJECT_ROOT
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.trigger_discovery.candidate_generation import (
    generate_parameter_sweep,
    generate_feature_clusters,
    TriggerFeatureColumns,
)
from project.research.trigger_discovery.candidate_scoring import score_trigger_candidates
from project.research.trigger_discovery.proposal_emission import emit_proposals
from project.research.search.search_feature_utils import prepare_search_features_for_symbol

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Advanced Trigger Discovery Lane (Phase 6)")
    parser.add_argument("--mode", type=str, required=True, choices=["parameter_sweep", "feature_cluster"])
    parser.add_argument("--family", type=str, default="vol_shock", help="Detector family for parameter sweep")
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--timeframe", type=str, default="5m")
    parser.add_argument("--data_root", type=str, default=str(PROJECT_ROOT.parent / "data"))
    parser.add_argument("--out_dir", type=str, default=str(PROJECT_ROOT.parent / "data" / "trigger_proposals"))

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    data_root = Path(args.data_root)
    out_dir = Path(args.out_dir)

    log.info(f"Preparing advanced trigger search features for {args.symbol} ({args.timeframe})")
    
    # Normally we load the actual extracted features. 
    # For Phase 6 pipeline contract, we depend on the same API as Phase 2
    try:
        features = prepare_search_features_for_symbol(
            run_id="trigger_discovery_run",
            symbol=args.symbol,
            timeframe=args.timeframe,
            data_root=data_root,
            expected_event_ids=[]
        )
    except Exception as e:
        log.warning(f"Could not fully load canonical features: {e}. Falling back to empty structure.")
        features = pd.DataFrame()

    if args.mode == "parameter_sweep":
        log.info(f"Running parameter sweep over family: {args.family}")
        # Build grid (in real system, would parse from registry boundaries)
        grid = {
            "z_threshold": [1.5, 2.0, 2.5, 3.0],
            "lookback_bars": [48, 96, 288]
        }
        proposals, trigger_cols = generate_parameter_sweep(
            features,
            family_grid={args.family: grid}
        )
    elif args.mode == "feature_cluster":
        log.info("Running feature excursion mining (clustering)")
        target_cols = [c for c in features.columns if "vol" in c.lower() or "liq" in c.lower() or "spread" in c.lower()]
        if not target_cols:
            log.warning("No target continuous columns detected. Using dummies.")

        proposals, trigger_cols = generate_feature_clusters(
            features,
            target_columns=target_cols,
            min_support=10
        )
    else:
        proposals, trigger_cols = [], TriggerFeatureColumns()

    log.info(f"Generated {len(proposals)} candidate trigger proposals.")

    # Reconstruct augmented features explicitly within the trigger-discovery path.
    # This prevents synthetic trigger columns from reaching the main discovery pipeline
    # even if this function's return value is accidentally passed upstream.
    augmented_features = trigger_cols.apply_to_features(features)

    if augmented_features.empty or not proposals:
        log.warning("No proposals generated or features empty. Ending discovery.")
        return
        
    # Generate evaluation boundaries using walkforward logic (reuses Phase 2 logic)
    ts = augmented_features["timestamp"] if "timestamp" in augmented_features.columns else None
    folds = []

    log.info("Scoring proposals using canonical evidence pipeline (Fold Stability / Significance)...")
    scored_df = score_trigger_candidates(proposals, augmented_features, folds=folds)
    
    emit_proposals(scored_df, out_dir)


if __name__ == "__main__":
    main()
