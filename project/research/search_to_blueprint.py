"""
Unified Alpha Pipeline: From Phased Search to Executable Blueprints.

This script unifies the new layered search system with the production strategy
compilation pipeline, solving the 'dead-end' integration gap.

Flow:
1. Generator: Expand spec/search/search_full.yaml
2. Evaluator: Compute rich metrics (MAE, MFE, Robustness, Capacity)
3. Adapter: Map to 40-column production schema
4. Multiplicity: Apply FDR control (BH procedure)
5. Promotion: Select best candidates
6. Compilation: Generate Strategy Blueprints
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from project.io.utils import write_parquet
from project.research.multiplicity import apply_multiplicity_controls
from project.research.search.bridge_adapter import hypotheses_to_bridge_candidates
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.search.generator import generate_hypotheses

# Handle optional dependencies for PoC
try:
    from project.research.promotion.blueprint_promotion import promote_candidates
except ImportError:
    log = logging.getLogger(__name__)
    log.warning("Could not import promote_candidates (likely missing statsmodels). Using mock.")

    def promote_candidates(df: pd.DataFrame) -> pd.DataFrame:
        # Mock: just return top 5 by t-stat
        if df.empty:
            return df
        return df.sort_values("t_stat", ascending=False).head(5)


try:
    from project.research.blueprint_compilation import compile_blueprint
except ImportError:
    log = logging.getLogger(__name__)
    log.warning("Could not import compile_blueprint. Using mock.")

    def compile_blueprint(*args, **kwargs):
        # Mock: return a dummy object with to_dict
        class MockBP:
            def to_dict(self):
                return {"status": "mock_blueprint"}

        return MockBP(), 1


log = logging.getLogger(__name__)


def run_alpha_pipeline(
    features_path: Path,
    search_spec: str = "full",
    output_dir: Path = Path("artifacts/search_to_blueprint"),
):
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Load Data
    log.info("Loading features from %s", features_path)
    features = pd.read_parquet(features_path)

    # 2. Generate Hypotheses
    log.info("Generating hypotheses from spec '%s'", search_spec)
    hypotheses = generate_hypotheses(search_spec)
    log.info("Generated %d hypotheses", len(hypotheses))

    # 3. Evaluate
    log.info("Evaluating hypotheses...")
    metrics_df = evaluate_hypothesis_batch(hypotheses, features, use_context_quality=True)
    write_parquet(metrics_df, output_dir / f"raw_metrics_{run_id}.parquet")

    # 4. Map to Bridge Schema
    log.info("Mapping to bridge schema...")
    candidates = hypotheses_to_bridge_candidates(metrics_df)
    if candidates.empty:
        log.warning("No candidates passed initial evaluation gates.")
        return

    # 5. Multiplicity Gating (FDR Control)
    log.info("Applying multiplicity gating...")
    # Add p-values if missing (simplified from t-stat)
    from project.research.gating import one_sided_p_from_t

    candidates["p_value"] = [
        one_sided_p_from_t(row["t_stat"], row["n"] - 1) for _, row in candidates.iterrows()
    ]
    candidates = apply_multiplicity_controls(candidates, max_q=0.05, mode="research")

    # 6. Promotion
    log.info("Promoting candidates...")
    survivors = promote_candidates(candidates)
    log.info("%d candidates survived promotion", len(survivors))

    # 7. Compilation
    log.info("Compiling blueprints...")
    blueprints = []
    for _, row in survivors.iterrows():
        try:
            # compile_blueprint expects a Dict
            merged_row = row.to_dict()
            # Add missing fields needed for compilation
            merged_row["symbol"] = "BTCUSDT"  # Fallback if missing

            bp, lag = compile_blueprint(
                merged_row,
                run_id=run_id,
                run_symbols=["BTCUSDT"],
                stats={},  # evaluator produces scalar stats in row
                fees_bps=1.0,
                slippage_bps=1.0,
                ontology_spec_hash_value="redesign_v1",
                cost_config_digest="standard",
            )
            blueprints.append(bp)
        except Exception as e:
            log.error("Failed to compile blueprint for %s: %s", row["candidate_id"], e)

    # Save Blueprints
    bp_path = output_dir / f"blueprints_{run_id}.json"
    with open(bp_path, "w") as f:
        import json

        json.dump([bp.to_dict() for bp in blueprints], f, indent=2)

    log.info("Pipeline complete. Saved %d blueprints to %s", len(blueprints), bp_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Example usage (would be called with actual paths in production)
    # run_alpha_pipeline(Path("data/lake/features_15m.parquet"))
