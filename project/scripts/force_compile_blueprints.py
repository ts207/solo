import json

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.research.blueprint_compilation import compile_blueprint
from project.specs.ontology import ontology_spec_hash


def main():
    data_root = get_data_root()

    selected = [
        ("synthetic_2025_full_year_v9", "BTCUSDT::hyp_1750dbb1563b501aceb9"),
        ("synthetic_2025_full_year_v9", "SOLUSDT::hyp_1185ad1b630e353df2f9"),  # Changed from v10
        ("synthetic_2025_stress_crash_v4", "ETHUSDT::hyp_9cd1674f4a0833aaf9aa"),
        ("synthetic_2025_stress_crash", "BTCUSDT::hyp_193d04253dd22a18bed7"),
    ]

    blueprints = []
    ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)

    for run_id, candidate_id in selected:
        path = data_root / "reports" / "phase2" / run_id / "search_engine" / "phase2_candidates.parquet"
        if not path.exists():
            path = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"

        df = pd.read_parquet(path)
        row = df[df["candidate_id"] == candidate_id].iloc[0].to_dict()
        row["source_path"] = str(path)

        bp, _ = compile_blueprint(
            merged_row=row,
            run_id=run_id,
            run_symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            stats={},
            fees_bps=0.5,
            slippage_bps=0.5,
            ontology_spec_hash_value=ontology_hash,
            cost_config_digest="synthetic",
        )
        blueprints.append(bp)

    out_dir = data_root / "reports" / "strategy_blueprints" / "multi_edge_portfolio"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = out_dir / "blueprints.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for bp in blueprints:
            f.write(json.dumps(bp.to_dict(), sort_keys=True) + "\n")

    print(f"Compiled {len(blueprints)} blueprints to {out_jsonl}")

if __name__ == "__main__":
    main()
