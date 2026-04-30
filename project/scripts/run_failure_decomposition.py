import argparse
from pathlib import Path

from project.research.regime_baselines import (
    load_market_context,
    REGIME_MATRIX_DEFINITIONS,
    REGIME_MATRIX_PROPOSAL_ELIGIBILITY
)
from project.research.failure_decomposition import analyze_failure_regime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--source-run-id", required=True)
    args = parser.parse_args()

    data_root = Path(args.data_root)
    source_run_id = args.source_run_id

    mechanisms = {
        "funding_squeeze_positioning_v1": "funding_squeeze",
        "forced_flow_crisis_v1": "forced_flow_reversal",
        "volatility_compression_release_v1": "volatility_compression_release"
    }

    symbols = ["BTCUSDT", "ETHUSDT"]
    horizons = [12, 24, 48]
    directions = ["long", "short"]

    print("Loading data...")
    features_by_symbol = {
        symbol: load_market_context(
            data_root,
            source_run_id=source_run_id,
            symbol=symbol,
            timeframe="5m",
        ) for symbol in symbols
    }

    results = []

    for matrix_id, mechanism_id in mechanisms.items():
        print(f"Analyzing {matrix_id}...")
        matrix = REGIME_MATRIX_DEFINITIONS.get(matrix_id, [])
        eligibility = REGIME_MATRIX_PROPOSAL_ELIGIBILITY.get(matrix_id, [])
        
        for i, filters in enumerate(matrix):
            if i < len(eligibility) and eligibility[i]:
                for symbol in symbols:
                    features = features_by_symbol[symbol]
                    for direction in directions:
                        for horizon in horizons:
                            res = analyze_failure_regime(features, filters, symbol, direction, horizon)
                            if res:
                                res["mechanism_id"] = mechanism_id
                                results.append(res)
                                
    out_dir = data_root / "reports" / "portfolio_autopsy"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    with open(out_dir / "failure_decomposition.md", "w") as f:
        f.write("# Failure Decomposition\n\n")
        f.write("This report decomposes the exact failure vector for each primary regime.\n\n")
        
        for mechanism_id in mechanisms.values():
            f.write(f"## {mechanism_id}\n\n")
            f.write("| regime_id | symbol | direction | horizon | mean_gross_bps | mean_cost_bps | mean_net_bps | cost_share | lag_0_net | lag_1_net | lag_2_net | year_stats | classification |\n")
            f.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
            
            mech_results = [r for r in results if r["mechanism_id"] == mechanism_id]
            for r in mech_results:
                f.write(f"| `{r['regime_id']}` "
                        f"| {r['symbol']} "
                        f"| {r['direction']} "
                        f"| {r['horizon']} "
                        f"| {r['mean_gross_bps']:.2f} "
                        f"| {r['mean_cost_bps']:.2f} "
                        f"| {r['mean_net_bps']:.2f} "
                        f"| {r['cost_share_of_gross']:.2f} "
                        f"| {r['entry_lag_0_net']:.2f} "
                        f"| {r['entry_lag_1_net']:.2f} "
                        f"| {r['entry_lag_2_net']:.2f} "
                        f"| {r['year_stats']} "
                        f"| **{r['classification']}** |\n")
            f.write("\n")
            
    print("Failure decomposition report generated at data/reports/portfolio_autopsy/failure_decomposition.md")

if __name__ == "__main__":
    main()
