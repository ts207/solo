import argparse
from pathlib import Path
import pandas as pd

from project.research.regime_baselines import (
    load_market_context,
    _context_mask,
    _suppress_overlap,
    REGIME_MATRIX_DEFINITIONS
)

def evaluate_execution_models(features: pd.DataFrame, mask: pd.Series, horizon: int, direction: str) -> dict:
    working = features.copy()
    if working.empty:
        return {}
    
    close = pd.to_numeric(working["close"], errors="coerce")
    future_close = close.shift(-horizon)
    direction_sign = 1.0 if direction == "long" else -1.0
    
    gross_bps = direction_sign * ((future_close / close) - 1.0) * 10_000.0
    
    # Costs
    spread_bps = pd.to_numeric(working.get("spread_bps", 0.0), errors="coerce").clip(lower=0.0)
    
    working["_pos"] = range(len(working))
    working["gross_bps"] = gross_bps
    working["spread_bps"] = spread_bps
    
    # Taker-Taker: pay spread_bps (crossing twice) + 10 bps fee
    working["cost_taker_taker"] = spread_bps + 10.0
    
    # Maker-Taker: enter at mid (no spread paid), exit taker (pay half spread) + 5 bps fee
    # So total cost = (spread_bps / 2) + 5.0
    working["cost_maker_taker"] = (spread_bps / 2.0) + 5.0
    
    # Maker-Maker: enter and exit at mid = 0 spread, 0 fee
    working["cost_maker_maker"] = 0.0
    
    working["net_baseline"] = working["gross_bps"] - working["spread_bps"]
    working["net_taker_taker"] = working["gross_bps"] - working["cost_taker_taker"]
    working["net_maker_taker"] = working["gross_bps"] - working["cost_maker_taker"]
    working["net_maker_maker"] = working["gross_bps"] - working["cost_maker_maker"]
    
    eligible = working[mask & working["gross_bps"].notna() & working["spread_bps"].notna()].copy()
    sampled = _suppress_overlap(eligible, horizon)
    
    if sampled.empty:
        return {}
        
    return {
        "n": len(sampled),
        "mean_gross": sampled["gross_bps"].mean(),
        "mean_spread": sampled["spread_bps"].mean(),
        "net_baseline": sampled["net_baseline"].mean(),
        "net_taker_taker": sampled["net_taker_taker"].mean(),
        "net_maker_taker": sampled["net_maker_taker"].mean(),
        "net_maker_maker": sampled["net_maker_maker"].mean(),
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--source-run-id", required=True)
    args = parser.parse_args()

    data_root = Path(args.data_root)
    
    symbols = ["BTCUSDT", "ETHUSDT"]
    print("Loading features...")
    data = {
        sym: load_market_context(data_root, source_run_id=args.source_run_id, symbol=sym, timeframe="5m")
        for sym in symbols
    }
    
    out_dir = data_root / "reports" / "execution_cost_autopsy"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    out_file = out_dir / "execution_cost_autopsy.md"
    
    with open(out_file, "w") as f:
        f.write("# Execution Cost Autopsy\n\n")
        
        f.write("## 1. Global vs Regime Spread Concentration\n\n")
        f.write("| Symbol | Global Mean Spread | Crisis (forced_flow) Mean Spread | Compression (vol_release) Mean Spread |\n")
        f.write("|---|---|---|---|\n")
        
        for sym in symbols:
            df = data[sym]
            global_spread = pd.to_numeric(df.get("spread_bps", pd.Series()), errors="coerce").mean()
            
            # Crisis regime
            crisis_filters = REGIME_MATRIX_DEFINITIONS["forced_flow_crisis_v1"][0]
            mask_crisis, _ = _context_mask(df, crisis_filters)
            crisis_spread = df[mask_crisis]["spread_bps"].mean() if mask_crisis is not None else float('nan')
            
            # Compression regime
            comp_filters = REGIME_MATRIX_DEFINITIONS["volatility_compression_release_v1"][0]
            mask_comp, _ = _context_mask(df, comp_filters)
            comp_spread = df[mask_comp]["spread_bps"].mean() if mask_comp is not None else float('nan')
            
            f.write(f"| {sym} | {global_spread:.2f} bps | {crisis_spread:.2f} bps | {comp_spread:.2f} bps |\n")

        f.write("\n## 2. Maker vs Taker Execution Dynamics\n\n")
        f.write("Evaluated on `forced_flow_crisis_v1` primary regime.\n\n")
        f.write("| Symbol | Dir | Hz | Gross | Baseline Spread | Taker-Taker Net | Maker-Taker Net | Maker-Maker Net |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        
        crisis_filters = REGIME_MATRIX_DEFINITIONS["forced_flow_crisis_v1"][0]
        for sym in symbols:
            df = data[sym]
            mask, _ = _context_mask(df, crisis_filters)
            for direction in ["long", "short"]:
                for hz in [12, 24, 48]:
                    res = evaluate_execution_models(df, mask, hz, direction)
                    if res:
                        f.write(f"| {sym} | {direction} | {hz} "
                                f"| {res['mean_gross']:.2f} "
                                f"| {res['mean_spread']:.2f} "
                                f"| {res['net_taker_taker']:.2f} "
                                f"| {res['net_maker_taker']:.2f} "
                                f"| {res['net_maker_maker']:.2f} |\n")

        f.write("\n## 3. Friction Filtering (Rescuing Crisis Rows)\n\n")
        f.write("Re-evaluating `forced_flow_crisis_v1` with `execution_friction=normal` appended.\n\n")
        f.write("| Symbol | Dir | Hz | N | Gross | Spread | Net Baseline |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        
        rescued_filters = dict(crisis_filters)
        rescued_filters["execution_friction"] = "normal"
        
        for sym in symbols:
            df = data[sym]
            mask, _ = _context_mask(df, rescued_filters)
            for direction in ["long", "short"]:
                for hz in [12, 24, 48]:
                    res = evaluate_execution_models(df, mask, hz, direction)
                    if res:
                        f.write(f"| {sym} | {direction} | {hz} | {res['n']} "
                                f"| {res['mean_gross']:.2f} "
                                f"| {res['mean_spread']:.2f} "
                                f"| {res['net_baseline']:.2f} |\n")
                        
    print(f"Wrote execution cost autopsy to {out_file}")

if __name__ == "__main__":
    main()
