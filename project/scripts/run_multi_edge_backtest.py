import json
import pandas as pd
import numpy as np
from pathlib import Path
from project.engine.runner import run_engine
from project.core.config import get_data_root

DATA_ROOT = get_data_root()

blueprints_file = (
    DATA_ROOT / "reports" / "strategy_blueprints" / "multi_edge_portfolio" / "blueprints.jsonl"
)
blueprints = []
with blueprints_file.open("r", encoding="utf-8") as f:
    for line in f:
        blueprints.append(json.loads(line))

strategies = []
params_by_strategy = {}

for i, bp in enumerate(blueprints):
    strat_name = f"dsl_interpreter_v1__edge_{i}"
    strategies.append(strat_name)
    params_by_strategy[strat_name] = {"dsl_blueprint": bp, "event_feature_ffill_bars": 12}

print(f"Running backtest for {len(strategies)} strategies.")

# Use the full year 2025 run for data
run_id = "synthetic_2025_full_year"
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Define start/end to cover full year
start_ts = pd.Timestamp("2025-01-01", tz="UTC")
end_ts = pd.Timestamp("2025-12-31", tz="UTC")

results = run_engine(
    run_id="multi_edge_portfolio_backtest",
    symbols=symbols,
    strategies=strategies,
    params={
        "allocator_mode": "heuristic",
        "max_portfolio_gross": 3.0,
        "max_symbol_gross": 1.0,
    },
    params_by_strategy=params_by_strategy,
    cost_bps=0.5,
    data_root=DATA_ROOT,
    timeframe="5m",
    start_ts=start_ts,
    end_ts=end_ts,
)

print("\n--- Backtest Metrics ---")
print(json.dumps(results["metrics"]["portfolio"], indent=2))

print("\n--- Strategy Contribution ---")
for strat, metrics in results["metrics"]["strategies"].items():
    print(
        f"{strat}: Total PnL = {metrics.get('total_pnl', 0.0):.4f}, Entries = {metrics.get('entries', 0)}"
    )

engine_dir = results["engine_dir"]
print(f"\nArtifacts saved to: {engine_dir}")
