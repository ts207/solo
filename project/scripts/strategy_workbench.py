#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import yaml

# Static maps for ease of use (from project.strategy.dsl.contract_v1)
REGIME_CHOICES = [
    "session_asia",
    "session_eu",
    "session_us",
    "vol_regime_low",
    "vol_regime_mid",
    "vol_regime_high",
    "bull_bear_bull",
    "bull_bear_bear",
    "ms_trend_state_0.0",
    "ms_trend_state_1.0",
    "ms_trend_state_2.0",
    "ms_vol_state_0.0",
    "ms_vol_state_1.0",
    "ms_vol_state_2.0",
    "ms_vol_state_3.0",
]


def main():
    parser = argparse.ArgumentParser(
        description="Strategy Workbench: Freely mix and match components."
    )
    parser.add_argument("--event", help="Event type to detect (e.g. VOL_SPIKE)")
    parser.add_argument("--regime", help="Required market regime (e.g. VOL_REGIME_LOW)")
    parser.add_argument("--template", default="mean_reversion", help="Base strategy template")
    parser.add_argument("--horizon", type=int, default=24, help="Prediction horizon in bars")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol to test")
    parser.add_argument("--list", action="store_true", help="List all available options")

    args = parser.parse_args()

    if args.list:
        print_menu()
        return

    if not args.event:
        print("Error: --event is required. Use --list to see options.")
        sys.exit(1)

    # Generate the Concept YAML
    concept = {
        "concept_id": f"workbench_{args.event.lower()}_{args.regime or 'all'}",
        "description": f"Generated via Workbench: {args.event} under {args.regime or 'unconditional'} context.",
        "event_definition": {
            "event_type": args.event.upper(),
            "canonical_family": "VOLATILITY_TRANSITION",  # Default family
        },
        "market_state": {"required_regimes": [args.regime] if args.regime else []},
        "templates": {"base": args.template, "overlays": ["liquidity_guard"]},
        "parameters": {
            "horizons_bars": [args.horizon],
            "risk": {"stop_loss_atr_multipliers": [2.0], "take_profit_atr_multipliers": [3.0]},
        },
    }

    temp_path = Path("spec/concepts/workbench_temp.yaml")
    with open(temp_path, "w") as f:
        yaml.dump(concept, f)

    print("\n--- Strategy Workbench ---")
    print(f"Targeting Event: {args.event}")
    print(f"Market Regime:   {args.regime or 'UNCONDITIONAL'}")
    print(f"Rule Template:   {args.template}")
    print(f"Symbol:          {args.symbol}")
    print(f"\nSaved concept to {temp_path}")
    print("\nProceeding to run discovery...")

    # Run the pipeline
    import subprocess

    cmd = [
        sys.executable,
        "-m",
        "project.pipelines.run_all",
        "--run_id",
        f"workbench_{args.event.lower()}",
        "--symbols",
        args.symbol,
        "--concept",
        "workbench_temp",
        "--run_phase2_conditional",
        "1",
        "--run_edge_candidate_universe",
        "1",
    ]
    subprocess.run(cmd)


def print_menu():
    print("\n--- Available Events (Validated) ---")
    print(
        "VOL_SPIKE, SPREAD_BLOWOUT, DEPTH_COLLAPSE, BASIS_DISLOC, FUNDING_EXTREME_ONSET, OI_FLUSH"
    )

    print("\n--- Available Regimes ---")
    for r in REGIME_CHOICES:
        print(f"  - {r}")

    print("\n--- Common Rule Templates ---")
    print("  - mean_reversion")
    print("  - continuation (Standard Trend Following)")
    print("  - carry")
    print("  - breakout")


if __name__ == "__main__":
    main()
