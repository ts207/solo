import os

funding_events = [
    "FUNDING_FLIP_TO_NEGATIVE",
    "FUNDING_FLIP_TO_POSITIVE",
    "FUNDING_NEG_EXTREME_ONSET",
    "FUNDING_NEG_NORMALIZATION",
    "FUNDING_NEG_PERSISTENCE",
    "FUNDING_POS_EXTREME_ONSET",
    "FUNDING_POS_NORMALIZATION",
    "FUNDING_POS_PERSISTENCE"
]

quadrant_events = [
    "PRICE_DOWN_OI_DOWN",
    "PRICE_DOWN_OI_UP",
    "PRICE_UP_OI_DOWN",
    "PRICE_UP_OI_UP"
]

funding_yaml = """expected_behavior:
  disabled_regimes:
  - 'low_liquidity: funding/OI signals unreliable'
  - 'stale_funding: funding timestamp unavailable or stale'
  - 'execution_friction_blocked: spread/slippage too high'
"""

quadrant_yaml = """expected_behavior:
  disabled_regimes:
  - 'stale_open_interest: OI unavailable or stale'
  - 'low_liquidity: price/OI quadrant unreliable'
  - 'execution_friction_blocked: spread/slippage too high'
"""

for event in funding_events + quadrant_events:
    file_path = f"spec/events/{event}.yaml"
    if not os.path.exists(file_path):
        continue
    with open(file_path, "r") as f:
        content = f.read()

    if "expected_behavior:" not in content:
        insert_yaml = funding_yaml if event in funding_events else quadrant_yaml
        with open(file_path, "a") as f:
            f.write("\n" + insert_yaml)
        print(f"Appended to {file_path}")
