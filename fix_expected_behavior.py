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

funding_yaml = """  disabled_regimes:
  - 'low_liquidity: funding/OI signals unreliable'
  - 'stale_funding: funding timestamp unavailable or stale'
  - 'execution_friction_blocked: spread/slippage too high'"""

quadrant_yaml = """  disabled_regimes:
  - 'stale_open_interest: OI unavailable or stale'
  - 'low_liquidity: price/OI quadrant unreliable'
  - 'execution_friction_blocked: spread/slippage too high'"""

for event in funding_events + quadrant_events:
    file_path = f"spec/events/{event}.yaml"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue
    with open(file_path, "r") as f:
        content = f.read()

    if "expected_behavior:" in content and "disabled_regimes:" not in content:
        insert_yaml = funding_yaml if event in funding_events else quadrant_yaml
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if line.startswith("expected_behavior:"):
                lines.insert(i+1, insert_yaml)
                break
        content = '\n'.join(lines)
        with open(file_path, "w") as f:
            f.write(content)
        print(f"Updated {file_path}")
    elif "disabled_regimes:" in content:
        print(f"Already has disabled_regimes: {file_path}")
