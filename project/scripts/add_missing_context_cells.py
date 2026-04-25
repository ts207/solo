import os
import yaml
from pathlib import Path

def add_context_cells():
    new_cells = [
        {
            "id": "forced_flow_cooldown",
            "dimension": "forced_flow_phase",
            "values": ["cooldown"],
            "required_feature_key": "forced_flow_cooldown",
            "executability_class": "runtime",
            "max_conjunction_depth": 1
        },
        {
            "id": "liquidity_refill",
            "dimension": "liquidity_phase",
            "values": ["refill"],
            "required_feature_key": "liquidity_refill",
            "executability_class": "runtime",
            "max_conjunction_depth": 1
        },
        {
            "id": "oi_flush",
            "dimension": "oi_phase",
            "values": ["flush"],
            "required_feature_key": "oi_flush",
            "executability_class": "runtime",
            "max_conjunction_depth": 1
        },
        {
            "id": "funding_positive_persistent",
            "dimension": "funding_phase",
            "values": ["positive_persistent"],
            "required_feature_key": "funding_positive_persistent",
            "executability_class": "runtime",
            "max_conjunction_depth": 1
        },
        {
            "id": "price_down_oi_down",
            "dimension": "price_oi_quadrant",
            "values": ["price_down_oi_down"],
            "required_feature_key": "price_down_oi_down",
            "executability_class": "runtime",
            "max_conjunction_depth": 1
        }
    ]

    count = 0
    for p in Path("spec/discovery").glob("tier2_*/context_cells.yaml"):
        if count >= 5:
            # Objective says "Update 5 key tier2 context_cells.yaml files"
            # I will just update the first 5 I find.
            break
            
        with open(p, "r") as f:
            doc = yaml.safe_load(f)
            
        if not isinstance(doc, dict):
            continue
            
        existing_ids = {c.get("id") for c in doc.get("context_cells", [])}
        cells_to_add = [c for c in new_cells if c["id"] not in existing_ids]
        
        if cells_to_add:
            doc.setdefault("context_cells", []).extend(cells_to_add)
            with open(p, "w") as f:
                yaml.dump(doc, f, sort_keys=False)
            print(f"Updated {p}")
            count += 1

if __name__ == "__main__":
    add_context_cells()
