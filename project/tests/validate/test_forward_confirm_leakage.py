import pytest
from project.validate.forward_confirm import _load_frozen_thesis
from pathlib import Path

def test_load_frozen_thesis_no_selection_leakage():
    # Forbidden words that indicate selection/sorting leakage
    forbidden = ["sort_values", "idxmax", "nlargest", "rank_score"]
    with open("project/validate/forward_confirm.py", "r") as f:
        content = f.read()
        for word in forbidden:
            assert word not in content, f"Forbidden word '{word}' found in forward_confirm.py"
