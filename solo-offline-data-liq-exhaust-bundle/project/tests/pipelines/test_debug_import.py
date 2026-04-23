import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[2]))


def test_import():
    print("Starting import...")
    import project.research.phase2_search_engine as stage

    print("Import successful")
    assert stage is not None
