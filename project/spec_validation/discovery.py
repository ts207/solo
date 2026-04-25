import sys
from pathlib import Path

from project.research.cell_discovery.registry import load_registry

def validate_all_discovery_specs(root: Path = Path("spec/discovery")):
    errors = []
    
    # Check the root directory itself if it has the required files
    dirs_to_check = [root] + [p for p in root.iterdir() if p.is_dir()]
    
    for d in dirs_to_check:
        # Check if it has event_atoms.yaml
        if not (d / "event_atoms.yaml").exists():
            continue
            
        print(f"Validating discovery spec in {d}")
        try:
            load_registry(d)
        except Exception as e:
            errors.append((str(d), str(e)))
            
    return errors

if __name__ == "__main__":
    errors = validate_all_discovery_specs()
    if errors:
        print(f"FAILURE: {len(errors)} discovery spec directories failed validation:")
        for loc, msg in errors:
            print(f"  [{loc}] {msg}")
        sys.exit(1)
    print("SUCCESS: All discovery specs passed validation.")
    sys.exit(0)
