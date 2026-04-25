import os
import yaml
from pathlib import Path
from project.research.cell_discovery.registry import load_registry
from project.domain.compiled_registry import get_domain_registry

def fix_event_atoms(root: Path = Path("spec/discovery")):
    registry = get_domain_registry()
    for yaml_path in root.rglob("event_atoms.yaml"):
        with open(yaml_path, "r") as f:
            content = f.read()
        
        # We will parse the yaml, fix it, but also want to keep the order and comments if possible.
        # However, for simplicity, we can just do line replacements or re-dump.
        # But yaml.dump removes comments. Let's do string replacement.
        doc = yaml.safe_load(content)
        if not isinstance(doc, dict):
            continue
            
        modified = False
        new_lines = content.split('\n')
        
        for atom in doc.get("event_atoms", []):
            atom_id = atom.get("id", "")
            event_type = atom.get("event_type", "").upper()
            thesis_eligible = atom.get("thesis_eligible", True)
            search_role = atom.get("search_role", "")
            
            if not registry.has_event(event_type):
                continue
                
            event_row = registry.event_row(event_type) or {}
            promotion_eligible = event_row.get("promotion_eligible", False)
            detector_band = event_row.get("detector_band", "")
            
            # Rule 1: if promotion_eligible is False, thesis_eligible must be False
            if thesis_eligible and not promotion_eligible:
                # Need to set thesis_eligible to false
                thesis_eligible = False
                
            # Rule 2: if detector_band is context_only, cannot be primary
            if detector_band == "context_only" and thesis_eligible:
                thesis_eligible = False
                
            # Rule 3: composite_or_fragile
            if detector_band == "composite_or_fragile" and thesis_eligible and atom.get("promotion_role", "") != "requires_composite":
                thesis_eligible = False
            
            if not thesis_eligible and atom.get("thesis_eligible", True):
                # Update in text
                # Find the atom in text.
                in_atom = False
                for i, line in enumerate(new_lines):
                    if line.strip() == f"- id: {atom_id}":
                        in_atom = True
                    elif in_atom and line.strip().startswith("- id:"):
                        in_atom = False
                    
                    if in_atom and line.strip().startswith("thesis_eligible:"):
                        new_lines[i] = line.replace("true", "false").replace("True", "False")
                        modified = True
                        break
        
        if modified:
            with open(yaml_path, "w") as f:
                f.write('\n'.join(new_lines))
            print(f"Fixed {yaml_path}")

if __name__ == "__main__":
    fix_event_atoms()
