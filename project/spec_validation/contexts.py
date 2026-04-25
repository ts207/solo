from __future__ import annotations

import yaml
from pathlib import Path
from typing import Any, List, Tuple

def validate_context_registry() -> List[Tuple[str, str]]:
    errors: List[Tuple[str, str]] = []
    
    contexts_yaml_path = Path("project/configs/registries/contexts.yaml")
    dim_registry_path = Path("spec/contexts/context_dimension_registry.yaml")
    
    if not contexts_yaml_path.exists():
        errors.append((str(contexts_yaml_path), "File missing"))
        return errors
        
    if not dim_registry_path.exists():
        errors.append((str(dim_registry_path), "File missing"))
        return errors
        
    try:
        with open(contexts_yaml_path, "r") as f:
            contexts_doc = yaml.safe_load(f)
    except Exception as e:
        errors.append((str(contexts_yaml_path), f"Failed to parse: {e}"))
        return errors
        
    try:
        with open(dim_registry_path, "r") as f:
            dim_doc = yaml.safe_load(f)
    except Exception as e:
        errors.append((str(dim_registry_path), f"Failed to parse: {e}"))
        return errors
        
    gen_dims = contexts_doc.get("context_dimensions", {})
    auth_dims = dim_doc.get("dimensions", {})
    
    for dim_name, dim_data in auth_dims.items():
        if dim_name not in gen_dims:
            errors.append((str(contexts_yaml_path), f"Missing authored dimension: {dim_name}"))
            continue
            
        auth_values = set(dim_data.get("values", {}).keys())
        gen_values = set(gen_dims[dim_name].get("allowed_values", []))
        
        missing_values = auth_values - gen_values
        if missing_values:
            errors.append((str(contexts_yaml_path), f"Dimension {dim_name} missing allowed values from registry: {', '.join(missing_values)}"))
            
    return errors
