from pathlib import Path

import yaml

from project.events.detectors.registry import list_registered_event_types

registry_path = Path("project/configs/registries/detectors.yaml")
with open(registry_path) as f:
    data = yaml.safe_load(f)

registered = set(list_registered_event_types())

new_ownership = {}
for k, v in data.get("detector_ownership", {}).items():
    if k.upper() in registered:
        new_ownership[k] = v

data["detector_ownership"] = new_ownership

with open(registry_path, "w") as f:
    yaml.dump(data, f, default_flow_style=False, sort_keys=False)
