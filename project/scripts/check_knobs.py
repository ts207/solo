import json

from project.research.knowledge.knobs import build_agent_knob_rows

rows = build_agent_knob_rows()
found = [r for r in rows if r["name"] == "run_phase2_conditional"]
print(json.dumps(found, indent=2))
