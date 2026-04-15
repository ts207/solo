import json
from pathlib import Path
from project.research.services.pathing import resolve_phase2_candidates_path, resolve_phase2_diagnostics_path
from project.scripts.run_golden_workflow import load_workflow_config
from project.scripts.validate_synthetic_detector_truth import validate_detector_truth
from project.scripts.run_golden_synthetic_discovery import _candidate_summary

config_path = Path("project/configs/golden_synthetic_discovery_6m.yaml")
root = Path("artifacts/golden_synthetic_discovery_6m_v3")
run_id = "synthetic_run_6m"

config = load_workflow_config(config_path)
synthetic_manifest_path = root / "synthetic" / run_id / "synthetic_generation_manifest.json"
synthetic_manifest = json.loads(synthetic_manifest_path.read_text(encoding="utf-8"))

truth_map_path = Path(synthetic_manifest["truth_map_path"])
truth_validation = validate_detector_truth(
    data_root=root,
    run_id=run_id,
    truth_map_path=truth_map_path,
)
search_diag_path = resolve_phase2_diagnostics_path(data_root=root, run_id=run_id)
search_diag = (
    json.loads(search_diag_path.read_text(encoding="utf-8")) if search_diag_path.exists() else {}
)
candidate_summary = _candidate_summary(resolve_phase2_candidates_path(data_root=root, run_id=run_id))

payload = {
    "workflow_id": str(config.get("workflow_id", "golden_synthetic_discovery_v1")),
    "config_path": str(config_path),
    "root": str(root),
    "run_id": run_id,
    "synthetic_manifest": synthetic_manifest,
    "pipeline": {
        "argv": [],  # dummy
        "returncode": 0,
    },
    "truth_validation": truth_validation,
    "search_engine_diagnostics": search_diag,
    "candidate_summary": candidate_summary,
    "required_outputs": list(config.get("required_outputs", [])),
}
out_path = root / "reliability" / "golden_synthetic_discovery_summary.json"
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"Summary written to {out_path}")
