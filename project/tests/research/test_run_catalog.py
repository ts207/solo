import json
import pytest
from pathlib import Path
from project.research.validation.manifest import RunArtifactManifest
from project.research.services import run_catalog_service

@pytest.fixture
def mock_manifest_data(tmp_path):
    data_root = tmp_path / "data"
    data_root.mkdir()
    
    # Discovery run
    discover_dir = data_root / "reports" / "phase2" / "run1"
    m1 = RunArtifactManifest(
        run_id="run1", stage="discover", created_at="2026-01-01T00:00:00Z",
        artifacts={"phase2_candidates": "phase2_candidates.parquet"}
    )
    m1.persist(discover_dir)
    
    # Validation run
    val_dir = data_root / "reports" / "validation" / "run1"
    m2 = RunArtifactManifest(
        run_id="run1", stage="validate", created_at="2026-01-01T00:01:00Z",
        upstream_run_ids=["run1"],
        artifacts={"validated_candidates": "validated_candidates.parquet"}
    )
    m2.persist(val_dir)
    
    return data_root

def test_list_runs(mock_manifest_data):
    runs = run_catalog_service.list_runs(data_root=mock_manifest_data)
    assert len(runs) == 2
    ids = [r["run_id"] for r in runs]
    assert "run1" in ids

def test_compare_manifests(mock_manifest_data):
    # Create another discover run to compare
    discover_dir2 = mock_manifest_data / "reports" / "phase2" / "run2"
    m3 = RunArtifactManifest(
        run_id="run2", stage="discover", created_at="2026-01-01T00:05:00Z",
        artifacts={"phase2_candidates": "phase2_candidates.parquet", "extra": "extra.json"}
    )
    m3.persist(discover_dir2)
    
    diff = run_catalog_service.compare_manifests("run1", "run2", stage="discover", data_root=mock_manifest_data)
    assert diff["run_id_a"] == "run1"
    assert diff["run_id_b"] == "run2"
    assert "extra" in diff["artifact_diff"]["only_in_b"]
