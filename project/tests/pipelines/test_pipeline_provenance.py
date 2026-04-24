from __future__ import annotations

import json
from pathlib import Path

import pytest

from project.core.exceptions import DataIntegrityError
from project.pipelines import pipeline_provenance as prov
from project.pipelines.execution_plan import (
    ArtifactVerificationResult,
    ExecutionPlan,
    ExecutionVerificationReport,
    PlannedArtifactObligation,
    PlannedStage,
)


def test_config_digest_is_order_independent(tmp_path: Path) -> None:
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text("alpha: 1\n", encoding="utf-8")
    b.write_text("beta: 2\n", encoding="utf-8")

    digest1 = prov.config_digest([str(a), str(b)])
    digest2 = prov.config_digest([str(b), str(a)])
    assert digest1 == digest2

    b.write_text("beta: 3\n", encoding="utf-8")
    digest3 = prov.config_digest([str(a), str(b)])
    assert digest3 != digest1


def test_data_fingerprint_is_deterministic_and_sensitive_to_file_changes(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    data_root = tmp_path / "data"
    (data_root / "lake" / "raw" / "binance" / "perp" / "BTCUSDT").mkdir(parents=True)
    (data_root / "lake" / "raw" / "binance" / "spot" / "BTCUSDT").mkdir(parents=True)
    perps = data_root / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "sample.csv"
    spots = data_root / "lake" / "raw" / "binance" / "spot" / "BTCUSDT" / "sample.csv"
    perps.write_text("x\n1\n", encoding="utf-8")
    spots.write_text("x\n2\n", encoding="utf-8")

    monkeypatch.setattr(prov, "feature_schema_metadata", lambda: ("v-test", "hash-test"))
    digest1, payload1 = prov.data_fingerprint(["btcusdt"], "run-1", project_root=project_root, data_root=data_root)
    digest2, payload2 = prov.data_fingerprint(["BTCUSDT"], "run-1", project_root=project_root, data_root=data_root)
    assert digest1 == digest2
    assert payload1["lake"]["file_count"] == 2
    assert payload1["feature_schema"]["version"] == "v-test"
    assert payload1["manifest_hash"] == payload2["manifest_hash"]
    assert payload1["lake"] == payload2["lake"]
    assert payload1["feature_schema"] == payload2["feature_schema"]

    perps.write_text("x\n9\n", encoding="utf-8")
    digest3, _ = prov.data_fingerprint(["BTCUSDT"], "run-1", project_root=project_root, data_root=data_root)
    assert digest3 != digest1


def test_manifest_roundtrip_and_resume_resolution(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(prov, "_get_data_root", lambda: tmp_path)
    manifest = {"run_id": "run-1", "value": 7}
    prov.write_run_manifest("run-1", manifest)
    assert prov.read_run_manifest("run-1") == manifest

    manifest_path = tmp_path / "runs" / "run-1" / "run_manifest.json"
    manifest_path.write_text(
        '{"ontology_spec_hash":"abc","effective_config_hash":"cfg","failed_stage_instance":"stage2"}',
        encoding="utf-8",
    )
    existing, ontology_hash, resume_from_index = prov.resolve_existing_manifest_state(
        existing_manifest_path=manifest_path,
        ontology_hash="abc",
        effective_config_hash="cfg",
        allow_ontology_hash_mismatch=False,
        planned_stage_instances=["stage1", "stage2", "stage3"],
        resume_from_failed_stage=True,
    )
    assert ontology_hash == "abc"
    assert existing["failed_stage_instance"] == "stage2"
    assert resume_from_index == 1


def test_read_run_manifest_raises_on_malformed_json(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(prov, "_get_data_root", lambda: tmp_path)
    manifest_path = tmp_path / "runs" / "run-1" / "run_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read run manifest"):
        prov.read_run_manifest("run-1")


def test_resume_resolution_rejects_manifest_with_external_effective_config(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest_dir = data_root / "runs" / "run-1"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "run_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "ontology_spec_hash": "abc",
                "effective_config_hash": "cfg",
                "failed_stage_instance": "stage2",
                "effective_config_path": "/tmp/external/effective_config.json",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="effective_config_path points outside active root"):
        prov.resolve_existing_manifest_state(
            existing_manifest_path=manifest_path,
            ontology_hash="abc",
            effective_config_hash="cfg",
            allow_ontology_hash_mismatch=False,
            planned_stage_instances=["stage1", "stage2", "stage3"],
            resume_from_failed_stage=True,
        )


def test_resume_resolution_rejects_manifest_with_missing_repo_spec_path(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest_dir = data_root / "runs" / "run-1"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "run_manifest.json"
    effective_config_path = manifest_dir / "effective_config.json"
    effective_config_path.write_text('{"ok": true}\n', encoding="utf-8")
    missing_objective = prov.PROJECT_ROOT.parent / "spec" / "objectives" / "missing_resume_test.yaml"

    manifest_path.write_text(
        json.dumps(
            {
                "ontology_spec_hash": "abc",
                "effective_config_hash": "cfg",
                "failed_stage_instance": "stage2",
                "effective_config_path": str(effective_config_path),
                "objective_spec_path": str(missing_objective),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="objective_spec_path missing"):
        prov.resolve_existing_manifest_state(
            existing_manifest_path=manifest_path,
            ontology_hash="abc",
            effective_config_hash="cfg",
            allow_ontology_hash_mismatch=False,
            planned_stage_instances=["stage1", "stage2", "stage3"],
            resume_from_failed_stage=True,
        )



def test_lineage_and_metadata_helpers(tmp_path: Path, monkeypatch) -> None:
    manifest = {"run_id": "run-1", "emit_run_hash": True, "payload": 3}
    prov.maybe_emit_run_hash(manifest)

    manifest2 = {"run_id": "run-2"}
    prov.refresh_runtime_lineage_fields(
        manifest2,
        determinism_replay_checks_requested=True,
        oms_replay_checks_requested=True,
    )
    assert manifest2["determinism_status"] == "requested"
    assert manifest2["oms_replay_status"] == "requested"
    assert "runtime_lineage_refreshed_at" in manifest2

    objective_path = tmp_path / "objective.yaml"
    objective_path.write_text("objective:\n  name: sample\n  min_net_expectancy_bps: 2.5\n", encoding="utf-8")
    objective, objective_hash, resolved_path = prov.objective_spec_metadata("ignored", str(objective_path))
    assert objective["name"] == "sample"
    assert objective_hash != "unknown_hash"
    assert resolved_path == str(objective_path)

    retail_path = tmp_path / "retail_profiles.yaml"
    retail_path.write_text(
        "profiles:\n  sample:\n    max_position_usd: 1000\n",
        encoding="utf-8",
    )
    profile, profile_hash, resolved_path = prov.retail_profile_metadata("sample", str(retail_path))
    assert profile["id"] == "sample"
    assert profile["max_position_usd"] == 1000
    assert profile_hash != "unknown_hash"
    assert resolved_path == str(retail_path)


def test_objective_and_profile_metadata_raise_on_malformed_specs(tmp_path: Path) -> None:
    objective_path = tmp_path / "objective.yaml"
    objective_path.write_text("objective:\n  [", encoding="utf-8")
    with pytest.raises(DataIntegrityError):
        prov.objective_spec_metadata("ignored", str(objective_path))

    retail_path = tmp_path / "retail_profiles.yaml"
    retail_path.write_text("profiles:\n  sample: [", encoding="utf-8")
    with pytest.raises(DataIntegrityError):
        prov.retail_profile_metadata("sample", str(retail_path))


def test_write_execution_reports_materializes_explain_plan_and_conformance(tmp_path: Path) -> None:
    plan = ExecutionPlan(
        run_id="run-1",
        planned_at="2026-04-18T00:00:00Z",
        stages=(
            PlannedStage(
                stage_name="phase2_search_engine",
                script_path="research/phase2_search_engine.py",
                reason_code="selected",
                stage_family="phase2_discovery",
                owner_service="project.research.services.candidate_discovery_service",
            ),
        ),
        artifact_obligations=(
            PlannedArtifactObligation(
                contract_id="discovery_phase2_candidates",
                producer_stage_family="phase2_discovery",
                schema_id="phase2_candidates",
                schema_version="phase2_candidates_v1",
                strictness="strict",
                required=True,
                expected_path="reports/phase2/run-1/phase2_candidates.parquet",
            ),
        ),
    )
    report = ExecutionVerificationReport(
        run_id="run-1",
        verified_at="2026-04-18T01:00:00Z",
        plan_stage_count=1,
        actual_stage_count=1,
        results=(),
        artifact_results=(
            ArtifactVerificationResult(
                contract_id="discovery_phase2_candidates",
                expected_path="reports/phase2/run-1/phase2_candidates.parquet",
                producer_stage_family="phase2_discovery",
                schema_id="phase2_candidates",
                schema_version="phase2_candidates_v1",
                strictness="strict",
                required=True,
                status="conformant",
                actual_path=str(tmp_path / "reports" / "phase2" / "run-1" / "phase2_candidates.parquet"),
            ),
        ),
        final_status="success",
    )

    paths = prov.write_execution_reports(
        run_id="run-1",
        plan=plan,
        verification_report=report,
        data_root=tmp_path,
    )

    assert Path(paths["explain_plan_json"]).exists()
    assert Path(paths["contract_conformance_json"]).exists()
    assert "Explain Plan" in Path(paths["explain_plan_markdown"]).read_text(encoding="utf-8")
    assert "Contract Conformance" in Path(paths["contract_conformance_markdown"]).read_text(
        encoding="utf-8"
    )
