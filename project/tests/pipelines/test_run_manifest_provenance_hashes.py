from __future__ import annotations

import hashlib
import json

import pytest

import project.specs.manifest as manifest_spec


def test_start_manifest_includes_git_and_spec_hashes():
    manifest = manifest_spec.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[],
        outputs=[],
    )
    assert "git_commit" in manifest
    assert "spec_hashes" in manifest
    assert isinstance(manifest["spec_hashes"], dict)
    assert str(manifest.get("ontology_spec_hash", "")).startswith("sha256:")
    assert "taxonomy_hash" in manifest
    assert "canonical_event_registry_hash" in manifest
    assert "state_registry_hash" in manifest
    assert "verb_lexicon_hash" in manifest
    assert "python_version" in manifest
    assert "platform" in manifest
    assert "env_snapshot" in manifest


def test_finalize_manifest_hashes_input_parquets(monkeypatch, tmp_path):
    parquet_path = tmp_path / "input.parquet"
    payload = b"fake parquet payload"
    parquet_path.write_bytes(payload)
    expected_hash = hashlib.sha256(payload).hexdigest()

    out_manifest = tmp_path / "unit_stage.json"
    monkeypatch.setattr(
        manifest_spec, "_manifest_path", lambda run_id, stage, stage_instance_id=None: out_manifest
    )

    manifest = manifest_spec.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[{"path": str(parquet_path)}],
        outputs=[],
    )
    finalized = manifest_spec.finalize_manifest(manifest, status="success", stats={})

    files = finalized["input_parquet_hashes"]["files"]
    assert str(parquet_path) in files
    assert files[str(parquet_path)] == expected_hash

    disk_payload = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert disk_payload["input_parquet_hashes"]["files"][str(parquet_path)] == expected_hash


def test_finalize_manifest_uses_manifest_stage_instance_id(monkeypatch, tmp_path):
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    def _manifest_path(run_id, stage, stage_instance_id=None):
        return manifest_dir / f"{stage_instance_id or stage}.json"

    monkeypatch.setattr(manifest_spec, "_manifest_path", _manifest_path)

    manifest = manifest_spec.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[],
        outputs=[],
        stage_instance_id="unit_stage__worker_a",
    )
    monkeypatch.setenv("BACKTEST_STAGE_INSTANCE_ID", "unit_stage__worker_b")

    manifest_spec.finalize_manifest(manifest, status="success", stats={})

    assert (manifest_dir / "unit_stage__worker_a.json").exists()
    assert not (manifest_dir / "unit_stage__worker_b.json").exists()


def test_finalize_manifest_reconciles_run_manifest_for_standalone_stage_reruns(
    monkeypatch, tmp_path
):
    out_manifest = tmp_path / "unit_stage.json"
    calls: list[str] = []

    monkeypatch.setattr(
        manifest_spec, "_manifest_path", lambda run_id, stage, stage_instance_id=None: out_manifest
    )
    monkeypatch.delenv("BACKTEST_PIPELINE_SESSION_ID", raising=False)

    def _fake_reconcile(run_id: str):
        calls.append(run_id)

    monkeypatch.setattr(
        "project.pipelines.pipeline_provenance.reconcile_run_manifest_from_stage_manifests",
        _fake_reconcile,
    )

    manifest = manifest_spec.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[],
        outputs=[],
    )

    manifest_spec.finalize_manifest(manifest, status="success", stats={})

    assert calls == ["r_manifest"]


def test_finalize_manifest_raises_when_reconciliation_fails_for_standalone_stage_reruns(
    monkeypatch, tmp_path
):
    out_manifest = tmp_path / "unit_stage.json"

    monkeypatch.setattr(
        manifest_spec, "_manifest_path", lambda run_id, stage, stage_instance_id=None: out_manifest
    )
    monkeypatch.delenv("BACKTEST_PIPELINE_SESSION_ID", raising=False)

    def _fail_reconcile(run_id: str):
        raise RuntimeError(f"boom:{run_id}")

    monkeypatch.setattr(
        "project.pipelines.pipeline_provenance.reconcile_run_manifest_from_stage_manifests",
        _fail_reconcile,
    )

    manifest = manifest_spec.start_manifest(
        stage_name="unit_stage",
        run_id="r_manifest",
        params={},
        inputs=[],
        outputs=[],
    )

    with pytest.raises(RuntimeError, match="Failed to reconcile run manifest"):
        manifest_spec.finalize_manifest(manifest, status="success", stats={})

    assert out_manifest.exists()
