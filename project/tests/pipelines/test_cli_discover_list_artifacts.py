from __future__ import annotations

import sys

from project.tests.pipelines.test_cli_contract import _load_cli_module


def test_discover_list_artifacts_lists_only_canonical_phase2_outputs(
    monkeypatch, capsys, tmp_path
) -> None:
    cli = _load_cli_module()
    data_root = tmp_path / "data"
    run_id = "unit"

    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    (phase2_dir / "phase2_candidates.parquet").write_bytes(b"PAR1")

    legacy_dir = data_root / "reports" / "edge_candidates" / run_id
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "edge_candidates_normalized.parquet").write_bytes(b"PAR1")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "list-artifacts",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
    )

    assert cli.main() == 0
    out = capsys.readouterr().out

    assert f"Artifacts for discovery run {run_id}:" in out
    assert f"reports/phase2/{run_id}/phase2_candidates.parquet" in out
    assert "edge_candidates_normalized.parquet" not in out


def test_discover_list_artifacts_does_not_treat_legacy_edge_candidates_as_canonical_artifacts(
    monkeypatch, capsys, tmp_path
) -> None:
    cli = _load_cli_module()
    data_root = tmp_path / "data"
    run_id = "legacy-only"

    legacy_dir = data_root / "reports" / "edge_candidates" / run_id
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "edge_candidates_normalized.parquet").write_bytes(b"PAR1")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "list-artifacts",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
    )

    assert cli.main() == 0
    out = capsys.readouterr().out

    assert f"No discovery artifacts found for run {run_id}" in out
    assert "edge_candidates_normalized.parquet" not in out
