from pathlib import Path

from project.scripts.debug import replay_r6_late


def test_bridge_stage_files_discovers_any_timeframe(tmp_path: Path):
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "bridge_evaluate_phase2__VOL_SHOCK_15m.json").write_text("{}", encoding="utf-8")
    (run_dir / "bridge_evaluate_phase2__VOL_SHOCK_5m.json").write_text("{}", encoding="utf-8")

    files = replay_r6_late._bridge_stage_files(["VOL_SHOCK"], run_dir)

    assert [p.name for p in files] == [
        "bridge_evaluate_phase2__VOL_SHOCK_15m.json",
        "bridge_evaluate_phase2__VOL_SHOCK_5m.json",
    ]


def test_bridge_stage_files_falls_back_to_unsuffixed_manifest(tmp_path: Path):
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "bridge_evaluate_phase2.json").write_text("{}", encoding="utf-8")

    files = replay_r6_late._bridge_stage_files(["VOL_SHOCK"], run_dir)

    assert [p.name for p in files] == ["bridge_evaluate_phase2.json"]
