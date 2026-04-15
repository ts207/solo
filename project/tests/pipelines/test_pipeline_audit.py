from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from project.pipelines import pipeline_audit


def test_apply_run_terminal_audit_records_top_level_artifact_count_and_uses_finished_at(
    monkeypatch, tmp_path: Path
) -> None:
    data_root = tmp_path / "data"
    run_id = "audit_run"
    run_dir = data_root / "runs" / run_id
    report_dir = data_root / "reports" / "phase2" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    early_file = run_dir / "early.txt"
    early_file.write_text("ok\n", encoding="utf-8")
    report_file = report_dir / "report.json"
    report_file.write_text("{}\n", encoding="utf-8")

    finished_at = datetime.now(timezone.utc)
    before_finish = finished_at - timedelta(seconds=5)
    after_finish = finished_at + timedelta(seconds=5)
    early_ts = before_finish.timestamp()
    late_ts = after_finish.timestamp()

    early_file.touch()
    report_file.touch()
    report_path = str(report_file)
    early_path = str(early_file)
    import os

    os.utime(early_path, (early_ts, early_ts))
    os.utime(report_path, (early_ts, early_ts))

    late_file = report_dir / "late.txt"
    late_file.write_text("late\n", encoding="utf-8")
    late_path = str(late_file)
    os.utime(late_path, (late_ts, late_ts))

    manifest = {
        "run_id": run_id,
        "started_at": (finished_at - timedelta(minutes=1)).isoformat(),
        "finished_at": finished_at.isoformat(),
    }

    monkeypatch.setattr(pipeline_audit, "DATA_ROOT", data_root)
    pipeline_audit.apply_run_terminal_audit(run_id, manifest)

    assert manifest["artifact_count"] == 3
    assert manifest["artifact_catalog"]["artifact_count"] == 3
    assert manifest["late_artifacts"] == [f"reports/phase2/{run_id}/late.txt"]

    artifact_manifest = data_root / "runs" / run_id / "artifact_manifest.json"
    payload = json.loads(artifact_manifest.read_text(encoding="utf-8"))
    assert payload["artifact_count"] == 3
