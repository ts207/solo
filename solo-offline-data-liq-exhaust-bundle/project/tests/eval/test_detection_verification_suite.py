from __future__ import annotations

from project.eval.detection_verification_suite import run_detection_verification


def test_detection_verification_suite_passes_live_detector_contracts() -> None:
    report = run_detection_verification()

    assert not report.empty
    assert report["pass"].all()
