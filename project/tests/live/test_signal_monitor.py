from __future__ import annotations

from datetime import datetime, timedelta, timezone

from project.live.signal_monitor import SignalMonitor


def _ts(offset_hours: float = 0.0) -> datetime:
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(hours=offset_hours)


class TestSignalSilence:
    def test_ok_when_recently_fired(self) -> None:
        monitor = SignalMonitor(silence_warn_sec=3600.0, silence_alert_sec=7200.0)
        monitor.record_event_fired("thesis_1", "LIQUIDATION_CASCADE", timestamp=_ts(0))
        report = monitor.check(now=_ts(0.5))
        assert len(report.silence_statuses) == 1
        assert report.silence_statuses[0].level == "ok"
        assert report.any_warn is False

    def test_warn_after_silence_threshold(self) -> None:
        monitor = SignalMonitor(silence_warn_sec=3600.0, silence_alert_sec=7200.0)
        monitor.record_event_fired("thesis_1", "LIQUIDATION_CASCADE", timestamp=_ts(0))
        report = monitor.check(now=_ts(1.5))  # 1.5 hours: past warn, before alert
        assert report.silence_statuses[0].level == "warn"
        assert report.any_warn is True
        assert report.any_alert is False

    def test_alert_after_alert_threshold(self) -> None:
        monitor = SignalMonitor(silence_warn_sec=3600.0, silence_alert_sec=7200.0)
        monitor.record_event_fired("thesis_1", "LIQUIDATION_CASCADE", timestamp=_ts(0))
        report = monitor.check(now=_ts(9))  # 9 hours later
        assert report.silence_statuses[0].level == "alert"
        assert report.any_alert is True

    def test_silence_resets_after_new_fire(self) -> None:
        monitor = SignalMonitor(silence_warn_sec=3600.0, silence_alert_sec=7200.0)
        monitor.record_event_fired("thesis_1", "LIQUIDATION_CASCADE", timestamp=_ts(0))
        monitor.record_event_fired("thesis_1", "LIQUIDATION_CASCADE", timestamp=_ts(5))
        report = monitor.check(now=_ts(5.1))
        assert report.silence_statuses[0].level == "ok"

    def test_empty_monitor_produces_no_statuses(self) -> None:
        monitor = SignalMonitor()
        report = monitor.check()
        assert report.silence_statuses == []
        assert report.fill_calibration_statuses == []
        assert report.any_warn is False


class TestFillCalibration:
    def test_insufficient_data_before_min_samples(self) -> None:
        monitor = SignalMonitor(min_fill_samples=5, fill_window=20)
        for _ in range(3):
            monitor.record_fill_outcome("thesis_1", predicted_fill_probability=0.9, was_filled=True)
        report = monitor.check()
        assert report.fill_calibration_statuses[0].level == "insufficient_data"
        assert report.any_warn is False

    def test_ok_when_fills_match_prediction(self) -> None:
        monitor = SignalMonitor(
            min_fill_samples=5, fill_warn_ratio=0.70, fill_alert_ratio=0.50
        )
        for _ in range(10):
            monitor.record_fill_outcome("thesis_1", predicted_fill_probability=0.85, was_filled=True)
        report = monitor.check()
        fc = report.fill_calibration_statuses[0]
        assert fc.level == "ok"
        assert abs(fc.actual_rate - 1.0) < 1e-9
        assert fc.calibration_ratio > 1.0

    def test_warn_when_fill_rate_below_warn_ratio(self) -> None:
        monitor = SignalMonitor(
            min_fill_samples=5, fill_window=20, fill_warn_ratio=0.70, fill_alert_ratio=0.50
        )
        # predicted=0.9, actual=5/10=0.5, ratio=0.5/0.9≈0.56 → warn (below 0.70)
        for i in range(10):
            monitor.record_fill_outcome(
                "thesis_1", predicted_fill_probability=0.9, was_filled=(i < 5)
            )
        report = monitor.check()
        fc = report.fill_calibration_statuses[0]
        assert fc.level == "warn"
        assert report.any_warn is True

    def test_alert_when_fill_rate_critically_low(self) -> None:
        monitor = SignalMonitor(
            min_fill_samples=5, fill_window=20, fill_warn_ratio=0.70, fill_alert_ratio=0.50
        )
        # predicted=0.9, actual=2/10=0.2, ratio≈0.22 → alert
        for i in range(10):
            monitor.record_fill_outcome(
                "thesis_1", predicted_fill_probability=0.9, was_filled=(i < 2)
            )
        report = monitor.check()
        fc = report.fill_calibration_statuses[0]
        assert fc.level == "alert"
        assert report.any_alert is True

    def test_rolling_window_evicts_old_records(self) -> None:
        monitor = SignalMonitor(min_fill_samples=3, fill_window=5)
        # First 5: all miss
        for _ in range(5):
            monitor.record_fill_outcome("thesis_1", predicted_fill_probability=0.9, was_filled=False)
        # Next 5: all fill → window should now contain only the last 5 (all True)
        for _ in range(5):
            monitor.record_fill_outcome("thesis_1", predicted_fill_probability=0.9, was_filled=True)
        report = monitor.check()
        fc = report.fill_calibration_statuses[0]
        assert fc.sample_count == 5
        assert abs(fc.actual_rate - 1.0) < 1e-9

    def test_multiple_theses_tracked_independently(self) -> None:
        monitor = SignalMonitor(min_fill_samples=3, fill_window=20)
        for _ in range(5):
            monitor.record_fill_outcome("thesis_a", predicted_fill_probability=0.9, was_filled=True)
        for i in range(5):
            monitor.record_fill_outcome(
                "thesis_b", predicted_fill_probability=0.9, was_filled=(i < 1)
            )
        report = monitor.check()
        by_tid = {fc.thesis_id: fc for fc in report.fill_calibration_statuses}
        assert by_tid["thesis_a"].level == "ok"
        assert by_tid["thesis_b"].level in {"warn", "alert"}

    def test_as_dict_is_serializable(self) -> None:
        monitor = SignalMonitor()
        monitor.record_event_fired("t1", "VOL_SPIKE", timestamp=_ts(0))
        report = monitor.check(now=_ts(1))
        d = report.as_dict()
        import json
        json.dumps(d)  # must not raise
        assert "silence" in d
        assert "fill_calibration" in d
        assert "any_alert" in d
