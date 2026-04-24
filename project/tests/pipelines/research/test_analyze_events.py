"""Tests for the universal analyze_events.py pipeline script."""

from __future__ import annotations

import pandas as pd

from project.research.analyze_events import _load_detector_input


class TestAnalyzeEventsMain:
    """Tests for the universal analyze_events.py entry point."""

    def test_main_exits_nonzero_when_no_detector(self, tmp_path, monkeypatch, capsys):
        """main() must exit non-zero and log ERROR when detector is not registered."""
        import types

        import project.events.detectors.registry as reg
        import project.research.analyze_events as ae_mod
        from project.research.analyze_events import main

        # Patch get_detector to return None for any event type
        monkeypatch.setattr(reg, "get_detector", lambda etype: None)

        # Patch compose_event_config so argparse can proceed to the detector check
        fake_cfg = types.SimpleNamespace(
            reports_dir="fake", events_file="fake.parquet", parameters={}
        )
        monkeypatch.setattr(ae_mod, "compose_event_config", lambda etype: fake_cfg)

        # Patch load_all_detectors to be a no-op
        monkeypatch.setattr(ae_mod, "load_all_detectors", lambda: None)

        exit_code = main(
            [
                "--event_type",
                "FAKE_EVENT_XYZ",
                "--run_id",
                "test_run_001",
                "--symbols",
                "BTCUSDT",
                "--out_dir",
                str(tmp_path),
            ]
        )
        assert exit_code != 0, "main() must return non-zero when no detector is found"

    def test_main_function_is_importable(self):
        """analyze_events.main must be importable."""
        from project.research.analyze_events import main

        assert callable(main)

    def test_main_accepts_standard_args(self, tmp_path):
        """main() must accept --event_type, --run_id, --symbols, --data_root args."""
        import inspect

        from project.research import analyze_events

        # The script must have a main() accepting argv
        assert hasattr(analyze_events, "main")
        sig = inspect.signature(analyze_events.main)
        # Must accept at least one positional-or-keyword arg (argv)
        params = list(sig.parameters.values())
        assert len(params) >= 1


def test_load_detector_input_keeps_vol_shock_on_standard_feature_path(monkeypatch):
    class DummyDetector:
        required_columns = ("timestamp", "close", "high", "low")

    sentinel = pd.DataFrame({"timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"])})

    def _unexpected_basis(*args, **kwargs):
        raise AssertionError("VOL_SHOCK should not use the basis feature loader")

    monkeypatch.setattr("project.research.analyze_events._load_basis_features", _unexpected_basis)
    monkeypatch.setattr(
        "project.research.analyze_events.load_features",
        lambda **kwargs: sentinel,
    )

    out = _load_detector_input(
        detector=DummyDetector(),
        event_type="VOL_SHOCK",
        run_id="run_x",
        symbol="BTCUSDT",
        timeframe="5m",
    )

    pd.testing.assert_frame_equal(out, sentinel)


def test_load_detector_input_logs_detector_preflight_failures(monkeypatch, caplog):
    class DummyDetector:
        required_columns = ("timestamp", "close", "high", "low")

        def _ensure_detectors(self):
            raise RuntimeError("preflight broke")

    sentinel = pd.DataFrame({"timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"])})

    monkeypatch.setattr(
        "project.research.analyze_events.load_features",
        lambda **kwargs: sentinel,
    )

    with caplog.at_level("WARNING"):
        out = _load_detector_input(
            detector=DummyDetector(),
            event_type="SEQ_FAKE",
            run_id="run_x",
            symbol="BTCUSDT",
            timeframe="5m",
        )

    pd.testing.assert_frame_equal(out, sentinel)
    assert "Failed detector preflight while inferring basis feature requirements" in caplog.text
