from __future__ import annotations

import logging

from project.core.logging_utils import build_stage_log_handlers


def test_build_stage_log_handlers_skips_file_handler_when_stdout_is_redirected(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("BACKTEST_STAGE_STDOUT_CAPTURED", "1")

    handlers = build_stage_log_handlers(str(tmp_path / "stage.log"))

    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.StreamHandler)
    assert not any(isinstance(handler, logging.FileHandler) for handler in handlers)
