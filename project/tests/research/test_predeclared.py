from __future__ import annotations

from pathlib import Path

from project.research.predeclared import validate_predeclared_hypotheses


def test_validate_predeclared_hypotheses_passes(tmp_path: Path) -> None:
    path = tmp_path / "predeclared.yaml"
    path.write_text(
        """
hypotheses:
  - id: h1
    mechanism: test mechanism
    event_id: VOL_SHOCK
    template: continuation
    direction: long
    horizon_bars: 12
    symbol: BTCUSDT
    timeframe: 5m
""",
        encoding="utf-8",
    )
    payload = validate_predeclared_hypotheses(path)
    assert payload["status"] == "pass"
    assert payload["count"] == 1


def test_validate_predeclared_hypotheses_rejects_missing_fields(tmp_path: Path) -> None:
    path = tmp_path / "predeclared.yaml"
    path.write_text("hypotheses:\n  - id: h1\n", encoding="utf-8")
    payload = validate_predeclared_hypotheses(path)
    assert payload["status"] == "fail"
    assert payload["errors"]
