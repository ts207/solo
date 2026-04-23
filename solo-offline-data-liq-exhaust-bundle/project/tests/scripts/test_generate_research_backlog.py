from __future__ import annotations

import csv
import json
from pathlib import Path

from project.scripts import generate_research_backlog as backlog


class _FakeRegistry:
    event_ids = ["VOL_SPIKE", "LIQ_GAP"]

    @staticmethod
    def event_row(event_type: str) -> dict:
        return {"horizons": [5, 10], "templates": [f"{event_type.lower()}_tpl"]}


def test_main_falls_back_to_template_registry_when_atlas_has_no_claims(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    atlas_path = tmp_path / "knowledge_atlas.json"
    atlas_path.write_text(json.dumps({"version": 1, "claims": []}), encoding="utf-8")
    output_path = tmp_path / "research_backlog.csv"
    monkeypatch.setattr(backlog, "ATLAS_PATH", atlas_path)
    monkeypatch.setattr(backlog, "OUTPUT_PATH", output_path)
    monkeypatch.setattr(backlog, "get_domain_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(
        backlog,
        "TEMPLATE_REGISTRY_PATH",
        Path("spec/templates/registry.yaml"),
    )

    assert backlog.main() is None

    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert len(rows) == 2
    assert {row["claim_id"] for row in rows} == {"EVENT_VOL_SPIKE", "EVENT_LIQ_GAP"}
    captured = capsys.readouterr().out
    assert "No usable atlas claims found; falling back to template registry." in captured
    assert "Source mode: template_registry_fallback" in captured


def test_main_uses_atlas_claims_when_present(monkeypatch, tmp_path: Path) -> None:
    atlas_payload = {
        "version": 1,
        "claims": [
            {
                "claim_id": "c1",
                "concept_id": "C_EVENT_DEFINITIONS",
                "claim_type": "empirical",
                "statement": "VOL_SPIKE trigger with volatility threshold.",
                "status": "unverified",
                "operationalization": {"features": ["rv_24h"], "label": "ret_fwd_5"},
                "scope": {"assets": ["BTCUSDT"], "horizon": "5m", "stage": "phase2"},
                "evidence": [{"locator": "paper#1", "source_id": "src1"}],
            }
        ],
    }
    atlas_path = tmp_path / "knowledge_atlas.json"
    atlas_path.write_text(json.dumps(atlas_payload), encoding="utf-8")
    output_path = tmp_path / "research_backlog.csv"
    monkeypatch.setattr(backlog, "ATLAS_PATH", atlas_path)
    monkeypatch.setattr(backlog, "OUTPUT_PATH", output_path)

    assert backlog.main() is None

    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0]["claim_id"] == "c1"
    assert rows[0]["source_id"] == "src1"


def test_main_skips_bootstrap_internal_claims_and_falls_back(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    atlas_payload = {
        "version": 1,
        "claims": [
            {
                "claim_id": "bootstrap1",
                "concept_id": "C_EVENT_DEFINITIONS",
                "claim_type": "spec",
                "statement": "Derived from local concept spec.",
                "status": "bootstrap_internal",
                "operationalization": {"features": ["rv_24h"], "label": ""},
                "scope": {"assets": ["BTCUSDT"], "horizon": "5m", "stage": "bootstrap_internal"},
                "evidence": [{"locator": "spec/concepts/C_EVENT_DEFINITIONS.yaml", "source_id": "local"}],
            }
        ],
    }
    atlas_path = tmp_path / "knowledge_atlas.json"
    atlas_path.write_text(json.dumps(atlas_payload), encoding="utf-8")
    output_path = tmp_path / "research_backlog.csv"
    monkeypatch.setattr(backlog, "ATLAS_PATH", atlas_path)
    monkeypatch.setattr(backlog, "OUTPUT_PATH", output_path)
    monkeypatch.setattr(backlog, "get_domain_registry", lambda: _FakeRegistry())
    monkeypatch.setattr(
        backlog,
        "TEMPLATE_REGISTRY_PATH",
        Path("spec/templates/registry.yaml"),
    )

    assert backlog.main() is None

    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert len(rows) == 2
    captured = capsys.readouterr().out
    assert "Skipped 1 bootstrap-internal claims." in captured
    assert "Source mode: template_registry_fallback" in captured
