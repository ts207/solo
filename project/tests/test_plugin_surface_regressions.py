import pytest
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).parents[2]


def _has_edge_agents() -> bool:
    return (_repo_root() / "plugins" / "edge-agents").exists()


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_plugin_readme_tracks_current_plugin_surfaces() -> None:
    path = _repo_root() / "plugins" / "edge-agents" / "README.md"
    text = path.read_text(encoding="utf-8")
    assert "make discover|validate|promote|export|bind-config|paper-run|live-run|deploy-status" in text
    assert "edge_validate_repo.sh contracts|minimum-green|all" in text
    assert "edge_sync_plugin.sh targets|check|sync" in text
    assert "edge_export_theses.sh" in text
    assert "bootstrap" not in text


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_export_wrapper_exists_and_targets_run_export_module() -> None:
    path = _repo_root() / "plugins" / "edge-agents" / "scripts" / "edge_export_theses.sh"
    text = path.read_text(encoding="utf-8")
    assert "--run_id" in text
    assert "project.research.export_promoted_theses" in text


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_validate_repo_wrapper_uses_supported_modes() -> None:
    path = _repo_root() / "plugins" / "edge-agents" / "scripts" / "edge_validate_repo.sh"
    text = path.read_text(encoding="utf-8")
    assert "contracts|minimum-green|all" in text
    assert "run_researcher_verification --mode contracts" in text
    assert "make minimum-green-gate" in text
    assert "make validate" not in text


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_sync_wrapper_supports_target_discovery() -> None:
    path = _repo_root() / "plugins" / "edge-agents" / "scripts" / "edge_sync_plugin.sh"
    text = path.read_text(encoding="utf-8")
    assert "[targets|sync|check]" in text
    assert "discover_targets()" in text
    assert "plugins/cache/edge-local/edge-agents/local" in text
    assert "if diff -qr" in text


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_plugin_files_do_not_reference_removed_docs_or_commands() -> None:
    root = _repo_root() / "plugins" / "edge-agents"
    banned = [
        "docs/AGENT_CONTRACT.md",
        "docs/VERIFICATION.md",
        "docs/08_TESTING_AND_MAINTENANCE.md",
        "docs/04_COMMANDS_AND_ENTRY_POINTS.md",
        "docs/03_OPERATOR_WORKFLOW.md",
        "docs/09_THESIS_BOOTSTRAP_AND_PROMOTION.md",
        "docs/10_APPS_PLUGINS_AND_AGENTS.md",
        "docs/00_START_HERE.md",
        "docs/05_ARTIFACTS_AND_INTERPRETATION.md",
        "docs/11_LIVE_THESIS_STORE_AND_OVERLAP.md",
        "project.scripts.generate_operator_surface_inventory",
        "DISCOVER_ACTION=preflight",
        "make discover|promote|export|deploy-paper",
    ]
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for needle in banned:
            assert needle not in text, f"{needle} still referenced in {path.relative_to(_repo_root())}"


@pytest.mark.skipif(not _has_edge_agents(), reason="edge-agents plugin not present")
def test_edge_agents_mechanism_hypothesis_skill_references_current_files() -> None:
    root = _repo_root() / "plugins" / "edge-agents"

    skill_path = root / "skills" / "edge-mechanism-hypothesis" / "SKILL.md"
    agent_path = root / "agents" / "mechanism-hypothesis.md"
    
    if not skill_path.exists() or not agent_path.exists():
        pytest.skip("skill or agent files not present")

    skill_text = skill_path.read_text(encoding="utf-8")
    agent_text = agent_path.read_text(encoding="utf-8")

    assert "agents/mechanism-hypothesis.md" in skill_text
    assert "agents/mechanism_hypothesis.md" not in skill_text
    assert "agents/handoffs/analyst_to_mechanism_hypothesis.md" not in skill_text
    assert "Read this file for the full spec before beginning." in agent_text
    assert "agents/mechanism_hypothesis.md" not in agent_text
