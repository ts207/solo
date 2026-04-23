from __future__ import annotations

import tomllib
from pathlib import Path


CANONICAL_COMMANDS = [
    "backtest",
    "edge-backtest",
    "edge-live-engine",
    "edge-run-all",
    "edge-phase2-discovery",
    "edge-promote",
]
REMOVED_ALIASES = [
    "run-all",
    "promote-candidates",
    "phase2-discovery",
]
CANONICAL_SCRIPT_TARGETS = {
    "edge-phase2-discovery": "project.research.cli.candidate_discovery_cli:main",
    "edge-promote": "project.research.cli.promotion_cli:main",
    "compile-strategy-blueprints": "project.research.compile_strategy_blueprints:main",
    "build-strategy-candidates": "project.research.build_strategy_candidates:main",
}


def test_canonical_commands_packaged_and_extended_detectors_removed():
    data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    scripts = data["project"]["scripts"]
    for cmd in CANONICAL_COMMANDS:
        assert cmd in scripts
    for cmd in REMOVED_ALIASES:
        assert cmd not in scripts


def test_research_console_scripts_point_to_canonical_modules():
    data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    scripts = data["project"]["scripts"]
    for command_name, target in CANONICAL_SCRIPT_TARGETS.items():
        assert scripts.get(command_name) == target
