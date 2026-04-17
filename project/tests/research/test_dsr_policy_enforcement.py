"""
E3-T1: DSR must be a nonzero default in all promotion entrypoints.

Verifies that the runtime default for --min_dsr is >= 0.5 in each
promotion CLI script.  Prevents regression to min_dsr=0.0 which
silently disables multiple-testing protection.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from project.tests.conftest import REPO_ROOT

_REPO_ROOT = REPO_ROOT
_PROMOTION_CLI_PATHS = [
    "project/research/cli/promotion_cli.py",
]
_MIN_REQUIRED_DSR = 0.5


@pytest.mark.parametrize("script_path", _PROMOTION_CLI_PATHS)
def test_promotion_cli_min_dsr_is_nonzero(script_path):
    """
    Production-profile promotion entrypoints must default min_dsr >= 0.5.
    Regression to 0.0 silently disables multiple-testing correction.
    """
    path = _REPO_ROOT / script_path
    assert path.exists(), f"Entrypoint not found: {path}"

    content = path.read_text()

    block_matches = re.findall(
        r"""add_argument\s*\([^)]{0,500}['"]{1,2}--min_dsr['"]{1,2}[^)]{0,500}\)""",
        content,
        re.DOTALL,
    )
    assert block_matches, f"Could not find add_argument block for --min_dsr in {script_path}"

    for block in block_matches:
        literal_matches = re.findall(r"""default\s*=\s*([0-9]+(?:\.[0-9]*)?)""", block)
        if literal_matches:
            for match in literal_matches:
                value = float(match)
                assert value >= _MIN_REQUIRED_DSR, (
                    f"{script_path}: --min_dsr default is {value}, must be >= {_MIN_REQUIRED_DSR}."
                )
        else:
            const_matches = re.findall(
                r"""default\s*=\s*([A-Za-z_][A-Za-z0-9_]*(?:\[.*?\])?)""", block
            )
            assert const_matches, f"Could not find default= in --min_dsr block in {script_path}"
            from project.research.cli.promotion_cli import PROMOTION_CONFIG_DEFAULTS

            actual_min_dsr = float(PROMOTION_CONFIG_DEFAULTS.get("min_dsr", 0.0))
            assert actual_min_dsr >= _MIN_REQUIRED_DSR, (
                f"{script_path}: --min_dsr resolves to {actual_min_dsr}, must be >= {_MIN_REQUIRED_DSR}."
            )
