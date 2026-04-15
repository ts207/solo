"""
E3-T1: DSR must be a nonzero default in all promotion entrypoints.

Parses the source of each CLI script to extract the --min_dsr default.
Prevents regression to min_dsr=0.0 which silently disables multiple-testing protection.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from project.tests.conftest import REPO_ROOT

_REPO_ROOT = REPO_ROOT
# Add any new promotion CLI entrypoints here to keep the policy guard comprehensive.
_PROMOTION_CLI_PATHS = [
    "project/research/cli/promotion_cli.py",
]
# 0.5 is the Bailey & Lopez de Prado DSR literature floor for post-selection strategies.
# Lower values weaken multiple-testing protection; do not reduce without documented justification.
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

    # Step 1: find the add_argument block that contains '--min_dsr'
    # Match from add_argument( to the closing ), limited to ~500 chars to avoid spanning multiple calls
    block_matches = re.findall(
        r"""add_argument\s*\([^)]{0,500}['"]{1,2}--min_dsr['"]{1,2}[^)]{0,500}\)""",
        content,
        re.DOTALL,
    )
    assert block_matches, f"Could not find add_argument block for --min_dsr in {script_path}"

    # Step 2: within that block only, find default=<value>
    for block in block_matches:
        matches = re.findall(r"""default\s*=\s*([0-9]+(?:\.[0-9]*)?)""", block)
        assert matches, f"Could not find default= in --min_dsr block in {script_path}"
        for match in matches:
            value = float(match)
            assert value >= _MIN_REQUIRED_DSR, (
                f"{script_path}: --min_dsr default is {value}, must be >= {_MIN_REQUIRED_DSR}. "
                "Set a nonzero default to protect against multiple-testing inflation."
            )
