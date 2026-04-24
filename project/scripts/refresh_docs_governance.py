from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project import PROJECT_ROOT  # noqa: E402

REPO_ROOT = PROJECT_ROOT.parent

SCRIPTS = [
    'project/scripts/build_repo_metrics.py',
    'project/scripts/build_system_map.py',
    'project/scripts/build_contract_strictness_inventory.py',
    'project/scripts/build_detector_governance_artifacts.py',
    'project/scripts/build_legacy_surface_inventory.py',
]


def _run(script: str, *, check: bool) -> int:
    cmd = [sys.executable, str(REPO_ROOT / script)]
    if check:
        cmd.append('--check')
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Refresh generated docs and governance inventories.'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Fail if generated outputs drift from disk.',
    )
    args = parser.parse_args(argv)

    failures: list[str] = []
    for script in SCRIPTS:
        rc = _run(script, check=bool(args.check))
        if rc != 0:
            failures.append(script)
    if failures:
        for script in failures:
            print(f'docs/governance refresh failed: {script}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
