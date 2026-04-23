from __future__ import annotations

from project.tests.conftest import REPO_ROOT


def test_no_windows_ads_sidecars_in_repo() -> None:
    offenders = sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in REPO_ROOT.rglob("*")
        if path.is_file()
        and path.name.endswith("Zone.Identifier")
        and ".git/" not in path.as_posix()
        and ".venv/" not in path.as_posix()
    )
    assert offenders == []
