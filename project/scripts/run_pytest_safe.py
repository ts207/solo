import os
import sys
import sysconfig
from pathlib import Path


def _candidate_site_packages() -> list[str]:
    candidates: list[str] = []
    configured = os.environ.get("EDGE_SAFE_SITE_PACKAGES")
    if configured:
        candidates.extend(configured.split(os.pathsep))

    purelib = sysconfig.get_paths().get("purelib")
    if purelib:
        candidates.append(purelib)

    executable = Path(sys.executable).resolve()
    if executable.parent.name == "bin":
        venv_root = executable.parent.parent
        version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        candidates.append(str(venv_root / "lib" / version / "site-packages"))

    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen and Path(candidate).exists():
            seen.add(candidate)
            out.append(candidate)
    return out


def _reexec_without_site() -> None:
    if not sys.flags.no_site:
        env = os.environ.copy()
        env["EDGE_SAFE_SITE_PACKAGES"] = os.pathsep.join(_candidate_site_packages())
        os.execvpe(sys.executable, [sys.executable, "-S", __file__, *sys.argv[1:]], env)


def main() -> int:
    _reexec_without_site()

    # Keep the test runner in this process after the -S re-exec.  Spawning a
    # nested pytest process made external timeouts kill only the wrapper,
    # leaving orphaned pytest children and making harness hangs hard to triage.
    pythonpath_parts = [".", *_candidate_site_packages()]
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)

    for part in reversed(pythonpath_parts):
        if part and part not in sys.path:
            sys.path.insert(0, part)

    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    os.environ.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")

    import pytest

    return int(pytest.main(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
