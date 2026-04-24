from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project import PROJECT_ROOT  # noqa: E402


def _repo_root() -> Path:
    return PROJECT_ROOT.parent


def collect_repo_metrics(root: Path | None = None) -> dict[str, object]:
    repo_root = (root or _repo_root()).resolve()
    project_root = repo_root / "project"
    tests_root = project_root / "tests"
    spec_root = repo_root / "spec"
    docs_root = repo_root / "docs"

    project_py = sum(1 for _ in project_root.rglob("*.py")) if project_root.exists() else 0
    test_py = sum(1 for _ in tests_root.rglob("*.py")) if tests_root.exists() else 0
    test_files = sum(1 for _ in tests_root.rglob("test_*.py")) if tests_root.exists() else 0
    spec_yaml = sum(1 for _ in spec_root.rglob("*.yaml")) if spec_root.exists() else 0
    docs_md = 0
    if docs_root.exists():
        docs_md = sum(
            1
            for path in docs_root.rglob("*.md")
            if "generated" not in path.relative_to(docs_root).parts
        )

    package_counts: list[dict[str, object]] = []
    if project_root.exists():
        for child in sorted(
            p for p in project_root.iterdir() if p.is_dir() and p.name != "__pycache__"
        ):
            py_count = sum(1 for _ in child.rglob('*.py'))
            if py_count == 0:
                continue
            package_counts.append({'package': child.name, 'python_files': py_count})

    return {
        'schema_version': 'repo_metrics_v1',
        'project_python_files': project_py,
        'test_python_files': test_py,
        'test_files': test_files,
        'spec_yaml_files': spec_yaml,
        'docs_markdown_files': docs_md,
        'top_packages': sorted(
            package_counts,
            key=lambda row: (-int(row['python_files']), str(row['package'])),
        )[:12],
    }


def render_repo_metrics_markdown(metrics: dict[str, object]) -> str:
    lines = [
        '# Repository Metrics',
        '',
        f"- Project Python files: **{metrics['project_python_files']}**",
        f"- Test Python files: **{metrics['test_python_files']}**",
        f"- `test_*.py` files: **{metrics['test_files']}**",
        f"- Spec YAML files: **{metrics['spec_yaml_files']}**",
        f"- Docs Markdown files: **{metrics['docs_markdown_files']}**",
        '',
        '## Largest Packages',
        '',
        '| Package | Python files |',
        '|---|---:|',
    ]
    for row in metrics['top_packages']:
        lines.append(f"| `{row['package']}` | {row['python_files']} |")
    lines.append('')
    return '\n'.join(lines)


def render_repo_metrics_json(metrics: dict[str, object]) -> str:
    return json.dumps(metrics, indent=2, sort_keys=True)


def update_root_readme_metrics(readme_text: str, metrics: dict[str, object]) -> str:
    generated_block = (
        "<!-- repo-metrics:start -->\n"
        f"- {metrics['project_python_files']} Python modules under `project/`\n"
        f"- {metrics['test_python_files']} test files under `project/tests/`\n"
        f"- {metrics['spec_yaml_files']} YAML spec files under `spec/`\n"
        "<!-- repo-metrics:end -->"
    )
    block_pattern = r"<!-- repo-metrics:start -->.*?<!-- repo-metrics:end -->"
    updated, count = re.subn(block_pattern, generated_block, readme_text, count=1, flags=re.DOTALL)
    if count == 1:
        return updated

    replacements = {
        r'- \d+ Python modules under `project/`': (
            f"- {metrics['project_python_files']} Python modules under `project/`"
        ),
        r'- \d+ test files under `project/tests/`': (
            f"- {metrics['test_python_files']} test files under `project/tests/`"
        ),
        r'- \d+ YAML spec files under `spec/`': (
            f"- {metrics['spec_yaml_files']} YAML spec files under `spec/`"
        ),
    }
    updated = readme_text
    replacement_count = 0
    for pattern, replacement in replacements.items():
        updated, count = re.subn(pattern, replacement, updated, count=1)
        replacement_count += count
    if replacement_count not in {0, len(replacements)}:
        raise ValueError(
            "README metrics block is partially present; "
            "add repo-metrics markers or remove the stale block"
        )
    return updated


def _target_paths(root: Path) -> tuple[Path, Path, Path]:
    generated = root / 'docs' / 'generated'
    return generated / 'repo_metrics.md', generated / 'repo_metrics.json', root / 'README.md'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Generate repository metrics artifacts and refresh README counts.'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Fail if generated/readme outputs drift from disk.',
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    metrics = collect_repo_metrics(repo_root)
    markdown = render_repo_metrics_markdown(metrics)
    json_text = render_repo_metrics_json(metrics)
    md_path, json_path, readme_path = _target_paths(repo_root)
    updated_readme = update_root_readme_metrics(readme_path.read_text(encoding='utf-8'), metrics)

    expected = [
        (md_path, markdown),
        (json_path, json_text),
        (readme_path, updated_readme),
    ]

    if args.check:
        drift: list[str] = []
        for path, content in expected:
            current = path.read_text(encoding='utf-8') if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f'repo metrics drift: {path}', file=sys.stderr)
            return 1
        return 0

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown, encoding='utf-8')
    json_path.write_text(json_text, encoding='utf-8')
    readme_path.write_text(updated_readme, encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
