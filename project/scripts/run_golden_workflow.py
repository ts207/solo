from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT
from project.reliability.cli_smoke import run_smoke_cli
from project.spec_registry import load_yaml_path


def _default_config_path() -> Path:
    return PROJECT_ROOT / "configs" / "golden_workflow.yaml"


def load_workflow_config(path: Path) -> dict[str, Any]:
    payload = load_yaml_path(path) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Golden workflow config must be a mapping: {path}")
    return dict(payload)


def run_golden_workflow(*, root: Path, config_path: Path) -> dict[str, Any]:
    config = load_workflow_config(config_path)
    mode = str(config.get("mode", "full") or "full").strip()
    seed = int(config.get("seed", 20260101) or 20260101)
    storage_mode = str(config.get("storage_mode", "auto") or "auto").strip()

    summary = run_smoke_cli(mode, root=root, seed=seed, storage_mode=storage_mode)
    payload = {
        "workflow_id": str(config.get("workflow_id", "golden_workflow_v1")),
        "config_path": str(config_path),
        "root": str(root),
        "mode": mode,
        "seed": seed,
        "storage_mode": storage_mode,
        "required_outputs": list(config.get("required_outputs", [])),
        "summary": summary,
    }
    out_path = root / "reliability" / "golden_workflow_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the canonical golden workflow.")
    parser.add_argument("--root", default=None, help="Output root for generated smoke artifacts.")
    parser.add_argument(
        "--config", default=str(_default_config_path()), help="Workflow config YAML path."
    )
    args = parser.parse_args(argv)

    root = Path(args.root) if args.root else (PROJECT_ROOT.parent / "artifacts" / "golden_workflow")
    run_golden_workflow(root=root, config_path=Path(args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
