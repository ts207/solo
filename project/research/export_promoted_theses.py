from __future__ import annotations

import argparse
import logging
from pathlib import Path

from project.research.live_export import export_promoted_theses_for_run


def _parse_deployment_state_overrides(values: list[str] | None) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for raw in values or []:
        selector, sep, state = str(raw or "").partition("=")
        if not sep or not selector.strip() or not state.strip():
            raise ValueError(
                "Deployment-state overrides must use the form '<thesis_id_or_candidate_id>=<deployment_state>'."
            )
        overrides[selector.strip()] = state.strip()
    return overrides


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export live-usable promoted thesis payloads.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--data_root", default=None)
    parser.add_argument(
        "--register-runtime",
        default=None,
        help="Optional named runtime registration to record in the thesis index without changing runtime defaults.",
    )
    parser.add_argument(
        "--set-deployment-state",
        action="append",
        default=None,
        help="Explicit override in the form '<thesis_id_or_candidate_id>=<deployment_state>'. Repeat as needed.",
    )
    parser.add_argument(
        "--allow-bundle-only-export",
        type=int,
        default=0,
        help="Permit zero-thesis exports when validation produced no promotable candidates.",
    )
    args = parser.parse_args(argv)

    try:
        deployment_state_overrides = _parse_deployment_state_overrides(args.set_deployment_state)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        result = export_promoted_theses_for_run(
            args.run_id,
            data_root=Path(args.data_root) if args.data_root else None,
            deployment_state_overrides=deployment_state_overrides,
            register_runtime_name=args.register_runtime,
            allow_bundle_only_export=bool(args.allow_bundle_only_export),
        )
    except ValueError as exc:
        logging.error("%s", exc)
        return 2
    logging.info(
        "Exported %s theses for %s to %s",
        result.thesis_count,
        result.run_id,
        result.output_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
