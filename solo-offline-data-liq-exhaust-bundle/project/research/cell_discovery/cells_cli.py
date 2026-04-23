from __future__ import annotations

import argparse
from typing import Any

from project.research.cell_discovery.cells_service import (
    assemble_cell_theses,
    plan_cells,
    run_cells,
    summarize_cells,
    verify_data,
)


def run_from_namespace(args: argparse.Namespace) -> dict[str, Any]:
    action = str(getattr(args, "cells_action", "")).strip()
    if action == "verify-data":
        return verify_data(
            run_id=args.run_id,
            symbols=args.symbols,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            data_root=args.data_root,
            spec_dir=args.spec_dir,
        )
    if action == "plan":
        return plan_cells(
            run_id=args.run_id,
            symbols=args.symbols,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            data_root=args.data_root,
            spec_dir=args.spec_dir,
        )
    if action == "run":
        return run_cells(
            run_id=args.run_id,
            symbols=args.symbols,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            data_root=args.data_root,
            spec_dir=args.spec_dir,
            registry_root=args.registry_root,
            search_budget=args.search_budget,
        )
    if action == "summarize":
        return summarize_cells(run_id=args.run_id, data_root=args.data_root)
    if action == "assemble-theses":
        return assemble_cell_theses(
            run_id=args.run_id,
            data_root=args.data_root,
            limit=args.limit,
        )
    return {"exit_code": 2, "status": "error", "reason": f"unknown cells action: {action}"}
