from __future__ import annotations

from pathlib import Path

import pandas as pd

import project.research.compile_strategy_blueprints as compiler
from project.events.registry import EVENT_REGISTRY_SPECS


def _write_events_csv(root: Path, run_id: str, spec_event_type: str, rows: list[dict]) -> Path:
    spec = EVENT_REGISTRY_SPECS[spec_event_type]
    path = root / "reports" / spec.reports_dir / run_id / spec.events_file
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return path


def test_event_stats_subtype_filters_shared_funding_file(monkeypatch, tmp_path):
    run_id = "r_stats_subtype"
    _write_events_csv(
        tmp_path,
        run_id,
        "FUNDING_EXTREME_ONSET",
        rows=[
            {
                "event_type": "FUNDING_EXTREME_ONSET",
                "time_to_secondary_shock": 10.0,
                "adverse_proxy_excess": 0.2,
                "forward_abs_return_h": 0.3,
            },
            {
                "event_type": "FUNDING_PERSISTENCE_TRIGGER",
                "time_to_secondary_shock": 20.0,
                "adverse_proxy_excess": 0.4,
                "forward_abs_return_h": 0.5,
            },
        ],
    )
    monkeypatch.setattr(compiler, "DATA_ROOT", tmp_path)
    stats = compiler._event_stats(run_id=run_id, event_type="FUNDING_EXTREME_ONSET")
