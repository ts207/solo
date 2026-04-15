from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from project.reliability.smoke_data import build_smoke_dataset, run_research_smoke


def test_research_smoke_produces_out_of_sample_evidence(tmp_path: Path):
    dataset = build_smoke_dataset(tmp_path, seed=20260101, storage_mode="auto")
    research_result = run_research_smoke(dataset)

    combined = research_result["combined_candidates"]
    assert len(combined) > 0, "Smoke research produced no candidates"

    oos_obs = combined[["validation_n_obs", "test_n_obs"]].fillna(0).sum(axis=1)
    assert (oos_obs > 0).any(), (
        "Smoke research produced no validation/test observations; "
        "the synthetic dataset no longer exercises out-of-sample scoring"
    )


def test_smoke_generators_are_deterministic_across_processes() -> None:
    code = """
import json
from project.reliability.smoke_data import build_smoke_bars, build_smoke_events

bars = build_smoke_bars("BTCUSDT", seed=20260101)
events = build_smoke_events("BTCUSDT", seed=20260101)
print(json.dumps({
    "bars_first_close": float(bars["close"].iloc[0]),
    "bars_last_close": float(bars["close"].iloc[-1]),
    "events_first_return": float(events["return_24"].iloc[0]),
    "events_last_return": float(events["return_24"].iloc[-1]),
}, sort_keys=True))
""".strip()
    outputs = [
        subprocess.check_output([sys.executable, "-c", code], text=True).strip() for _ in range(3)
    ]
    payloads = [json.loads(item) for item in outputs]
    assert payloads[1:] == [payloads[0], payloads[0]]
