import cProfile
import pstats
import io
from pathlib import Path
import pandas as pd
import numpy as np
from project.research.services.candidate_discovery_service import CandidateDiscoveryConfig, execute_candidate_discovery

def profile_discovery():
    tmp_path = Path("/tmp/edge_profile")
    tmp_path.mkdir(parents=True, exist_ok=True)
    
    # Create dummy events
    ts = pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC")
    events = pd.DataFrame({
        "enter_ts": ts,
        "timestamp": ts,
        "symbol": ["BTCUSDT"] * 10,
        "event_type": ["VOL_SHOCK"] * 10,
    })
    events_path = tmp_path / "events.parquet"
    events.to_parquet(events_path)

    config = CandidateDiscoveryConfig(
        run_id="profile_run",
        symbols=("BTCUSDT",),
        config_paths=(),
        data_root=tmp_path,
        event_type="VOL_SHOCK",
        timeframe="5m",
        horizon_bars=24,
        out_dir=tmp_path / "phase2",
        run_mode="exploratory",
        split_scheme_id="WF_60_20_20",
        embargo_bars=0,
        purge_bars=0,
        train_only_lambda_used=0.0,
        discovery_profile="standard",
        candidate_generation_method="phase2_v1",
        concept_file=None,
        entry_lag_bars=1,
        shift_labels_k=0,
        fees_bps=4.0,
        slippage_bps=2.0,
        cost_bps=None,
        cost_calibration_mode="auto",
        cost_min_tob_coverage=0.6,
        cost_tob_tolerance_minutes=5,
        candidate_origin_run_id=None,
        frozen_spec_hash=None,
    )

    pr = cProfile.Profile()
    pr.enable()
    
    # We might need to mock some things if it expects data to be present
    execute_candidate_discovery(config)
    
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(30)
    print(s.getvalue())

if __name__ == "__main__":
    profile_discovery()
