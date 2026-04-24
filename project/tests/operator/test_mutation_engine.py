
import yaml

from project.operator.mutation_engine import generate_next_proposal


def _baseline_payload():
    return {
        "program_id": "btc_campaign",
        "start": "2021-01-01",
        "end": "2021-12-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "hypothesis": {
            "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
            "template": {"id": "mean_reversion"},
            "direction": "short",
            "horizon_bars": 12,
            "entry_lag_bars": 1,
        },
        "search_spec": {},
    }


def test_generate_next_proposal_changes_exactly_one_field(tmp_path):
    baseline_path = tmp_path / "baseline.yaml"
    baseline_path.write_text(yaml.safe_dump(_baseline_payload()), encoding="utf-8")

    result = generate_next_proposal(
        baseline_proposal_path=baseline_path,
        parent_run_id="run_base",
        diagnostics={"diagnosis": "low_sample_power"},
        decision={"classification": "near_miss"},
        campaign_id="camp1",
        cycle_number=2,
    )
    payload = result.proposal_payload
    assert payload["horizons_bars"] != [12]
    assert payload["entry_lags"] == [1]
    assert payload["bounded"]["allowed_change_field"] == "horizons_bars"
    assert payload["campaign"]["campaign_id"] == "camp1"
