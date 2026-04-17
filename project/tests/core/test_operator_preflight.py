from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from project.io.utils import write_parquet
from project.operator.preflight import run_preflight


def _write_frame(path: Path, timestamps: list[str]) -> None:
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1.0] * len(timestamps),
            "high": [1.0] * len(timestamps),
            "low": [1.0] * len(timestamps),
            "close": [1.0] * len(timestamps),
            "volume": [1.0] * len(timestamps),
        }
    )
    write_parquet(frame, path)


def _proposal_payload(search_spec: Path) -> dict[str, object]:
    return {
        "program_id": "btc_campaign",
        "start": "2021-01-01",
        "end": "2021-01-02",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": "VOL_SHOCK",
            },
            "filters": {
                "feature_predicates": [],
            },
            "sampling_policy": {
                "entry_lag_bars": 1,
            },
            "template": {
                "id": "mean_reversion",
            },
            "direction": "short",
            "horizon_bars": 12,
        },
        "search_spec": {
            "path": str(search_spec),
        },
    }


def _stub_translation(*args, **kwargs) -> dict[str, object]:
    return {
        "validated_plan": {
            "program_id": "btc_campaign",
            "estimated_hypothesis_count": 1,
            "required_detectors": ["vol_shock"],
            "required_features": ["ret_1"],
            "required_states": [],
        }
    }


def test_operator_preflight_passes_with_vendorless_local_raw_layout(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    raw_dir = data_root / "lake" / "raw" / "perp" / "BTCUSDT" / "ohlcv_5m"
    raw_dir.mkdir(parents=True, exist_ok=True)
    _write_frame(
        raw_dir / "part-000.parquet",
        ["2021-01-01T00:00:00Z", "2021-01-01T12:00:00Z", "2021-01-03T00:00:00Z"],
    )
    (tmp_path / "spec").mkdir()
    search_spec = tmp_path / "spec" / "search_space.yaml"
    search_spec.write_text("search: {}\n", encoding="utf-8")
    proposal_path = tmp_path / "proposal.yaml"
    import yaml

    proposal_path.write_text(yaml.safe_dump(_proposal_payload(search_spec)), encoding="utf-8")

    monkeypatch.setattr(
        "project.research.agent_io.proposal_to_experiment._build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(
            program_id="btc_campaign",
            estimated_hypothesis_count=1,
            required_detectors=["vol_shock"],
            required_features=["ret_1"],
            required_states=[],
        ),
    )
    monkeypatch.setattr(
        "project.operator.preflight.translate_and_validate_proposal", _stub_translation
    )

    out_json = tmp_path / "preflight.json"
    result = run_preflight(
        proposal_path=proposal_path,
        registry_root=Path("project/configs/registries"),
        data_root=data_root,
        out_dir=tmp_path / "out",
        json_output=out_json,
    )
    assert result["status"] == "warn"
    local_data = next(item for item in result["checks"] if item["name"] == "local_data_resolution")
    btc = local_data["details"]["BTCUSDT"]
    assert btc["ohlcv"]["status"] == "pass"
    assert btc["ohlcv"]["resolved_path"].endswith("lake/raw/perp/BTCUSDT/ohlcv_5m")
    assert out_json.exists()
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "operator_preflight_v1"


def test_operator_preflight_blocks_when_ohlcv_is_missing(tmp_path, monkeypatch) -> None:
    search_spec = tmp_path / "search_space.yaml"
    search_spec.write_text("search: {}\n", encoding="utf-8")
    proposal_path = tmp_path / "proposal.yaml"
    import yaml

    proposal_path.write_text(yaml.safe_dump(_proposal_payload(search_spec)), encoding="utf-8")

    monkeypatch.setattr(
        "project.research.agent_io.proposal_to_experiment._build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(
            program_id="btc_campaign",
            estimated_hypothesis_count=1,
            required_detectors=["vol_shock"],
            required_features=["ret_1"],
            required_states=[],
        ),
    )
    monkeypatch.setattr(
        "project.operator.preflight.translate_and_validate_proposal", _stub_translation
    )

    result = run_preflight(
        proposal_path=proposal_path,
        registry_root=Path("project/configs/registries"),
        data_root=tmp_path / "data",
        out_dir=tmp_path / "out",
    )
    assert result["status"] == "block"
    local_data = next(item for item in result["checks"] if item["name"] == "local_data_resolution")
    assert local_data["details"]["BTCUSDT"]["ohlcv"]["status"] == "block"


def test_operator_preflight_warns_when_some_local_raw_shards_are_unreadable(
    tmp_path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    raw_dir = data_root / "lake" / "raw" / "perp" / "BTCUSDT" / "ohlcv_5m"
    raw_dir.mkdir(parents=True, exist_ok=True)
    good_path = raw_dir / "good.parquet"
    bad_path = raw_dir / "bad.parquet"
    _write_frame(
        good_path,
        ["2021-01-01T00:00:00Z", "2021-01-01T12:00:00Z", "2021-01-03T00:00:00Z"],
    )
    _write_frame(
        bad_path,
        ["2021-01-01T06:00:00Z", "2021-01-01T18:00:00Z"],
    )
    search_spec = tmp_path / "search_space.yaml"
    search_spec.write_text("search: {}\n", encoding="utf-8")
    proposal_path = tmp_path / "proposal.yaml"
    import yaml

    proposal_path.write_text(yaml.safe_dump(_proposal_payload(search_spec)), encoding="utf-8")

    monkeypatch.setattr(
        "project.research.agent_io.proposal_to_experiment._build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(
            program_id="btc_campaign",
            estimated_hypothesis_count=1,
            required_detectors=["vol_shock"],
            required_features=["ret_1"],
            required_states=[],
        ),
    )
    monkeypatch.setattr(
        "project.operator.preflight.translate_and_validate_proposal", _stub_translation
    )

    from project.io import utils as io_utils

    original_read_parquet = io_utils.read_parquet

    def _patched_read_parquet(path, *args, **kwargs):
        if Path(path) == bad_path:
            raise OSError("corrupt shard")
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr("project.io.utils.read_parquet", _patched_read_parquet)

    result = run_preflight(
        proposal_path=proposal_path,
        registry_root=Path("project/configs/registries"),
        data_root=data_root,
        out_dir=tmp_path / "out",
    )

    local_data = next(item for item in result["checks"] if item["name"] == "local_data_resolution")
    ohlcv = local_data["details"]["BTCUSDT"]["ohlcv"]
    assert ohlcv["coverage"] == "full"
    assert ohlcv["unreadable_file_count"] == 1
    assert ohlcv["status"] == "warn"
