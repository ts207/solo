from __future__ import annotations

from project.io.funnel import append_funnel_index, load_funnel, write_funnel


def test_funnel_round_trip_and_index(tmp_path) -> None:
    payload = {
        "schema_version": 1,
        "run_id": "r1",
        "program_id": "p1",
        "proposal_id": "proposal",
        "stages": {
            "generated": {"count": 3},
            "feasible": {"count": 2},
            "t_net_passed": {"count": 1},
        },
    }
    path = write_funnel("r1", payload, data_root=tmp_path)
    assert path.exists()
    assert load_funnel("r1", data_root=tmp_path)["run_id"] == "r1"

    index_path = append_funnel_index(payload, data_root=tmp_path)
    assert index_path.exists()
