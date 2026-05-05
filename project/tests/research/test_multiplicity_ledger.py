from project.research.multiplicity_ledger import (
    append_multiplicity_record,
    build_multiplicity_record,
    build_multiplicity_report,
)


def test_multiplicity_report_sums_records(tmp_path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    record = build_multiplicity_record(
        campaign_id="c1",
        run_id="r1",
        proposal_path="p.yaml",
        symbols=["BTCUSDT"],
        horizons=[12, 24],
        directions=["long", "short"],
        templates=["continuation"],
    )
    append_multiplicity_record(ledger, record)
    report = build_multiplicity_report(ledger, campaign_id="c1")
    assert report["records"] == 1
    assert report["total_estimated_hypothesis_count"] == 4
