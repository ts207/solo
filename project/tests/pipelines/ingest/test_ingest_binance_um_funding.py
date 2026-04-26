from __future__ import annotations

import sys
from datetime import UTC, datetime

from project.pipelines.ingest import ingest_binance_um_funding as funding


def test_main_fails_when_required_funding_coverage_is_missing(monkeypatch, tmp_path):
    finalized: dict[str, object] = {}

    monkeypatch.setattr(funding, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(
        funding,
        "_iter_months",
        lambda _start, _end: [datetime(2026, 1, 1, tzinfo=UTC)],
    )

    class _DownloadResult:
        status = "not_found"
        error = "missing archive"

    class _DummySession:
        pass

    monkeypatch.setattr(funding.requests, "Session", lambda: _DummySession())
    monkeypatch.setattr(
        funding,
        "download_with_retries",
        lambda *_args, **_kwargs: _DownloadResult(),
    )
    monkeypatch.setattr(funding, "start_manifest", lambda *args, **kwargs: {})

    def fake_finalize_manifest(manifest, status, **kwargs):
        finalized["status"] = status
        finalized["error"] = kwargs.get("error")
        finalized["stats"] = kwargs.get("stats")

    monkeypatch.setattr(funding, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest_binance_um_funding.py",
            "--run_id",
            "funding_missing_coverage",
            "--symbols",
            "BTCUSDT",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--use_api_fallback",
            "0",
        ],
    )

    rc = funding.main()

    assert rc == 1
    assert finalized["status"] == "failed"
    assert "missing required funding coverage" in str(finalized["error"])
