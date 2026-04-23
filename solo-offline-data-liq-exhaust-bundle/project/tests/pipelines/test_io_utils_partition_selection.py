from __future__ import annotations

from project.io.utils import choose_partition_dir


def test_choose_partition_dir_allows_fallback_when_not_strict(monkeypatch, tmp_path):
    run_scoped = tmp_path / "runscope"
    canonical = tmp_path / "canonical"
    canonical.mkdir(parents=True, exist_ok=True)
    (canonical / "slice.csv").write_text("x\n1\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_STRICT_RUN_SCOPED_READS", "0")
    chosen = choose_partition_dir([run_scoped, canonical])
    assert chosen == canonical


def test_choose_partition_dir_fails_closed_in_strict_mode(monkeypatch, tmp_path):
    run_scoped = tmp_path / "runscope"
    canonical = tmp_path / "canonical"
    canonical.mkdir(parents=True, exist_ok=True)
    (canonical / "slice.csv").write_text("x\n1\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_STRICT_RUN_SCOPED_READS", "1")
    chosen = choose_partition_dir([run_scoped, canonical])
    assert chosen is None


def test_choose_partition_dir_uses_first_candidate_when_strict(monkeypatch, tmp_path):
    run_scoped = tmp_path / "runscope"
    run_scoped.mkdir(parents=True, exist_ok=True)
    (run_scoped / "slice.parquet").write_text("placeholder", encoding="utf-8")
    canonical = tmp_path / "canonical"
    canonical.mkdir(parents=True, exist_ok=True)
    (canonical / "slice.csv").write_text("x\n1\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_STRICT_RUN_SCOPED_READS", "1")
    chosen = choose_partition_dir([run_scoped, canonical])
    assert chosen == run_scoped
