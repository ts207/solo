from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

import project.pipelines.execution_engine as engine


def test_stage_cache_scope_treats_phase2_search_engine_as_phase2():
    assert engine.is_phase2_stage("phase2_search_engine") is True
    assert engine.is_phase2_stage("build_features_5m") is False


def test_run_stage_cache_hit_short_circuits_subprocess(monkeypatch, tmp_path):
    run_id = "cache_hit_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")
    stage = "phase2_conditional_hypotheses"
    base_args = ["--event_type", "VOL_SHOCK"]
    stage_instance = engine.stage_instance_base(stage, base_args)
    cache_context = {
        "feature_schema_version": "v2",
        "pipeline_session_id": "",
        "require_stage_manifest": False,
    }
    input_hash = engine.compute_stage_input_hash(
        script_path, base_args, run_id, cache_context=cache_context
    )
    manifest_path = data_root / "runs" / run_id / f"{stage_instance}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    # We need to satisfy _manifest_declared_outputs_exist and validation.
    manifest_path.write_text(
        json.dumps(
            {
                "status": "success",
                "input_hash": input_hash,
                "outputs": [{"path": str(script_path)}],
                "run_id": run_id,
                "stage": stage,
                "stage_instance_id": stage_instance,
                "started_at": "2024-01-01T00:00:00Z",
                "finished_at": "2024-01-01T00:00:01Z",
                "parameters": {},
                "inputs": [],
                "spec_hashes": {},
                "ontology_spec_hash": "sha256:abc",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("BACKTEST_STAGE_CACHE", "1")

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("subprocess.Popen should not be called on cache hit")

    monkeypatch.setattr(engine.subprocess, "Popen", _fail_if_called)
    stage_cache_meta: dict[str, dict[str, object]] = {}
    ok = engine.run_stage(
        stage,
        script_path,
        base_args,
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id=None,
        stage_cache_meta=stage_cache_meta,
    )
    assert ok is True
    assert stage_cache_meta[stage_instance]["cache_hit"] is True


def test_run_stage_retries_once_then_succeeds(monkeypatch, tmp_path):
    run_id = "retry_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")
    calls = {"count": 0}

    class _FakeProc:
        def __init__(self, returncode: int, stdout_handle, stdout_text: str = ""):
            self.returncode = int(returncode)
            self._stdout_handle = stdout_handle
            self._stdout_text = str(stdout_text)
            self.pid = 12345

        def wait(self, timeout=None):
            if self._stdout_text:
                self._stdout_handle.write(self._stdout_text.encode("utf-8"))
                self._stdout_handle.flush()
            return self.returncode

        def poll(self):
            return self.returncode

    def _fake_popen(*_args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return _FakeProc(2, kwargs["stdout"], "attempt1")
        return _FakeProc(0, kwargs["stdout"], "attempt2")

    monkeypatch.setattr(engine.subprocess, "Popen", _fake_popen)
    stage_cache_meta: dict[str, dict[str, object]] = {}
    ok = engine.run_stage(
        "ingest_binance_um_ohlcv_5m",
        script_path,
        ["--symbols", "BTCUSDT"],
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id=None,
        stage_cache_meta=stage_cache_meta,
        max_attempts=2,
        retry_backoff_sec=0.0,
    )
    assert ok is True
    assert calls["count"] == 2


def test_run_stage_buffers_output_with_stage_prefix(monkeypatch, tmp_path, capsys):
    run_id = "buffered_output_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    class _FakeProc:
        def __init__(self, stdout_handle):
            self.returncode = 0
            self._stdout_handle = stdout_handle
            self.pid = 9001

        def wait(self, timeout=None):
            self._stdout_handle.write(b"line_a\nline_b\n")
            self._stdout_handle.flush()
            return self.returncode

        def poll(self):
            return self.returncode

    monkeypatch.setattr(
        engine.subprocess, "Popen", lambda *_a, **kwargs: _FakeProc(kwargs["stdout"])
    )
    stage_cache_meta: dict[str, dict[str, object]] = {}
    ok = engine.run_stage(
        "build_features",
        script_path,
        ["--symbols", "BTCUSDT"],
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id="build_features_custom",
        stage_cache_meta=stage_cache_meta,
    )
    assert ok is True
    captured = capsys.readouterr()
    assert "[build_features_custom] buffered output (build_features)" in captured.out
    assert "[build_features_custom] line_a" in captured.out
    log_path = data_root / "runs" / run_id / "build_features_custom.log"
    assert "line_a" in log_path.read_text(encoding="utf-8")


def test_run_stage_sets_stage_identity_in_subprocess_env(monkeypatch, tmp_path):
    run_id = "env_forwarding_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")
    captured_env: dict[str, str] = {}

    class _FakeProc:
        def __init__(self, stdout_handle):
            self.returncode = 0
            self._stdout_handle = stdout_handle
            self.pid = 999

        def wait(self, timeout=None):
            self._stdout_handle.write(b"ok\n")
            self._stdout_handle.flush()
            return self.returncode

        def poll(self):
            return self.returncode

    def _fake_popen(*_args, **kwargs):
        captured_env.update(kwargs["env"])
        return _FakeProc(kwargs["stdout"])

    monkeypatch.setattr(engine.subprocess, "Popen", _fake_popen)
    stage_cache_meta: dict[str, dict[str, object]] = {}

    ok = engine.run_stage(
        "build_features",
        script_path,
        ["--symbols", "BTCUSDT"],
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id="pipeline_session_a",
        current_stage_instance_id="build_features__worker_a",
        stage_cache_meta=stage_cache_meta,
    )

    assert ok is True
    assert captured_env["BACKTEST_RUN_ID"] == run_id
    assert captured_env["BACKTEST_STAGE_INSTANCE_ID"] == "build_features__worker_a"
    assert captured_env["BACKTEST_PIPELINE_SESSION_ID"] == "pipeline_session_a"


def test_run_stages_parallel_sequential_path_stops_on_failure():
    seen: list[str] = []

    def _worker(args):
        stage_instance_id, stage_name, _script, _base_args, _run_id = args
        seen.append(stage_name)
        ok = stage_name != "stage_2"
        return stage_instance_id, stage_name, ok, 0.01, {"cache_enabled": False}

    stages = [
        ("s1", "stage_1", Path("a.py"), []),
        ("s2", "stage_2", Path("b.py"), []),
        ("s3", "stage_3", Path("c.py"), []),
    ]
    all_ok, timings = engine.run_stages_parallel(stages, "run_a", 1, worker_fn=_worker)
    assert all_ok is False
    assert [row[1] for row in timings] == ["stage_1", "stage_2"]
    assert seen == ["stage_1", "stage_2"]


def test_run_stages_parallel_parallel_branch(monkeypatch):
    class _FakeFuture:
        def __init__(self, result):
            self._result = result
            self.was_cancelled = False

        def result(self):
            return self._result

        def cancel(self):
            self.was_cancelled = True
            return True

    class _FakePool:
        def __init__(self, max_workers: int):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, arg):
            return _FakeFuture(fn(arg))

    def _fake_as_completed(futures):
        return list(futures)[::-1]

    monkeypatch.setattr(engine.concurrent.futures, "ThreadPoolExecutor", _FakePool)
    monkeypatch.setattr(engine.concurrent.futures, "as_completed", _fake_as_completed)

    def _worker(args):
        stage_instance_id, stage_name, _script, _base_args, _run_id = args
        return stage_instance_id, stage_name, True, 0.02, {"cache_enabled": False}

    stages = [
        ("s1", "stage_1", Path("a.py"), []),
        ("s2", "stage_2", Path("b.py"), []),
    ]
    all_ok, timings = engine.run_stages_parallel(stages, "run_b", 4, worker_fn=_worker)
    assert all_ok is True
    assert len(timings) == 2
    assert {row[1] for row in timings} == {"stage_1", "stage_2"}


def test_run_stages_parallel_falls_back_on_thread_pool_error(monkeypatch):
    class _RaisingPool:
        def __init__(self, max_workers: int):
            raise PermissionError(f"blocked-{max_workers}")

    monkeypatch.setattr(engine.concurrent.futures, "ThreadPoolExecutor", _RaisingPool)
    seen: list[str] = []

    def _worker(args):
        stage_instance_id, stage_name, _script, _base_args, _run_id = args
        seen.append(stage_name)
        return stage_instance_id, stage_name, True, 0.01, {"cache_enabled": False}

    stages = [
        ("s1", "stage_1", Path("a.py"), []),
        ("s2", "stage_2", Path("b.py"), []),
    ]
    all_ok, timings = engine.run_stages_parallel(stages, "run_c", 2, worker_fn=_worker)
    assert all_ok is True
    assert len(timings) == 2
    assert seen == ["stage_1", "stage_2"]


def test_run_stages_parallel_fail_fast_terminates_pending_stage_instances(monkeypatch):
    terminated: list[tuple[str, ...]] = []

    def _fake_terminate(_run_id, stage_instance_ids):
        terminated.append(tuple(stage_instance_ids))

    monkeypatch.setattr(engine, "terminate_stage_instances", _fake_terminate)

    class _FakeFuture:
        def __init__(self, result):
            self._result = result
            self.cancelled = False

        def result(self):
            return self._result

        def cancel(self):
            self.cancelled = True
            return True

    class _FakePool:
        def __init__(self, max_workers: int):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, arg):
            return _FakeFuture(fn(arg))

    def _fake_as_completed(futures):
        # return in failure-first order so one stage remains pending
        ordered = list(futures)
        return [ordered[1], ordered[0], ordered[2]]

    monkeypatch.setattr(engine.concurrent.futures, "ThreadPoolExecutor", _FakePool)
    monkeypatch.setattr(engine.concurrent.futures, "as_completed", _fake_as_completed)

    def _worker(args):
        stage_instance_id, stage_name, _script, _base_args, _run_id = args
        ok = stage_name != "stage_2"
        return stage_instance_id, stage_name, ok, 0.01, {"cache_enabled": False}

    stages = [
        ("s1", "stage_1", Path("a.py"), []),
        ("s2", "stage_2", Path("b.py"), []),
        ("s3", "stage_3", Path("c.py"), []),
    ]
    all_ok, timings = engine.run_stages_parallel(stages, "run_d", 3, worker_fn=_worker)
    assert all_ok is False
    assert any("s3" in batch for batch in terminated)
    assert len(timings) >= 1


def test_partition_items_preserves_order_within_partition():
    items = [
        {"symbol": "ETHUSDT", "value": 1},
        {"symbol": "BTCUSDT", "value": 2},
        {"symbol": "ETHUSDT", "value": 3},
    ]
    out = engine.partition_items(items, key_fn=lambda row: row["symbol"])
    assert list(out.keys()) == ["ETHUSDT", "BTCUSDT"]
    assert [row["value"] for row in out["ETHUSDT"]] == [1, 3]


def test_partition_map_reduce_is_deterministic_across_worker_counts():
    partitions = {
        "b": [3, 4],
        "a": [1, 2],
    }

    def _map_fn(key: str, values):
        return {"key": key, "sum": int(sum(values))}

    def _reduce_fn(rows):
        return [f"{row['key']}={row['sum']}" for row in rows]

    reduced_seq, results_seq = engine.run_partition_map_reduce(
        partitions,
        map_fn=_map_fn,
        reduce_fn=_reduce_fn,
        max_workers=1,
    )
    reduced_par, results_par = engine.run_partition_map_reduce(
        partitions,
        map_fn=_map_fn,
        reduce_fn=_reduce_fn,
        max_workers=4,
    )
    assert reduced_seq == ["a=3", "b=7"]
    assert reduced_par == reduced_seq
    assert results_seq["a"]["sum"] == results_par["a"]["sum"] == 3


def test_run_stage_requires_manifest_when_enabled(monkeypatch, tmp_path):
    run_id = "manifest_required"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    class _FakeProc:
        def __init__(self, stdout_handle):
            self.returncode = 0
            self._stdout_handle = stdout_handle
            self.pid = 777

        def wait(self, timeout=None):
            self._stdout_handle.write(b"ok\n")
            self._stdout_handle.flush()
            return self.returncode

        def poll(self):
            return self.returncode

    monkeypatch.setenv("BACKTEST_REQUIRE_STAGE_MANIFEST", "1")
    monkeypatch.setattr(
        engine.subprocess, "Popen", lambda *_a, **kwargs: _FakeProc(kwargs["stdout"])
    )
    stage_cache_meta: dict[str, dict[str, object]] = {}
    ok = engine.run_stage(
        "build_features",
        script_path,
        ["--symbols", "BTCUSDT"],
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id="build_features",
        stage_cache_meta=stage_cache_meta,
    )
    assert ok is False
    manifest_path = data_root / "runs" / run_id / "build_features.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"


def test_run_stage_cache_hit_rejected_when_declared_output_missing(monkeypatch, tmp_path):
    run_id = "cache_rejection_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")
    stage = "phase2_conditional_hypotheses"
    base_args = ["--event_type", "VOL_SHOCK"]
    stage_instance = engine.stage_instance_base(stage, base_args)

    cache_context = {
        "feature_schema_version": "v2",
        "pipeline_session_id": "",
        "require_stage_manifest": False,
    }
    input_hash = engine.compute_stage_input_hash(
        script_path, base_args, run_id, cache_context=cache_context
    )
    manifest_path = data_root / "runs" / run_id / f"{stage_instance}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    # Declare an output that DOES NOT exist
    missing_path = tmp_path / "never_created.parquet"
    manifest_path.write_text(
        json.dumps(
            {
                "status": "success",
                "input_hash": input_hash,
                "outputs": [{"path": str(missing_path)}],
                "run_id": run_id,
                "stage": stage,
                "stage_instance_id": stage_instance,
                "started_at": "2024-01-01T00:00:00Z",
                "finished_at": "2024-01-01T00:00:01Z",
                "parameters": {},
                "inputs": [],
                "spec_hashes": {},
                "ontology_spec_hash": "sha256:abc",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("BACKTEST_STAGE_CACHE", "1")

    # If the cache hit is rejected, it WILL try to call Popen
    popen_called = []

    def _fake_popen(*_args, **_kwargs):
        popen_called.append(True)

        class _FakeProc:
            def __init__(self, stdout_handle):
                self.returncode = 0
                self._stdout_handle = stdout_handle
                self.pid = 123

            def wait(self, timeout=None):
                self._stdout_handle.write(b"ok")
                self._stdout_handle.flush()
                return 0

            def poll(self):
                return 0

        return _FakeProc(_kwargs["stdout"])

    monkeypatch.setattr(engine.subprocess, "Popen", _fake_popen)
    stage_cache_meta: dict[str, dict[str, object]] = {}

    ok = engine.run_stage(
        stage,
        script_path,
        base_args,
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id=None,
        stage_cache_meta=stage_cache_meta,
    )
    assert ok is False
    assert len(popen_called) == 1
    assert stage_cache_meta[stage_instance]["cache_hit"] is False
    assert stage_cache_meta[stage_instance]["cache_reason"] == "miss_or_invalid_manifest_or_outputs"


def test_run_stage_does_not_block_on_descendant_inheriting_stdout(tmp_path):
    run_id = "descendant_stdout_run"
    data_root = tmp_path / "data"
    script_path = tmp_path / "stage_script.py"
    script_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "import subprocess",
                "import sys",
                "",
                "subprocess.Popen(",
                "    [sys.executable, '-c', 'import time; print(\"child-alive\"); time.sleep(10)']",
                ")",
                "print('parent-finished')",
                "sys.exit(0)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    stage_cache_meta: dict[str, dict[str, object]] = {}
    started = time.perf_counter()
    ok = engine.run_stage(
        "build_features",
        script_path,
        ["--symbols", "BTCUSDT"],
        run_id,
        data_root=data_root,
        strict_recommendations_checklist=False,
        feature_schema_version="v2",
        current_pipeline_session_id=None,
        current_stage_instance_id="build_features_descendant_stdout",
        stage_cache_meta=stage_cache_meta,
    )
    elapsed = time.perf_counter() - started

    assert ok is True
    # The descendant sleeps long enough that genuine stdout-inheritance
    # blocking would push this call well past the launcher path. Shared CI load
    # can still make subprocess startup itself slow, so keep the cutoff focused
    # on real descendant blocking rather than machine-speed jitter.
    assert elapsed < 6.0
    log_path = data_root / "runs" / run_id / "build_features_descendant_stdout.log"
    assert "parent-finished" in log_path.read_text(encoding="utf-8")


def test_validate_stage_manifest_on_disk_propagates_unexpected_runtime_errors(
    monkeypatch, tmp_path
):
    manifest_path = tmp_path / "stage.json"
    manifest_path.write_text("{}", encoding="utf-8")

    def _boom(_text):
        raise RuntimeError("unexpected parse failure")

    monkeypatch.setattr(engine.json, "loads", _boom)

    with pytest.raises(RuntimeError, match="unexpected parse failure"):
        engine._validate_stage_manifest_on_disk(manifest_path, allow_failed_minimal=False)


def test_terminate_stage_process_does_not_swallow_unexpected_runtime_errors(monkeypatch):
    class _FakeProc:
        pid = 123

        def poll(self):
            return None

        def wait(self, timeout=None):
            return 0

    if engine.os.name == "posix":
        monkeypatch.setattr(
            engine.os,
            "killpg",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("unexpected terminate failure")
            ),
        )
    else:
        monkeypatch.setattr(
            _FakeProc,
            "terminate",
            lambda self: (_ for _ in ()).throw(RuntimeError("unexpected terminate failure")),
        )

    with pytest.raises(RuntimeError, match="unexpected terminate failure"):
        engine._terminate_stage_process("run_a", "stage_a", _FakeProc())
