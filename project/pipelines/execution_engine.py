from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from json import JSONDecodeError
from pathlib import Path

from project import PROJECT_ROOT
from project.pipelines.execution_engine_support import (
    _allow_synthesized_manifest,
    _filter_unsupported_flags,
    _manifest_declared_outputs_exist,
    _required_stage_manifest_enabled,
    _script_supports_log_path,
    _synthesize_stage_manifest_if_missing,
    _validate_stage_manifest_on_disk,
    compute_stage_input_hash,
    is_phase2_stage,
    stage_instance_base,
)
from project.pipelines.pipeline_defaults import DATA_ROOT
from project.pipelines.planner import StageDefinition

StageLaunch = tuple[str, str, Path, list[str]]
WorkerArgs = tuple[str, str, Path, list[str], str]
WorkerResult = tuple[str, str, bool, float, dict[str, object]]
StageTiming = tuple[str, str, float, dict[str, object]]
PartitionMapFn = Callable[[str, Sequence[object]], object]
PartitionReduceFn = Callable[[list[object]], object]

_RUNNING_STAGE_PROCS: dict[tuple[str, str], subprocess.Popen[str]] = {}
_RUNNING_STAGE_PROCS_LOCK = threading.Lock()
_STAGE_OUTPUT_LOCK = threading.Lock()


def _register_running_stage_proc(
    run_id: str, stage_instance_id: str, proc: subprocess.Popen[str]
) -> None:
    with _RUNNING_STAGE_PROCS_LOCK:
        _RUNNING_STAGE_PROCS[(run_id, stage_instance_id)] = proc


def _unregister_running_stage_proc(run_id: str, stage_instance_id: str) -> None:
    with _RUNNING_STAGE_PROCS_LOCK:
        _RUNNING_STAGE_PROCS.pop((run_id, stage_instance_id), None)


def _terminate_stage_process(
    run_id: str, stage_instance_id: str, proc: subprocess.Popen[str], grace_sec: float = 5.0
) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except (OSError, ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=max(0.1, float(grace_sec)))
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except (OSError, ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=max(0.1, float(grace_sec)))
    except subprocess.TimeoutExpired:
        pass


def terminate_stage_instances(run_id: str, stage_instance_ids: Sequence[str]) -> None:
    with _RUNNING_STAGE_PROCS_LOCK:
        proc_items = [
            (stage_instance_id, _RUNNING_STAGE_PROCS.get((run_id, stage_instance_id)))
            for stage_instance_id in stage_instance_ids
        ]
    for stage_instance_id, proc in proc_items:
        if proc is None:
            continue
        _terminate_stage_process(run_id, stage_instance_id, proc)


def _emit_buffered_stage_output(stage_instance_id: str, stage: str, text: str) -> None:
    payload = str(text or "").rstrip()
    if not payload:
        return
    prefix = f"[{stage_instance_id}]"
    lines = [f"{prefix} buffered output ({stage})"]
    lines.extend(f"{prefix} {line}" for line in payload.splitlines())
    message = "\n".join(lines) + "\n"
    with _STAGE_OUTPUT_LOCK:
        sys.stdout.write(message)
        sys.stdout.flush()


def _read_log_delta(log_path: Path, start_offset: int) -> str:
    if not log_path.exists():
        return ""
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(max(0, int(start_offset)))
        return handle.read()


def run_stage(
    stage: str,
    script_path: Path,
    base_args: list[str],
    run_id: str,
    *,
    data_root: Path,
    strict_recommendations_checklist: bool,
    feature_schema_version: str,
    current_pipeline_session_id: str | None,
    current_stage_instance_id: str | None,
    stage_cache_meta: dict[str, dict[str, object]],
    max_attempts: int = 1,
    retry_backoff_sec: float = 0.0,
) -> bool:
    runs_dir = data_root / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    stage_instance_id = current_stage_instance_id or stage_instance_base(stage, base_args)
    log_path = runs_dir / f"{stage_instance_id}.log"
    manifest_path = runs_dir / f"{stage_instance_id}.json"

    # Stage output caching: skip if manifest exists with matching input_hash.
    # Enable globally with BACKTEST_STAGE_CACHE=1, or for phase2 only with
    # BACKTEST_EVENT_STAGE_CACHE=1.
    stage_cache_enabled = bool(int(os.environ.get("BACKTEST_STAGE_CACHE", "0")))
    event_stage_cache_enabled = bool(
        int(os.environ.get("BACKTEST_EVENT_STAGE_CACHE", "0"))
    ) and is_phase2_stage(stage)
    cache_enabled = stage_cache_enabled or event_stage_cache_enabled
    cache_context = {
        "feature_schema_version": str(feature_schema_version or ""),
        "pipeline_session_id": str(current_pipeline_session_id or ""),
        "require_stage_manifest": _required_stage_manifest_enabled(),
    }
    if cache_enabled:
        input_hash = compute_stage_input_hash(
            script_path,
            base_args,
            run_id,
            cache_context=cache_context,
        )
        if manifest_path.exists():
            try:
                cached = json.loads(manifest_path.read_text(encoding="utf-8"))
                outputs_ok = _manifest_declared_outputs_exist(manifest_path, cached)
                stats = cached.get("stats", {}) if isinstance(cached.get("stats", {}), dict) else {}
                is_synthesized = bool(stats.get("synthesized_manifest", False))
                if (
                    cached.get("input_hash") == input_hash
                    and cached.get("status") == "success"
                    and outputs_ok
                    and not is_synthesized
                ):
                    print(f"[CACHE HIT] {stage} (input_hash={input_hash}) — skipping.")
                    stage_cache_meta[stage_instance_id] = {
                        "cache_enabled": True,
                        "cache_scope": "global" if stage_cache_enabled else "phase2_only",
                        "cache_key": input_hash,
                        "cache_hit": True,
                        "cache_reason": "input_hash_match",
                    }
                    return True
            except (
                OSError,
                UnicodeDecodeError,
                JSONDecodeError,
                AttributeError,
                TypeError,
                ValueError,
            ):
                pass
        stage_cache_meta[stage_instance_id] = {
            "cache_enabled": True,
            "cache_scope": "global" if stage_cache_enabled else "phase2_only",
            "cache_key": input_hash,
            "cache_hit": False,
            "cache_reason": "miss_or_invalid_manifest_or_outputs",
        }
    else:
        input_hash = None
        stage_cache_meta[stage_instance_id] = {
            "cache_enabled": False,
            "cache_scope": "disabled",
            "cache_key": None,
            "cache_hit": False,
            "cache_reason": "disabled",
        }

    filtered_args = _filter_unsupported_flags(script_path, base_args)
    cmd = [sys.executable, str(script_path)] + filtered_args
    if _script_supports_log_path(script_path):
        cmd.extend(["--log_path", str(log_path)])
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT.parent) + os.pathsep + env.get("PYTHONPATH", "")
    env["BACKTEST_RUN_ID"] = run_id
    env["BACKTEST_STAGE_INSTANCE_ID"] = stage_instance_id
    env["BACKTEST_FEATURE_SCHEMA_VERSION"] = feature_schema_version
    env["BACKTEST_STAGE_STDOUT_CAPTURED"] = "1"
    if current_pipeline_session_id:
        env["BACKTEST_PIPELINE_SESSION_ID"] = current_pipeline_session_id

    allowed_nonzero = {}
    if not strict_recommendations_checklist:
        allowed_nonzero["generate_recommendations_checklist"] = {1}
    if stage.startswith("bridge_evaluate_phase2"):
        allowed_nonzero[stage] = {1}
    # promote_candidates exits 1 when validation bundle is missing (graceful skip)
    if stage == "promote_candidates":
        allowed_nonzero[stage] = {1}
    accepted_codes = {0} | allowed_nonzero.get(stage, set())
    attempts = max(1, int(max_attempts))
    backoff_sec = max(0.0, float(retry_backoff_sec))

    # Run the stage script with per-stage output buffering.
    # This keeps parallel logs readable and emits each stage block atomically.
    result_returncode: int | None = None
    for attempt in range(1, attempts + 1):
        log_start_offset = log_path.stat().st_size if log_path.exists() else 0
        popen_kwargs: dict[str, object] = {
            "env": env,
            "stderr": subprocess.STDOUT,
        }
        if os.name == "posix":
            # New process-group allows fail-fast cancellation to terminate
            # every subprocess spawned by a stage worker.
            popen_kwargs["start_new_session"] = True
        elif os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        with log_path.open("ab") as stage_log:
            popen_kwargs["stdout"] = stage_log
            proc = subprocess.Popen(cmd, **popen_kwargs)  # type: ignore[arg-type]
            _register_running_stage_proc(run_id, stage_instance_id, proc)
            try:
                result_returncode = int(proc.wait())
                stage_log.flush()
            finally:
                _unregister_running_stage_proc(run_id, stage_instance_id)

        stage_output = _read_log_delta(log_path, log_start_offset)
        if stage_output:
            _emit_buffered_stage_output(stage_instance_id, stage, stage_output)

        if result_returncode not in accepted_codes:
            print(
                f"\nERROR: Stage {stage} failed with exit code {result_returncode}",
                file=sys.stderr,
            )

        if result_returncode in accepted_codes:
            require_manifest = _required_stage_manifest_enabled()
            allow_synth = _allow_synthesized_manifest()
            if not manifest_path.exists():
                if require_manifest:
                    error = (
                        "stage manifest missing on success while BACKTEST_REQUIRE_STAGE_MANIFEST=1"
                    )
                    _synthesize_stage_manifest_if_missing(
                        manifest_path=manifest_path,
                        stage=stage,
                        stage_instance_id=stage_instance_id,
                        run_id=run_id,
                        script_path=script_path,
                        base_args=base_args,
                        log_path=log_path,
                        status="failed",
                        error=error,
                        input_hash=input_hash,
                    )
                    print(f"Stage failed: {stage} ({error})", file=sys.stderr)
                    return False

                # Best-effort stages historically succeeded without an emitted
                # manifest. Keep that fast path when caching is irrelevant and
                # no explicit synthesized-manifest mode is requested.
                if not (cache_enabled or allow_synth):
                    return True

                _synthesize_stage_manifest_if_missing(
                    manifest_path=manifest_path,
                    stage=stage,
                    stage_instance_id=stage_instance_id,
                    run_id=run_id,
                    script_path=script_path,
                    base_args=base_args,
                    log_path=log_path,
                    status="success",
                    input_hash=input_hash,
                )
            valid_manifest, validation_error = _validate_stage_manifest_on_disk(
                manifest_path, allow_failed_minimal=False
            )
            if not valid_manifest:
                print(validation_error, file=sys.stderr)
                if require_manifest:
                    _synthesize_stage_manifest_if_missing(
                        manifest_path=manifest_path,
                        stage=stage,
                        stage_instance_id=stage_instance_id,
                        run_id=run_id,
                        script_path=script_path,
                        base_args=base_args,
                        log_path=log_path,
                        status="failed",
                        error=validation_error,
                        input_hash=input_hash,
                    )
                return False
            manifest_payload: dict[str, object] | None = None
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    manifest_payload = payload
            except (OSError, UnicodeDecodeError, JSONDecodeError, TypeError, ValueError) as exc:
                print(f"[WARN] Failed to load manifest {manifest_path}: {exc}", file=sys.stderr)
            if manifest_payload is not None and str(manifest_payload.get("status", "")).strip().lower() == "success":
                outputs_ok = _manifest_declared_outputs_exist(manifest_path, manifest_payload)
                if not outputs_ok:
                    error = (
                        f"stage manifest declared outputs missing or empty ({manifest_path})"
                    )
                    print(error, file=sys.stderr)
                    if require_manifest:
                        _synthesize_stage_manifest_if_missing(
                            manifest_path=manifest_path,
                            stage=stage,
                            stage_instance_id=stage_instance_id,
                            run_id=run_id,
                            script_path=script_path,
                            base_args=base_args,
                            log_path=log_path,
                            status="failed",
                            error=error,
                            input_hash=input_hash,
                        )
                    return False
            # Stamp input_hash into stage manifest on success for future cache reads.
            if input_hash and manifest_path.exists():
                try:
                    mdata = json.loads(manifest_path.read_text(encoding="utf-8"))
                    mdata["input_hash"] = input_hash
                    manifest_path.write_text(json.dumps(mdata, indent=2), encoding="utf-8")
                    if isinstance(mdata, dict):
                        manifest_payload = mdata
                except (OSError, UnicodeDecodeError, JSONDecodeError, TypeError, ValueError) as exc:
                    print(
                        f"[WARN] Failed to stamp input_hash into {manifest_path}: {exc}",
                        file=sys.stderr,
                    )

            if manifest_payload is not None and str(
                os.environ.get("BACKTEST_EXPERIMENT_STORE", "0")
            ).strip() in {"1", "true", "TRUE"}:
                try:
                    from project.io.experiment_store import upsert_stage_manifest

                    upsert_stage_manifest(
                        data_root=data_root,
                        run_id=run_id,
                        stage_instance_id=stage_instance_id,
                        manifest_path=manifest_path,
                        payload=manifest_payload,
                    )
                except (ImportError, OSError, ValueError) as exc:
                    print(f"[WARN] Failed to write to experiment store: {exc}", file=sys.stderr)
            return True

        if attempt < attempts:
            print(
                f"[RETRY] Stage {stage} attempt {attempt}/{attempts} failed; retrying...",
                file=sys.stderr,
            )
            if backoff_sec > 0.0:
                time.sleep(backoff_sec)

    assert result_returncode is not None
    _synthesize_stage_manifest_if_missing(
        manifest_path=manifest_path,
        stage=stage,
        stage_instance_id=stage_instance_id,
        run_id=run_id,
        script_path=script_path,
        base_args=base_args,
        log_path=log_path,
        status="failed",
        error=f"exit_code={result_returncode}",
        input_hash=input_hash,
    )
    print(f"Stage failed: {stage}", file=sys.stderr)
    print(f"Stage log: {log_path}", file=sys.stderr)
    print(f"Stage manifest: {manifest_path}", file=sys.stderr)
    return False


def partition_items(
    items: Sequence[object],
    *,
    key_fn: Callable[[object], str],
) -> dict[str, list[object]]:
    """
    Deterministically partition items by key while preserving input order
    within each partition.
    """
    out: dict[str, list[object]] = {}
    for item in items:
        key = str(key_fn(item))
        out.setdefault(key, []).append(item)
    return out


def run_partition_map_reduce(
    partitions: dict[str, Sequence[object]],
    *,
    map_fn: PartitionMapFn,
    reduce_fn: PartitionReduceFn,
    max_workers: int = 1,
) -> tuple[object, dict[str, object]]:
    """
    Execute a deterministic map-reduce over partitioned artifacts.

    - Map execution may run in parallel.
    - Reduce input order is stable (sorted by partition key).
    """
    results: dict[str, object] = {}
    keys = sorted(str(key) for key in partitions)
    workers = max(1, min(int(max_workers), max(1, len(keys))))
    if workers <= 1:
        for key in keys:
            results[key] = map_fn(key, list(partitions.get(key, ())))
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(map_fn, key, list(partitions.get(key, ()))): key for key in keys}
            for fut in concurrent.futures.as_completed(futures):
                key = futures[fut]
                results[key] = fut.result()
    ordered_results = [results[key] for key in sorted(results.keys())]
    reduced = reduce_fn(ordered_results)
    return reduced, results


def run_stages_parallel(
    stages: Sequence[StageLaunch],
    run_id: str,
    max_workers: int,
    *,
    worker_fn: Callable[[WorkerArgs], WorkerResult] | None = None,
    continue_on_failure: bool = False,
) -> tuple[bool, list[StageTiming]]:
    """Run a batch of independent stages in parallel using subprocess workers.

    Returns (all_ok, [(stage_instance, stage_name, elapsed_sec, cache_meta), ...])
    in completion order.
    """
    timings: list[StageTiming] = []
    all_ok = True
    effective_workers = max(1, min(max_workers, len(stages)))
    if effective_workers <= 1:
        for stage_instance_id, stage_name, script, base_args in stages:
            stage_inst, stage_nm, ok, elapsed, cache_meta = worker_fn(
                (stage_instance_id, stage_name, script, base_args, run_id)
            )
            timings.append((stage_inst, stage_nm, elapsed, cache_meta))
            if not ok:
                all_ok = False
                if not continue_on_failure:
                    break
        return all_ok, timings

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
            args_list = [(s[0], s[1], s[2], s[3], run_id) for s in stages]
            futures = {pool.submit(worker_fn, a): a[0] for a in args_list}
            pending_stage_ids = set(futures.values())
            for fut in concurrent.futures.as_completed(futures):
                stage_instance_id, stage_name, ok, elapsed, cache_meta = fut.result()
                pending_stage_ids.discard(stage_instance_id)
                timings.append((stage_instance_id, stage_name, elapsed, cache_meta))
                if not ok:
                    all_ok = False
                    if not continue_on_failure:
                        # Cancel pending futures and terminate running stage subprocesses.
                        terminate_stage_instances(run_id, sorted(pending_stage_ids))
                        for remaining in futures:
                            remaining.cancel()
                        break
    except (PermissionError, OSError) as exc:
        print(
            f"[WARN] ThreadPool unavailable ({exc}). Falling back to sequential stage execution.",
            file=sys.stderr,
        )
        timings = []
        all_ok = True
        for stage_instance_id, stage_name, script, base_args in stages:
            stage_inst, stage_nm, ok, elapsed, cache_meta = worker_fn(
                (stage_instance_id, stage_name, script, base_args, run_id)
            )
            timings.append((stage_inst, stage_nm, elapsed, cache_meta))
            if not ok:
                all_ok = False
                if not continue_on_failure:
                    break
    return all_ok, timings


def run_dag(
    plan: Mapping[str, StageDefinition],
    run_id: str,
    max_workers: int,
    *,
    worker_fn: Callable[[WorkerArgs], WorkerResult] | None = None,
    completed_already: set[str] | None = None,
    continue_on_failure: bool = False,
) -> tuple[bool, list[StageTiming]]:
    """
    Execute a pipeline DAG in parallel.

    Returns (all_ok, timings)
    """
    timings: list[StageTiming] = []
    completed = set(completed_already or [])
    failed: set[str] = set()
    running: dict[concurrent.futures.Future, str] = {}

    all_ok = True

    # Task execution router
    def _execute_task_or_subprocess(
        stage_name: str, script: str | Path, args: list[str], rid: str
    ) -> bool:
        path_str = str(script)
        if ":" in path_str or (path_str.startswith("project.") and not path_str.endswith(".py")):
            import importlib

            try:
                mod_name, func_name = (
                    path_str.rsplit(":", 1) if ":" in path_str else (path_str, "run_task")
                )
                mod = importlib.import_module(mod_name)
                func = getattr(mod, func_name)
                return func(rid, args) == 0
            except (ImportError, AttributeError, TypeError, ValueError) as e:
                print(f"[DAG] Task {stage_name} failed: {e}")
                return False

        # Fallback to subprocess using the execution engine directly.
        return run_stage(
            stage=stage_name,
            script_path=Path(path_str),
            base_args=args,
            run_id=rid,
            data_root=DATA_ROOT,
            strict_recommendations_checklist=False,
            feature_schema_version=str(
                os.environ.get("BACKTEST_FEATURE_SCHEMA_VERSION", "v2") or "v2"
            ),
            current_pipeline_session_id=str(
                os.environ.get("BACKTEST_PIPELINE_SESSION_ID", "")
            ).strip()
            or None,
            current_stage_instance_id=stage_name,
            stage_cache_meta={},
        )

    if worker_fn is None:

        def default_worker(args_tuple: WorkerArgs) -> WorkerResult:
            inst_id, name, script, args, rid = args_tuple
            start_ts = time.time()
            ok = _execute_task_or_subprocess(name, script, args, rid)
            return inst_id, name, ok, time.time() - start_ts, {}

        worker_fn = default_worker

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        while len(completed) + len(failed) < len(plan):
            # 1. Find ready stages
            ready = []
            for name, stage in plan.items():
                if name in completed or name in failed:
                    continue

                # Check if already running (by stage name)
                if any(stage_name == name for stage_name in running.values()):
                    continue

                # Check dependencies
                if all(dep in completed for dep in stage.depends_on):
                    ready.append(stage)

            # 2. Launch ready stages
            for stage in ready:
                if len(running) >= max_workers:
                    break
                print(f"[DAG] Launching {stage.name} (deps={stage.depends_on})")

                # Use a unique key for the future to stage name mapping
                fut = pool.submit(
                    worker_fn, (stage.name, stage.name, stage.script_path, stage.args, run_id)
                )
                running[fut] = stage.name

            if not running:
                if len(completed) + len(failed) < len(plan):
                    unmet = [n for n in plan if n not in completed and n not in failed]
                    print(f"ERROR: DAG deadlock detected. Unmet stages: {unmet}", file=sys.stderr)
                    return False, timings
                break

            # 3. Wait for progress
            done, _ = concurrent.futures.wait(
                running.keys(), return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                stage_instance_id = running.pop(fut)
                try:
                    # Result tuple: (instance_id, name, ok, elapsed, cache_meta)
                    res_inst, res_name, ok, elapsed, cache_meta = fut.result()
                    timings.append((res_inst, res_name, elapsed, cache_meta))
                    if ok:
                        completed.add(res_inst)
                    else:
                        print(f"[DAG] Stage {res_inst} failed.")
                        failed.add(res_inst)
                        all_ok = False
                        if not continue_on_failure:
                            terminate_stage_instances(run_id, list(running.values()))
                            return False, timings
                except (
                    concurrent.futures.CancelledError,
                    concurrent.futures.TimeoutError,
                    RuntimeError,
                    ValueError,
                    TypeError,
                ) as e:
                    print(f"[DAG] Execution error in {stage_instance_id}: {e}")
                    failed.add(stage_instance_id)
                    all_ok = False
                    if not continue_on_failure:
                        return False, timings

    return all_ok, timings
