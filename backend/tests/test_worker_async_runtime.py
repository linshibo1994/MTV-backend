"""Celery 同步任务包装器的异步运行时回归测试。"""

from __future__ import annotations

import ast
import asyncio
import os
import signal
import threading
import time
from concurrent.futures import CancelledError as ConcurrentCancelledError
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError
from celery.exceptions import WorkerTerminate
from celery.worker import request as request_module
from celery.worker.request import Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.workers import async_runtime as async_runtime_module
from app.workers import media_storage, outbox, pipeline, video_generation
from app.workers.async_runtime import (
    WorkerAsyncRuntime,
    discard_inherited_database_pool,
)


class _ExpectedFailure(RuntimeError):
    pass


class _SynchronousInterrupt(BaseException):
    pass


def test_consecutive_outbox_and_pipeline_tasks_share_one_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """连接池资源不得在连续 Celery 任务之间被切换到新事件循环。"""

    pooled_waiter: asyncio.Future[str] | None = None
    loop_ids: list[int] = []

    async def _dispatch() -> int:
        nonlocal pooled_waiter
        loop = asyncio.get_running_loop()
        loop_ids.append(id(loop))
        # 模拟 asyncpg 连接归还连接池时仍持有的 loop 绑定 Future。
        pooled_waiter = loop.create_future()
        return 1

    async def _run_stage_async(*args: object, **kwargs: object) -> str:
        assert pooled_waiter is not None
        loop = asyncio.get_running_loop()
        loop_ids.append(id(loop))
        loop.call_soon(pooled_waiter.set_result, "ready")
        await pooled_waiter
        return "script-1"

    monkeypatch.setattr(outbox, "_dispatch", _dispatch)
    monkeypatch.setattr(pipeline, "_run_stage_async", _run_stage_async)

    assert outbox.dispatch_outbox_task.run() == 1
    assert (
        pipeline._run_stage(
            "split",
            "script-1",
            "episode-1",
            ("episode-1",),
            1,
            None,
            0,
            "dispatch-1",
            "run-1",
        )
        == "script-1"
    )
    assert len(set(loop_ids)) == 1


def test_runtime_cleans_child_tasks_before_propagating_failure() -> None:
    """顶层协程失败后不得让其 heartbeat 等子任务继续污染下一任务。"""

    runtime = WorkerAsyncRuntime()
    child_cleaned = threading.Event()

    async def _fail_with_child() -> None:
        async def _child() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                child_cleaned.set()

        asyncio.create_task(_child())
        await asyncio.sleep(0)
        raise _ExpectedFailure("expected")

    async def _next_task() -> str:
        return "next"

    try:
        with pytest.raises(_ExpectedFailure, match="expected"):
            runtime.run(_fail_with_child())
        assert child_cleaned.is_set()
        assert runtime.run(_next_task()) == "next"
    finally:
        runtime.shutdown()


def test_stubborn_provider_close_is_detached_and_poisons_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.providers import video_gen
    from app.providers.video_gen import (
        VideoGenerationRemoteTerminalError,
        managed_video_client,
    )
    from app.workers.async_runtime import WorkerAsyncRuntimeStopping

    runtime = WorkerAsyncRuntime()
    primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="runtime-stubborn-close",
        terminal_status="failed",
    )
    close_cancelled = threading.Event()
    close_finished = threading.Event()
    release_close = threading.Event()

    async def stubborn_close() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            close_cancelled.set()
            current = asyncio.current_task()
            assert current is not None
            while current.cancelling():
                current.uncancel()
            while not release_close.is_set():
                await asyncio.sleep(0.005)
        finally:
            close_finished.set()

    async def fail_during_provider_call() -> None:
        async with managed_video_client(
            object(),
            stubborn_close,
            provider_key="openai",
        ):
            raise primary_error

    async def next_invocation() -> None:
        return None

    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
    release_timer = threading.Timer(0.15, release_close.set)
    release_timer.start()
    started_at = time.monotonic()
    try:
        with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
            runtime.run(fail_during_provider_call())
        elapsed = time.monotonic() - started_at

        assert exc_info.value is primary_error
        assert close_cancelled.is_set()
        assert elapsed < 0.08
        assert runtime._stopping is True
        with pytest.raises(WorkerAsyncRuntimeStopping):
            runtime.run(next_invocation())
    finally:
        release_close.set()
        assert close_finished.wait(timeout=1)
        release_timer.cancel()
        release_timer.join(timeout=1)
        runtime.shutdown(timeout=1)


def test_shutdown_abandons_permanently_stubborn_detached_provider_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.providers import video_gen
    from app.providers.video_gen import (
        VideoGenerationRemoteTerminalError,
        managed_video_client,
    )

    runtime = WorkerAsyncRuntime()
    primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="runtime-permanent-stubborn-close",
        terminal_status="failed",
    )
    close_cancelled = threading.Event()
    release_close = threading.Event()

    async def permanently_stubborn_close() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            close_cancelled.set()
            current = asyncio.current_task()
            assert current is not None
            while not release_close.is_set():
                while current.cancelling():
                    current.uncancel()
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    continue

    async def fail_during_provider_call() -> None:
        async with managed_video_client(
            object(),
            permanently_stubborn_close,
            provider_key="openai",
        ):
            raise primary_error

    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
    runtime_thread: threading.Thread | None = None
    try:
        with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
            runtime.run(fail_during_provider_call())
        assert exc_info.value is primary_error
        assert close_cancelled.is_set()
        assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1

        runtime_thread = runtime._thread
        assert runtime_thread is not None
        started_at = time.monotonic()
        runtime.shutdown(timeout=0.05)
        elapsed = time.monotonic() - started_at

        assert elapsed < 0.15
        assert not runtime_thread.is_alive()
        assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()
    finally:
        release_close.set()
        if runtime_thread is not None:
            runtime_thread.join(timeout=1)
        runtime.shutdown(timeout=0.1)


def test_volcengine_stubborn_close_does_not_block_runtime_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.providers import video_gen
    from app.providers.video_gen import VideoCancellationOutcome
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    runtime = WorkerAsyncRuntime()
    close_cancelled = threading.Event()
    release_close = threading.Event()
    caller_errors: list[BaseException] = []
    outcomes: list[VideoCancellationOutcome] = []

    class Tasks:
        async def delete(self, *, task_id: str) -> None:
            assert task_id == "volcengine-runtime-stubborn-close"

    class Client:
        content_generation = SimpleNamespace(tasks=Tasks())

        async def close(self) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                current = asyncio.current_task()
                assert current is not None
                while not release_close.is_set():
                    while current.cancelling():
                        current.uncancel()
                    try:
                        await asyncio.sleep(0.005)
                    except asyncio.CancelledError:
                        continue

    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: Client())
    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)

    def invoke() -> None:
        try:
            result = runtime.run(provider.cancel("volcengine-runtime-stubborn-close"))
            outcomes.append(result.outcome)
        except BaseException as exc:
            caller_errors.append(exc)

    caller = threading.Thread(target=invoke, daemon=True)
    caller.start()
    assert close_cancelled.wait(timeout=2)
    runtime_thread = runtime._thread
    assert runtime_thread is not None

    try:
        started_at = time.monotonic()
        runtime.shutdown(timeout=0.05)
        elapsed = time.monotonic() - started_at

        assert elapsed < 0.15
        assert not runtime_thread.is_alive()
        caller.join(timeout=1)
        assert not caller.is_alive()
        assert caller_errors == []
        assert outcomes == [VideoCancellationOutcome.CANCELED]
        assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()
    finally:
        release_close.set()
        caller.join(timeout=1)
        runtime_thread.join(timeout=1)
        runtime.shutdown(timeout=0.1)


@pytest.mark.skipif(
    not hasattr(signal, "setitimer"), reason="当前平台不支持同步计时信号"
)
def test_sync_interrupt_waits_for_top_and_child_cleanup() -> None:
    """模拟 Celery 软超时：返回前必须等待顶层和子任务完成 finally。"""

    runtime = WorkerAsyncRuntime()
    top_cleanup_started = threading.Event()
    top_cleanup_done = threading.Event()
    child_cleanup_done = threading.Event()

    async def _long_running() -> None:
        async def _child() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                child_cleanup_done.set()

        asyncio.create_task(_child())
        try:
            await asyncio.Event().wait()
        finally:
            top_cleanup_started.set()
            await asyncio.sleep(0.05)
            top_cleanup_done.set()

    async def _assert_previous_cleanup_finished() -> None:
        assert top_cleanup_done.is_set()
        assert child_cleanup_done.is_set()

    def _interrupt(_signum: int, _frame: object) -> None:
        raise _SynchronousInterrupt("soft timeout")

    previous_handler = signal.signal(signal.SIGALRM, _interrupt)
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.02)
        with pytest.raises(_SynchronousInterrupt, match="soft timeout"):
            runtime.run(_long_running())
        assert top_cleanup_started.is_set()
        assert top_cleanup_done.is_set()
        assert child_cleanup_done.is_set()
        runtime.run(_assert_previous_cleanup_finished())
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        runtime.shutdown()


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"), reason="当前平台不支持同步计时信号"
)
def test_sync_interrupt_hook_runs_before_cleanup_and_uses_bounded_grace() -> None:
    runtime = WorkerAsyncRuntime()
    invocation_started = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    hook_observations: list[bool] = []

    async def _stubborn_cleanup() -> None:
        invocation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            current = asyncio.current_task()
            assert current is not None
            while current.cancelling():
                current.uncancel()
            while not release_cleanup.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    while current.cancelling():
                        current.uncancel()

    def _interrupt_hook(_interrupt: BaseException) -> None:
        hook_observations.append(cleanup_started.is_set())

    def _interrupt(_signum: int, _frame: object) -> None:
        raise SoftTimeLimitExceeded()

    def _send_interrupt() -> None:
        if invocation_started.wait(timeout=2):
            os.kill(os.getpid(), signal.SIGALRM)

    previous_handler = signal.signal(signal.SIGALRM, _interrupt)
    sender = threading.Thread(target=_send_interrupt, daemon=True)
    sender.start()
    started_at = time.monotonic()
    try:
        with pytest.raises(SoftTimeLimitExceeded):
            runtime.run(
                _stubborn_cleanup(),
                on_interrupt=_interrupt_hook,
                interrupt_grace_timeout=0.05,
            )
        elapsed = time.monotonic() - started_at
        assert elapsed < 0.3
        assert hook_observations == [False]
        assert cleanup_started.is_set()
        assert runtime._stopping is True
    finally:
        signal.signal(signal.SIGALRM, previous_handler)
        release_cleanup.set()
        sender.join(timeout=1)
        runtime.shutdown(timeout=1)


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"), reason="当前平台不支持同步计时信号"
)
def test_second_hard_signal_is_not_swallowed_while_waiting_for_cleanup() -> None:
    runtime = WorkerAsyncRuntime()
    invocation_started = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    signal_count = 0
    hard_error = TimeLimitExceeded()

    async def _stubborn_cleanup() -> None:
        invocation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            current = asyncio.current_task()
            assert current is not None
            while current.cancelling():
                current.uncancel()
            while not release_cleanup.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    while current.cancelling():
                        current.uncancel()

    def _interrupt(_signum: int, _frame: object) -> None:
        nonlocal signal_count
        signal_count += 1
        if signal_count == 1:
            raise SoftTimeLimitExceeded()
        raise hard_error

    def _send_interrupts() -> None:
        if not invocation_started.wait(timeout=2):
            return
        os.kill(os.getpid(), signal.SIGALRM)
        if cleanup_started.wait(timeout=2):
            time.sleep(0.02)
            os.kill(os.getpid(), signal.SIGALRM)

    previous_handler = signal.signal(signal.SIGALRM, _interrupt)
    sender = threading.Thread(target=_send_interrupts, daemon=True)
    sender.start()
    try:
        with pytest.raises(TimeLimitExceeded) as exc_info:
            runtime.run(
                _stubborn_cleanup(),
                interrupt_grace_timeout=1.0,
            )
        assert exc_info.value is hard_error
        assert signal_count == 2
    finally:
        signal.signal(signal.SIGALRM, previous_handler)
        release_cleanup.set()
        sender.join(timeout=1)
        runtime.shutdown(timeout=1)


def test_runtime_supports_concurrent_thread_callers_on_one_loop() -> None:
    """Celery threads 池可并发提交，但所有异步资源仍归属同一 loop。"""

    runtime = WorkerAsyncRuntime()
    loop_ids: set[int] = set()
    active = 0
    max_active = 0

    async def _work(value: int) -> int:
        nonlocal active, max_active
        loop_ids.add(id(asyncio.get_running_loop()))
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.03)
            return value
        finally:
            active -= 1

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(runtime.run, _work(index)) for index in range(4)]
            assert [future.result() for future in futures] == [0, 1, 2, 3]
        assert len(loop_ids) == 1
        assert max_active == 4
    finally:
        runtime.shutdown()


def test_shutdown_drains_invocations_before_disposing_database_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = WorkerAsyncRuntime()
    invocation_started = threading.Event()
    invocation_cleaned = threading.Event()
    dispose_observations: list[bool] = []
    caller_errors: list[BaseException] = []

    async def _running() -> None:
        invocation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            invocation_cleaned.set()

    class _Engine:
        async def dispose(self) -> None:
            dispose_observations.append(invocation_cleaned.is_set())

    def _invoke() -> None:
        try:
            runtime.run(_running())
        except BaseException as exc:
            caller_errors.append(exc)

    monkeypatch.setattr("app.core.database.engine", _Engine())
    caller = threading.Thread(target=_invoke, daemon=True)
    caller.start()
    assert invocation_started.wait(timeout=2)

    runtime.shutdown()
    caller.join(timeout=2)

    assert not caller.is_alive()
    assert len(caller_errors) == 1
    assert isinstance(caller_errors[0], ConcurrentCancelledError)
    assert dispose_observations == [True]


def test_shutdown_deadline_does_not_dispose_pool_before_stubborn_task_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = WorkerAsyncRuntime()
    invocation_started = threading.Event()
    release_cleanup = threading.Event()
    dispose_calls: list[bool] = []

    async def _stubborn_cleanup() -> None:
        invocation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            current = asyncio.current_task()
            assert current is not None
            while current.cancelling():
                current.uncancel()
            while not release_cleanup.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    while current.cancelling():
                        current.uncancel()

    class _Engine:
        async def dispose(self) -> None:
            dispose_calls.append(True)

    def _invoke() -> None:
        runtime.run(_stubborn_cleanup())

    monkeypatch.setattr("app.core.database.engine", _Engine())
    caller = threading.Thread(target=_invoke, daemon=True)
    caller.start()
    assert invocation_started.wait(timeout=2)
    runtime_thread = runtime._thread
    assert runtime_thread is not None
    release_timer = threading.Timer(0.15, release_cleanup.set)
    release_timer.start()

    started_at = time.monotonic()
    runtime.shutdown(timeout=0.03)
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.1
    assert dispose_calls == []
    release_timer.join(timeout=1)
    runtime_thread.join(timeout=1)
    caller.join(timeout=1)
    assert not runtime_thread.is_alive()
    assert not caller.is_alive()

    async def _must_not_restart() -> None:
        return None

    with pytest.raises(RuntimeError, match="runtime is stopping"):
        runtime.run(_must_not_restart())


def test_shutdown_during_loop_startup_does_not_leak_runtime_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = WorkerAsyncRuntime()
    startup_entered = threading.Event()
    release_startup = threading.Event()
    caller_errors: list[BaseException] = []
    original_new_event_loop = async_runtime_module.asyncio.new_event_loop

    def _blocked_new_event_loop() -> asyncio.AbstractEventLoop:
        startup_entered.set()
        assert release_startup.wait(timeout=2)
        return original_new_event_loop()

    async def _work() -> None:
        return None

    def _invoke() -> None:
        try:
            runtime.run(_work())
        except BaseException as exc:
            caller_errors.append(exc)

    monkeypatch.setattr(
        async_runtime_module.asyncio, "new_event_loop", _blocked_new_event_loop
    )
    caller = threading.Thread(target=_invoke, daemon=True)
    caller.start()
    assert startup_entered.wait(timeout=2)
    runtime_thread = runtime._thread
    assert runtime_thread is not None

    runtime.shutdown(timeout=0.03)
    release_startup.set()
    caller.join(timeout=1)
    runtime_thread.join(timeout=1)

    try:
        assert not caller.is_alive()
        assert not runtime_thread.is_alive()
        assert len(caller_errors) == 1
        assert isinstance(caller_errors[0], RuntimeError)
    finally:
        if runtime_thread.is_alive() and runtime._loop is not None:
            runtime._loop.call_soon_threadsafe(runtime._loop.stop)
            runtime_thread.join(timeout=1)


def test_shutdown_from_runtime_thread_does_not_poison_or_leak_runtime() -> None:
    runtime = WorkerAsyncRuntime()

    async def _invalid_shutdown() -> None:
        runtime.shutdown()

    async def _still_usable() -> str:
        return "ok"

    try:
        with pytest.raises(RuntimeError, match="cannot synchronously stop itself"):
            runtime.run(_invalid_shutdown())
        assert runtime.run(_still_usable()) == "ok"
    finally:
        runtime.shutdown()


def test_prefork_pool_discard_replaces_pool_without_closing_parent_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[bool] = []
    fake_engine = SimpleNamespace(
        sync_engine=SimpleNamespace(dispose=lambda *, close: calls.append(close))
    )
    monkeypatch.setattr("app.core.database.engine", fake_engine)

    discard_inherited_database_pool()

    assert calls == [False]


@pytest.mark.parametrize(
    ("worker_module", "task", "args", "kwargs"),
    [
        (video_generation, video_generation.run_video_generation, ("job-1",), {}),
        (
            video_generation,
            video_generation.reconcile_video_remote_cancellations_task,
            (),
            {"limit": 1},
        ),
        (
            video_generation,
            video_generation.reconcile_video_financial_cleanup_task,
            (),
            {"limit": 1},
        ),
        (video_generation, video_generation.recover_stale_jobs, (), {}),
        (
            media_storage,
            media_storage.recover_media_storage_jobs,
            (),
            {"limit": 1},
        ),
    ],
)
def test_worker_entrypoints_do_not_swallow_soft_time_limit(
    monkeypatch: pytest.MonkeyPatch,
    worker_module: object,
    task: object,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    def _raise_timeout(coroutine: object) -> None:
        coroutine.close()
        raise SoftTimeLimitExceeded

    monkeypatch.setattr(worker_module, "run_worker_async", _raise_timeout)

    with pytest.raises(SoftTimeLimitExceeded):
        task.run(*args, **kwargs)


@pytest.mark.parametrize(
    ("task", "args", "kwargs"),
    [
        (video_generation.run_video_generation, ("job-runtime-stopping",), {}),
        (
            video_generation.persist_video_task_checkpoint_task,
            (
                "job-runtime-stopping",
                "provider-runtime-stopping",
                "lease-runtime-stopping",
            ),
            {},
        ),
        (
            video_generation.reconcile_video_remote_cancellations_task,
            (),
            {"limit": 1},
        ),
        (
            video_generation.reconcile_video_financial_cleanup_task,
            (),
            {"limit": 1},
        ),
        (
            video_generation.reconcile_video_attempt_outputs_task,
            (),
            {
                "job_id": "job-runtime-stopping",
                "shot_group_id": "shot-runtime-stopping",
                "storage_attempt_id": "attempt-runtime-stopping",
                "video_storage_key": None,
                "last_frame_storage_key": None,
            },
        ),
        (video_generation.recover_stale_jobs, (), {}),
    ],
)
def test_video_entrypoints_recycle_child_when_async_runtime_is_stopping(
    monkeypatch: pytest.MonkeyPatch,
    task: object,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    from app.workers.async_runtime import WorkerAsyncRuntimeStopping

    stopping_error = WorkerAsyncRuntimeStopping("worker async runtime is stopping")

    def _raise_stopping(coroutine: object) -> None:
        coroutine.close()
        raise stopping_error

    monkeypatch.setattr(video_generation, "run_worker_async", _raise_stopping)

    with pytest.raises(WorkerTerminate) as exc_info:
        task.run(*args, **kwargs)

    assert exc_info.value.__cause__ is stopping_error


@pytest.mark.parametrize(
    "task",
    [
        video_generation.run_video_generation,
        video_generation.persist_video_task_checkpoint_task,
        video_generation.reconcile_video_remote_cancellations_task,
        video_generation.reconcile_video_financial_cleanup_task,
        video_generation.reconcile_video_attempt_outputs_task,
        video_generation.recover_stale_jobs,
    ],
)
def test_video_entrypoint_worker_loss_rejects_and_requeues_without_ack(
    monkeypatch: pytest.MonkeyPatch,
    task: object,
) -> None:
    reject = Mock()
    acknowledge = Mock()
    fake_request = SimpleNamespace(
        task=task,
        reject=reject,
        acknowledge=acknowledge,
        id="poisoned-runtime-request-id",
        _context={},
        store_errors=False,
        send_event=Mock(),
    )
    monkeypatch.setattr(request_module, "task_ready", Mock())

    assert task.acks_late is True
    assert task.reject_on_worker_lost is True
    assert task.acks_on_failure_or_timeout is False

    Request.on_failure(
        fake_request,
        SimpleNamespace(exception=WorkerLostError("poisoned async runtime")),
        send_failed_event=False,
        return_ok=True,
    )

    reject.assert_called_once_with(requeue=True)
    acknowledge.assert_not_called()


def test_worker_modules_do_not_create_per_task_event_loops() -> None:
    workers_dir = Path(__file__).parents[1] / "app" / "workers"
    isolated_recovery_functions = {
        ("video_generation.py", "_persist_video_task_checkpoint_in_isolated_runtime"),
    }
    offenders: list[str] = []
    for path in workers_dir.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "asyncio"
                and node.func.attr == "run"
            ):
                owner = parents.get(node)
                while owner is not None and not isinstance(
                    owner,
                    (ast.FunctionDef, ast.AsyncFunctionDef),
                ):
                    owner = parents.get(owner)
                owner_name = (
                    owner.name
                    if isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef))
                    else ""
                )
                if (path.name, owner_name) in isolated_recovery_functions:
                    continue
                offenders.append(f"{path.name}:{node.lineno}")

    assert offenders == []


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 跨任务连接池测试",
)
def test_runtime_reuses_real_async_engine_across_consecutive_tasks() -> None:
    """真实 asyncpg 单连接池连续查询不得发生跨 loop 或已关闭 loop 错误。"""

    engine = create_async_engine(
        os.environ["MTV_TEST_POSTGRES_URL"], pool_size=1, max_overflow=0
    )
    runtime = WorkerAsyncRuntime()

    async def _query() -> tuple[int, int]:
        async with engine.connect() as connection:
            backend_pid = (await connection.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        return id(asyncio.get_running_loop()), int(backend_pid)

    try:
        first = runtime.run(_query())
        second = runtime.run(_query())
        assert second == first
        runtime.run(engine.dispose())
    finally:
        runtime.shutdown()
