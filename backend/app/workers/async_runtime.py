"""Celery 同步任务共用的进程级异步运行时。"""

from __future__ import annotations

import asyncio
import atexit
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)
_DEFAULT_INTERRUPT_GRACE_SECONDS = 1.0


InterruptHook = Callable[[BaseException], None]
RuntimePoisonHook = Callable[[], None]
DetachedTaskAbandonHook = Callable[[], None]
DetachedTaskRegisterHook = Callable[
    [asyncio.Task[Any], DetachedTaskAbandonHook | None], None
]


class WorkerAsyncRuntimeStopping(RuntimeError):
    """持久事件循环已中毒或正在关闭，调用方不得继续复用。"""


@dataclass(frozen=True)
class _InterruptContext:
    hook: InterruptHook
    grace_timeout: float


_INTERRUPT_LOCAL = threading.local()


@contextmanager
def worker_async_interrupt_hook(
    hook: InterruptHook,
    *,
    grace_timeout: float = _DEFAULT_INTERRUPT_GRACE_SECONDS,
):
    """为当前同步 worker 调用临时注册 interrupt hook。"""

    previous = getattr(_INTERRUPT_LOCAL, "context", None)
    _INTERRUPT_LOCAL.context = _InterruptContext(
        hook=hook,
        grace_timeout=max(grace_timeout, 0.0),
    )
    try:
        yield
    finally:
        if previous is None:
            try:
                del _INTERRUPT_LOCAL.context
            except AttributeError:
                pass
        else:
            _INTERRUPT_LOCAL.context = previous


class _TaskScope:
    """跟踪一次 Celery 调用派生的 asyncio 子任务。"""

    def __init__(self) -> None:
        self.tasks: set[asyncio.Task[Any]] = set()

    def track(self, task: asyncio.Task[Any]) -> None:
        self.tasks.add(task)
        task.add_done_callback(self._discard)

    def _discard(self, future: asyncio.Future[Any]) -> None:
        if isinstance(future, asyncio.Task):
            self.tasks.discard(future)

    async def cancel_and_wait(self) -> None:
        """取消并等待所有子任务，包括取消清理期间新派生的任务。"""

        while True:
            pending = [task for task in self.tasks if not task.done()]
            if not pending:
                return
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)


_TASK_SCOPE: ContextVar[_TaskScope | None] = ContextVar(
    "celery_worker_task_scope", default=None
)
_RUNTIME_POISON_HOOK: ContextVar[RuntimePoisonHook | None] = ContextVar(
    "celery_worker_runtime_poison_hook", default=None
)
_RUNTIME_DETACHED_TASK_HOOK: ContextVar[DetachedTaskRegisterHook | None] = (
    ContextVar("celery_worker_runtime_detached_task_hook", default=None)
)


class _Invocation[T]:
    """一个可从同步线程取消、并能等待完整清理的顶层协程调用。"""

    def __init__(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        poison_runtime: RuntimePoisonHook,
        register_detached_task: DetachedTaskRegisterHook,
    ) -> None:
        self.coroutine = coroutine
        self.poison_runtime = poison_runtime
        self.register_detached_task = register_detached_task
        self.done = threading.Event()
        self._state_lock = threading.Lock()
        self._task: asyncio.Task[T] | None = None
        self._cancel_requested = False

    async def execute(self) -> T:
        task = asyncio.current_task()
        if task is None:
            self.coroutine.close()
            self.done.set()
            raise RuntimeError("worker async invocation has no current task")

        with self._state_lock:
            self._task = task
            cancel_requested = self._cancel_requested
        if cancel_requested:
            task.cancel()

        scope = _TaskScope()
        scope_token = _TASK_SCOPE.set(scope)
        poison_token = _RUNTIME_POISON_HOOK.set(self.poison_runtime)
        detached_task_token = _RUNTIME_DETACHED_TASK_HOOK.set(
            self.register_detached_task
        )
        try:
            return await self.coroutine
        finally:
            _RUNTIME_DETACHED_TASK_HOOK.reset(detached_task_token)
            _RUNTIME_POISON_HOOK.reset(poison_token)
            _TASK_SCOPE.reset(scope_token)
            try:
                await scope.cancel_and_wait()
            finally:
                self.done.set()

    def request_cancel(self, loop: asyncio.AbstractEventLoop) -> None:
        """在所属事件循环中取消顶层 Task。"""

        try:
            loop.call_soon_threadsafe(self._cancel_in_loop)
        except RuntimeError:
            # 循环意外关闭时无法执行异步 finally，只能关闭尚未开始的协程。
            self.close_unsubmitted()

    def _cancel_in_loop(self) -> None:
        with self._state_lock:
            self._cancel_requested = True
            task = self._task
        if task is not None and not task.done():
            task.cancel()

    def close_unsubmitted(self) -> None:
        self.coroutine.close()
        self.done.set()


class WorkerAsyncRuntime:
    """在专用线程的持久事件循环中执行 worker 异步主体。

    SQLAlchemy AsyncEngine 的连接池是进程级对象，池内 asyncpg 连接绑定首次
    使用它的事件循环。专用循环既保留连接池，也允许 Celery threads 池中的多个
    同步调用方安全地并发提交协程。

    生产环境要求 API 与 Celery worker 使用独立进程。若在 FastAPI 进程启用 Celery
    eager，同一个池仍可能被 API loop 与 worker loop 交叉使用，必须改用独立 worker
    Engine 或 NullPool；本运行时不能让同一个异步连接池同时归属两个事件循环。
    """

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._pid = os.getpid()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready: threading.Event | None = None
        self._startup_error: BaseException | None = None
        self._stopping = False
        self._terminal = False
        self._invocations: set[_Invocation[Any]] = set()
        self._detached_tasks: dict[
            asyncio.Task[Any], DetachedTaskAbandonHook | None
        ] = {}

    def reset_after_fork(self) -> None:
        """丢弃父进程的线程与循环引用，子进程按需创建自己的运行时。"""

        # fork 时其他线程可能正持有旧锁；子进程不能尝试获取继承的锁。
        self._state_lock = threading.Lock()
        self._pid = os.getpid()
        self._loop = None
        self._thread = None
        self._ready = None
        self._startup_error = None
        self._stopping = False
        self._terminal = False
        self._invocations = set()
        self._detached_tasks = {}

    def run[T](
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        on_interrupt: InterruptHook | None = None,
        interrupt_grace_timeout: float = _DEFAULT_INTERRUPT_GRACE_SECONDS,
    ) -> T:
        """同步等待协程完成，并原样传播结果或异常。"""

        invocation = _Invocation(
            coroutine,
            poison_runtime=self._poison,
            register_detached_task=self._register_detached_task,
        )
        try:
            loop = self._ensure_loop()
        except BaseException:
            invocation.close_unsubmitted()
            raise

        with self._state_lock:
            if self._stopping or self._terminal:
                invocation.close_unsubmitted()
                raise WorkerAsyncRuntimeStopping("worker async runtime is stopping")
            self._invocations.add(invocation)
            execution = invocation.execute()
            try:
                future: Future[T] = asyncio.run_coroutine_threadsafe(
                    execution, loop
                )
            except BaseException:
                self._invocations.discard(invocation)
                execution.close()
                invocation.close_unsubmitted()
                raise

        sync_interrupted = False
        try:
            return future.result()
        except BaseException as interrupt:
            # 协程自身异常会先完成 invocation，不属于同步线程中断。
            if invocation.done.is_set():
                raise
            sync_interrupted = True
            try:
                if on_interrupt is not None:
                    on_interrupt(interrupt)
            finally:
                invocation.request_cancel(loop)
            drained = self._wait_for_invocation(
                invocation,
                timeout=max(interrupt_grace_timeout, 0.0),
            )
            if not drained:
                logger.critical("worker_async_runtime_interrupt_cleanup_timeout")
            raise
        finally:
            with self._state_lock:
                if invocation.done.is_set():
                    self._invocations.discard(invocation)
                elif sync_interrupted:
                    # 未清理 invocation 继续受 shutdown 跟踪；禁止后续任务复用同一循环。
                    self._stopping = True

    def _poison(self) -> None:
        """发现无法收敛的脱离任务后停止接收新 invocation。"""

        with self._state_lock:
            self._stopping = True

    def shutdown(self, *, timeout: float = 10.0) -> None:
        """按 task drain → DB dispose → loop close 的顺序停止运行时。"""

        deadline = time.monotonic() + max(timeout, 0.0)
        with self._state_lock:
            thread = self._thread
            if thread is not None and threading.current_thread() is thread:
                raise RuntimeError("worker async runtime cannot synchronously stop itself")
            if self._terminal:
                return
            self._terminal = True
            self._stopping = True
            loop = self._loop
            ready = self._ready
            if thread is None:
                return
            invocations = tuple(self._invocations)

        # 覆盖 start thread 后、thread_main 发布 loop 前的窗口。超时返回后
        # thread_main 仍会观察 terminal，并在创建 loop 后直接关闭而不 run_forever。
        if loop is None and ready is not None:
            ready.wait(max(deadline - time.monotonic(), 0.0))
            with self._state_lock:
                loop = self._loop
        if loop is None:
            thread.join(timeout=max(deadline - time.monotonic(), 0.0))
            if thread.is_alive():
                logger.error("worker_async_runtime_startup_shutdown_timeout")
            return

        # startup 线程已经发布 loop 但尚未进入 run_forever 时，预排 stop 回调。
        # 若 shutdown 先到，thread_main 也会因 terminal 跳过 run_forever。
        if not loop.is_running():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass
            thread.join(timeout=max(deadline - time.monotonic(), 0.0))
            if thread.is_alive():
                logger.error("worker_async_runtime_startup_shutdown_timeout")
            return

        for invocation in invocations:
            invocation.request_cancel(loop)
        drained = True
        for invocation in invocations:
            remaining = max(deadline - time.monotonic(), 0.0)
            if not self._wait_for_invocation(invocation, timeout=remaining):
                drained = False
                logger.error("worker_async_runtime_invocation_drain_timeout")
                break

        try:
            if drained:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("worker async runtime drain deadline exceeded")
                drain = asyncio.run_coroutine_threadsafe(
                    self._cancel_remaining_tasks(), loop
                )
                drain.result(timeout=remaining)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("worker async runtime dispose deadline exceeded")
                cleanup = asyncio.run_coroutine_threadsafe(
                    self._dispose_database_pool(), loop
                )
                cleanup.result(timeout=remaining)
        except Exception:
            logger.exception("worker_async_runtime_cleanup_failed")
        finally:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass

        thread.join(timeout=max(deadline - time.monotonic(), 0.0))
        if thread.is_alive():
            logger.error("worker_async_runtime_shutdown_timeout")

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        current_pid = os.getpid()
        if self._pid != current_pid:
            self.reset_after_fork()

        with self._state_lock:
            if self._stopping or self._terminal:
                raise WorkerAsyncRuntimeStopping("worker async runtime is stopping")
            if (
                self._loop is not None
                and self._thread is not None
                and self._thread.is_alive()
                and self._loop.is_running()
            ):
                return self._loop

            if self._thread is None or not self._thread.is_alive():
                self._ready = threading.Event()
                self._startup_error = None
                self._thread = threading.Thread(
                    target=self._thread_main,
                    name=f"celery-async-runtime-{current_pid}",
                    daemon=True,
                )
                self._thread.start()
            ready = self._ready

        if ready is None:
            raise RuntimeError("worker async runtime failed to initialize")
        ready.wait()

        with self._state_lock:
            if self._startup_error is not None:
                raise RuntimeError("worker async runtime failed to start") from self._startup_error
            if self._stopping or self._terminal:
                raise WorkerAsyncRuntimeStopping("worker async runtime is stopping")
            if self._loop is None or not self._loop.is_running():
                raise RuntimeError("worker async runtime is unavailable")
            return self._loop

    def _thread_main(self) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        try:
            loop = asyncio.new_event_loop()
            loop.set_task_factory(self._task_factory)
            asyncio.set_event_loop(loop)
            with self._state_lock:
                self._loop = loop
                ready = self._ready
                should_run = not self._stopping and not self._terminal
            if ready is not None:
                ready.set()
            if should_run:
                loop.run_forever()
        except BaseException as exc:
            with self._state_lock:
                self._startup_error = exc
                ready = self._ready
            if ready is not None:
                ready.set()
            logger.exception("worker_async_runtime_loop_failed")
        finally:
            try:
                if loop is not None:
                    self._close_loop(loop)
            except BaseException:
                logger.exception("worker_async_runtime_loop_close_failed")
            finally:
                with self._state_lock:
                    if self._loop is loop:
                        self._loop = None
                        self._thread = None
                        self._ready = None
                        # loop 意外退出或 shutdown 超时后禁止复用未确认释放的旧池。
                        self._stopping = True
                        self._terminal = True
                        self._invocations.clear()
                        self._detached_tasks.clear()

    def _register_detached_task(
        self,
        task: asyncio.Task[Any],
        on_abandon: DetachedTaskAbandonHook | None,
    ) -> None:
        """登记不由 invocation 收尾等待、但 loop 关闭时必须放弃的任务。"""

        with self._state_lock:
            self._detached_tasks[task] = on_abandon
        task.add_done_callback(self._discard_detached_task)

    def _discard_detached_task(self, future: asyncio.Future[Any]) -> None:
        if not isinstance(future, asyncio.Task):
            return
        with self._state_lock:
            self._detached_tasks.pop(future, None)

    @staticmethod
    def _task_factory(
        loop: asyncio.AbstractEventLoop,
        coroutine: Coroutine[Any, Any, Any],
        context: Context | None = None,
    ) -> asyncio.Task[Any]:
        task = asyncio.Task(coroutine, loop=loop, context=context)
        scope = (
            context.get(_TASK_SCOPE, None) if context is not None else _TASK_SCOPE.get()
        )
        if scope is not None:
            scope.track(task)
        return task

    @staticmethod
    def _wait_for_invocation(
        invocation: _Invocation[Any], *, timeout: float | None = None
    ) -> bool:
        deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)
        while not invocation.done.is_set():
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
            invocation.done.wait(remaining)
        return True

    async def _cancel_remaining_tasks(self) -> None:
        current = asyncio.current_task()
        with self._state_lock:
            detached = set(self._detached_tasks)
        pending = [
            task
            for task in asyncio.all_tasks()
            if task is not current and task not in detached and not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def _close_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        with self._state_lock:
            detached = {
                task: on_abandon
                for task, on_abandon in self._detached_tasks.items()
                if task.get_loop() is loop
            }
        pending = asyncio.all_tasks(loop) - set(detached)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        for task, on_abandon in detached.items():
            if not task.done():
                task.cancel()
                # 子进程即将退出，不再驱动拒绝取消的 detached 协程。
                task._log_destroy_pending = False  # type: ignore[attr-defined]
            if on_abandon is not None:
                try:
                    on_abandon()
                except Exception:
                    logger.exception("worker_async_runtime_detached_abandon_failed")
            with self._state_lock:
                self._detached_tasks.pop(task, None)
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        asyncio.set_event_loop(None)
        loop.close()

    @staticmethod
    async def _dispose_database_pool() -> None:
        database_module = sys.modules.get("app.core.database")
        engine = getattr(database_module, "engine", None)
        if engine is not None:
            await engine.dispose()


_runtime = WorkerAsyncRuntime()

if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_runtime.reset_after_fork)


def run_worker_async[T](
    coroutine: Coroutine[Any, Any, T],
    *,
    on_interrupt: InterruptHook | None = None,
    interrupt_grace_timeout: float = _DEFAULT_INTERRUPT_GRACE_SECONDS,
) -> T:
    """在当前 Celery worker 进程的持久事件循环中运行协程。"""

    context = getattr(_INTERRUPT_LOCAL, "context", None)
    if on_interrupt is None and isinstance(context, _InterruptContext):
        on_interrupt = context.hook
        interrupt_grace_timeout = context.grace_timeout
    return _runtime.run(
        coroutine,
        on_interrupt=on_interrupt,
        interrupt_grace_timeout=interrupt_grace_timeout,
    )


def poison_current_worker_async_runtime() -> bool:
    """毒化当前 invocation 所属运行时；普通 asyncio 调用返回 False。"""

    poison = _RUNTIME_POISON_HOOK.get()
    if poison is None:
        return False
    poison()
    return True


def register_current_worker_async_runtime_detached_task(
    task: asyncio.Task[Any],
    *,
    on_abandon: DetachedTaskAbandonHook | None = None,
) -> bool:
    """登记脱离 invocation scope 的任务，供运行时关闭时跳过并放弃。"""

    register = _RUNTIME_DETACHED_TASK_HOOK.get()
    if register is None:
        return False
    register(task, on_abandon)
    return True


def reset_worker_async_runtime_after_fork() -> None:
    """供 Celery prefork 子进程初始化信号调用。"""

    _runtime.reset_after_fork()


def discard_inherited_database_pool() -> None:
    """子进程替换父进程继承的池，不触碰父进程持有的连接。"""

    database_module = sys.modules.get("app.core.database")
    engine = getattr(database_module, "engine", None)
    if engine is not None:
        engine.sync_engine.dispose(close=False)


def shutdown_worker_async_runtime() -> None:
    """停止运行时；可由多个 Celery 关闭信号幂等调用。"""

    _runtime.shutdown()


atexit.register(shutdown_worker_async_runtime)
