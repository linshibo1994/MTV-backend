"""视频生成 Celery worker — 调 provider → 存视频 → settle 积分 → 更新 last_frame。

流程:
1. 读 VideoGenerationJob (status=starting)
2. 取激活 provider (provider_key)
3. provider.generate_all_reference(prompt, reference_images, reference_audios, model)
   → 返回 video storage_key
4. credits.settle (actual=settings.price_video_gen)
5. 更新 job.output_json (video storage_key) + metrics
6. 从视频末尾提帧（ffmpeg）更新 shot_group.reference_frames.last_frame
7. job.status=succeeded, progress=100

失败路径:
- provider 抛错 → job.status=failed, error_message 记录
- credits.release(settings.price_video_gen_frozen) 释放冻结
- 不自动重试（用户手动重试创建新 attempt，见 PLAN 5.5）

worker 内使用进程级持久事件循环复用 async session，避免连接池跨 loop。
"""

import asyncio
import hashlib
import logging
import os
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from time import sleep
from typing import Any, NoReturn

from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.exceptions import Reject, WorkerTerminate

from app.core.config import settings
from app.providers.video_gen import (
    VideoCancellationOutcome,
    VideoCancellationResult,
    VideoGenerationRemoteTerminalError,
    VideoGenerationSubmissionUncertain,
    VideoGenerationSubmitRejected,
    VideoTaskCheckpointConflict,
    VideoTaskCheckpointUnavailable,
    get_video_provider,
    sanitize_video_submit_rejection_diagnostics,
)
from app.workers.async_runtime import (
    WorkerAsyncRuntimeStopping,
    run_worker_async,
    worker_async_interrupt_hook,
)
from app.workers.celery_app import app

logger = logging.getLogger(__name__)
_LEASE_SECONDS = 1800
_STORAGE_CHUNK_SIZE = 1024 * 1024
_MAX_LAST_FRAME_BYTES = 50 * 1024 * 1024
_ATTEMPT_CLEANUP_RETRY_SECONDS = 30
_CANCELLATION_RECOVERY_CHANNEL_RETRY_SECONDS = 5
_TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS = 5
_VIDEO_INTERRUPT_GRACE_SECONDS = 0.25
_ATTEMPT_CLEANUP_DEBT_KEY = "attempt_output_cleanup"
_PROVIDER_REMOTE_TERMINAL_KEY = "provider_remote_terminal"
_PROVIDER_MEDIA_LIMIT_ERRORS = frozenset(
    {
        "download_too_large",
        "video_download_too_large",
        "video_inline_too_large",
        "reference_image_too_large",
        "reference_audio_too_large",
        "reference_media_too_large",
        "reference_media_total_too_large",
    }
)


def _requeue_and_recycle_poisoned_video_worker(
    error: WorkerAsyncRuntimeStopping,
) -> NoReturn:
    """让 prefork 子进程退出，由 worker-lost 契约重排当前原消息。"""

    logger.critical("视频异步运行时已停止，回收 prefork 子进程并重排当前消息")
    raise WorkerTerminate() from error


@dataclass(frozen=True)
class _VideoTaskCheckpointSnapshot:
    provider_task_id: str | None
    lease_token: str | None
    durable: bool
    error: BaseException | None


class _VideoTaskCheckpointHolder:
    """跨同步 Celery 线程与异步运行时传递远端 ID 的单次调用状态。"""

    def __init__(self) -> None:
        self._lock = Lock()
        self._provider_task_id: str | None = None
        self._lease_token: str | None = None
        self._durable = False
        self._error: BaseException | None = None

    def observe(self, provider_task_id: str, lease_token: str) -> None:
        """在 checkpoint callback 的首个 await 前记录唯一执行事实。"""

        with self._lock:
            if (
                self._provider_task_id is not None
                and self._provider_task_id != provider_task_id
            ) or (self._lease_token is not None and self._lease_token != lease_token):
                conflict = VideoTaskCheckpointConflict(
                    "video_task_checkpoint_conflict:holder",
                    provider_task_id=provider_task_id,
                )
                self._error = conflict
                raise conflict
            self._provider_task_id = provider_task_id
            self._lease_token = lease_token

    def mark_durable(self, provider_task_id: str, lease_token: str) -> None:
        with self._lock:
            if (
                self._provider_task_id != provider_task_id
                or self._lease_token != lease_token
            ):
                conflict = VideoTaskCheckpointConflict(
                    "video_task_checkpoint_conflict:holder_durable",
                    provider_task_id=provider_task_id,
                )
                self._error = conflict
                raise conflict
            self._durable = True
            self._error = None

    def mark_error(self, error: BaseException) -> None:
        with self._lock:
            if not self._durable:
                self._error = error

    def snapshot(self) -> _VideoTaskCheckpointSnapshot:
        with self._lock:
            return _VideoTaskCheckpointSnapshot(
                provider_task_id=self._provider_task_id,
                lease_token=self._lease_token,
                durable=self._durable,
                error=self._error,
            )


def _publish_video_task_checkpoint_on_interrupt(
    holder: _VideoTaskCheckpointHolder,
    *,
    job_id: str,
    interrupt: BaseException,
) -> None:
    """同步 soft/hard 到达时，在等待异步 cleanup 前抢先持久化 broker 事实。"""

    checkpoint = holder.snapshot()
    if (
        checkpoint.durable
        or checkpoint.provider_task_id is None
        or checkpoint.lease_token is None
    ):
        return
    try:
        _publish_video_task_checkpoint(
            job_id=job_id,
            provider_task_id=checkpoint.provider_task_id,
            lease_token=checkpoint.lease_token,
        )
    except TimeLimitExceeded:
        raise
    except Exception as exc:  # noqa: BLE001 - cleanup 后继续竞争独立 DB 通道
        holder.mark_error(exc)
        logger.warning(
            "同步中断时发布视频 checkpoint 失败 job_id=%s "
            "interrupt_type=%s error_type=%s",
            job_id,
            interrupt.__class__.__name__,
            exc.__class__.__name__,
        )
        return
    holder.mark_durable(
        checkpoint.provider_task_id,
        checkpoint.lease_token,
    )


class VideoCancellationRecoveryUnavailable(RuntimeError):
    """远端取消事实既未持久化，也没有可恢复补偿通道。"""

    def __init__(
        self,
        message: str,
        *,
        provider_task_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.provider_task_id = provider_task_id


class VideoTaskCheckpointRecoveryUnavailable(VideoTaskCheckpointUnavailable):
    """生成协程已退出，入口必须继续竞争 DB 与专用 broker 通道。"""

    def __init__(
        self,
        *,
        provider_task_id: str,
        lease_token: str,
    ) -> None:
        super().__init__(
            "video_task_checkpoint_recovery_unavailable",
            provider_task_id=provider_task_id,
        )
        self.lease_token = lease_token


async def _persist_video_task_checkpoint_independently(
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> bool:
    """使用独立事务提交 Provider ID，提交不确定由调用方切换 broker。"""

    from app.core.database import async_session_factory
    from app.services.video_remote_cancellation import persist_video_task_checkpoint

    async with async_session_factory() as checkpoint_db:
        persisted = await persist_video_task_checkpoint(
            checkpoint_db,
            job_id,
            provider_task_id,
            lease_token,
        )
        if not persisted:
            await checkpoint_db.rollback()
            return False
        await checkpoint_db.commit()
        return True


async def _persist_provider_submission_state_independently(
    job_id: str,
    lease_token: str,
    *,
    state: str,
    provider_key: str,
    attempt: int,
) -> bool:
    """提交边界使用独立事务，确保远端调用前已有 durable intent。"""

    from app.core.database import async_session_factory
    from app.services.video_remote_cancellation import persist_provider_submission_state

    async with async_session_factory() as submission_db:
        persisted = await persist_provider_submission_state(
            submission_db,
            job_id,
            lease_token,
            state=state,
            provider_key=provider_key,
            attempt=attempt,
        )
        if not persisted:
            await submission_db.rollback()
            return False
        await submission_db.commit()
        return True


async def _persist_video_task_checkpoint_with_isolated_engine(
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> bool:
    """在一次性引擎上提交 checkpoint，避免复用中毒 loop 绑定的连接池。"""

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    from app.services.video_remote_cancellation import persist_video_task_checkpoint

    isolated_engine = create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
        poolclass=NullPool,
    )
    isolated_session_factory = async_sessionmaker(
        isolated_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    try:
        async with isolated_session_factory() as checkpoint_db:
            persisted = await persist_video_task_checkpoint(
                checkpoint_db,
                job_id,
                provider_task_id,
                lease_token,
            )
            if not persisted:
                await checkpoint_db.rollback()
                return False
            await checkpoint_db.commit()
            return True
    finally:
        await isolated_engine.dispose()


def _persist_video_task_checkpoint_in_isolated_runtime(
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> bool:
    """用一次性事件循环执行独立引擎事务。"""

    return asyncio.run(
        _persist_video_task_checkpoint_with_isolated_engine(
            job_id,
            provider_task_id,
            lease_token,
        )
    )


def _publish_video_task_checkpoint(
    *,
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> None:
    """发布只携检查点事实的专用消息，不复用业务生成入口。"""

    persist_video_task_checkpoint_task.apply_async(
        kwargs={
            "job_id": job_id,
            "provider_task_id": provider_task_id,
            "lease_token": lease_token,
        }
    )


def _try_checkpoint_recovery_after_runtime_stops(
    *,
    job_id: str,
    provider_task_id: str,
    lease_token: str,
    attempt: int,
) -> dict[str, str] | None:
    """共享 loop 无法复用时，依次竞争隔离 DB 与专用 broker。"""

    try:
        if _persist_video_task_checkpoint_in_isolated_runtime(
            job_id,
            provider_task_id,
            lease_token,
        ):
            return {
                "job_id": job_id,
                "provider_task_id": provider_task_id,
                "status": "checkpointed",
            }
    except TimeLimitExceeded:
        raise
    except VideoTaskCheckpointConflict:
        logger.critical(
            "视频任务 ID 隔离 checkpoint 恢复冲突，保留 processing lease "
            "job_id=%s task_id=%s",
            job_id,
            provider_task_id,
        )
        return {
            "job_id": job_id,
            "provider_task_id": provider_task_id,
            "status": "conflict",
        }
    except SoftTimeLimitExceeded as exc:
        logger.warning(
            "视频任务 ID 隔离 checkpoint 进入 soft limit 恢复窗口 "
            "job_id=%s task_id=%s attempt=%s error_type=%s",
            job_id,
            provider_task_id,
            attempt,
            exc.__class__.__name__,
        )
    except Exception as exc:  # noqa: BLE001 - 继续竞争专用 broker 通道
        logger.warning(
            "视频任务 ID 隔离 checkpoint 恢复失败 job_id=%s task_id=%s "
            "attempt=%s error_type=%s",
            job_id,
            provider_task_id,
            attempt,
            exc.__class__.__name__,
        )

    try:
        _publish_video_task_checkpoint(
            job_id=job_id,
            provider_task_id=provider_task_id,
            lease_token=lease_token,
        )
    except TimeLimitExceeded:
        raise
    except SoftTimeLimitExceeded:
        return None
    except Exception as exc:  # noqa: BLE001 - 内存仍持有唯一远端 ID
        logger.warning(
            "视频任务 ID 隔离恢复的 broker 通道失败 "
            "job_id=%s task_id=%s attempt=%s error_type=%s",
            job_id,
            provider_task_id,
            attempt,
            exc.__class__.__name__,
        )
        return None
    return {
        "job_id": job_id,
        "provider_task_id": provider_task_id,
        "status": "checkpointed",
    }


async def _ensure_video_task_checkpoint_durable(
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> None:
    """DB 或 broker 至少成功一个；双故障短退避，soft 转携 ID 恢复。"""

    attempt = 0
    while True:
        attempt += 1
        soft_error: SoftTimeLimitExceeded | None = None
        try:
            if await _persist_video_task_checkpoint_independently(
                job_id,
                provider_task_id,
                lease_token,
            ):
                return
        except TimeLimitExceeded:
            raise
        except VideoTaskCheckpointConflict:
            raise
        except SoftTimeLimitExceeded as exc:
            soft_error = exc
        except Exception as exc:  # noqa: BLE001 - 继续竞争 broker 持久通道
            logger.warning(
                "视频任务 ID 独立 checkpoint 失败 job_id=%s task_id=%s "
                "attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                exc.__class__.__name__,
            )

        try:
            _publish_video_task_checkpoint(
                job_id=job_id,
                provider_task_id=provider_task_id,
                lease_token=lease_token,
            )
        except TimeLimitExceeded:
            raise
        except SoftTimeLimitExceeded as exc:
            soft_error = soft_error or exc
        except Exception as exc:  # noqa: BLE001 - 双通道失败后留在当前进程重试
            logger.warning(
                "发布视频任务 ID checkpoint 失败 job_id=%s task_id=%s "
                "attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                exc.__class__.__name__,
            )
        else:
            return

        if soft_error is not None:
            raise VideoTaskCheckpointUnavailable(
                "video_task_checkpoint_unavailable",
                provider_task_id=provider_task_id,
            ) from soft_error
        try:
            await asyncio.sleep(_TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS)
        except TimeLimitExceeded:
            raise
        except SoftTimeLimitExceeded as exc:
            raise VideoTaskCheckpointUnavailable(
                "video_task_checkpoint_unavailable",
                provider_task_id=provider_task_id,
            ) from exc


def _video_cancellation_recovery_error(
    provider_task_id: str,
    *,
    cause: Exception | None = None,
) -> VideoCancellationRecoveryUnavailable:
    """构造唯一的携 Provider ID 恢复信号。"""

    error = VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    error.__cause__ = cause
    return error


def _retry_video_cancellation_recovery_until_durable(
    task: Any,
    *,
    job_id: str,
    provider_task_id: str,
    recovery_error: VideoCancellationRecoveryUnavailable,
) -> NoReturn:
    """持续竞争 DB 与 broker，直到至少一个恢复通道持久化。

    原始 raw payload 不含运行中才取得的 Provider ID，双通道失败时不能靠 requeue 保存它。
    """

    retry_kwargs = dict(getattr(task.request, "kwargs", None) or {})
    retry_kwargs["provider_task_id"] = provider_task_id
    attempt = 0
    soft_limit_seen = False
    while True:
        attempt += 1
        recovery_ready = False
        try:
            recovery_ready = run_worker_async(
                _ensure_video_cancellation_recovery_independently(
                    job_id,
                    provider_task_id,
                )
            )
        except SoftTimeLimitExceeded:
            if not soft_limit_seen:
                logger.critical(
                    "视频取消恢复进入 soft time limit 最后窗口 job_id=%s task_id=%s",
                    job_id,
                    provider_task_id,
                )
            soft_limit_seen = True
        except TimeLimitExceeded:
            raise
        except Exception as recovery_exc:  # noqa: BLE001 - 仍继续尝试 broker 通道
            logger.warning(
                "建立视频取消 DB 恢复通道失败 job_id=%s task_id=%s "
                "attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                recovery_exc.__class__.__name__,
            )

        try:
            retry_signal = task.retry(
                exc=recovery_error,
                countdown=_ATTEMPT_CLEANUP_RETRY_SECONDS,
                kwargs=retry_kwargs,
                throw=False,
            )
        except SoftTimeLimitExceeded:
            if not soft_limit_seen:
                logger.critical(
                    "视频取消恢复发布进入 soft time limit 最后窗口 "
                    "job_id=%s task_id=%s",
                    job_id,
                    provider_task_id,
                )
            soft_limit_seen = True
        except Reject:
            if recovery_ready:
                raise
            if attempt == 1 or attempt % 30 == 0:
                logger.critical(
                    "视频取消 DB 与 broker 恢复通道同时不可用，当前任务继续重试 "
                    "job_id=%s task_id=%s attempt=%s",
                    job_id,
                    provider_task_id,
                    attempt,
                )
        else:
            raise retry_signal from recovery_error

        try:
            sleep(_CANCELLATION_RECOVERY_CHANNEL_RETRY_SECONDS)
        except SoftTimeLimitExceeded:
            if not soft_limit_seen:
                logger.critical(
                    "视频取消恢复退避进入 soft time limit 最后窗口 "
                    "job_id=%s task_id=%s",
                    job_id,
                    provider_task_id,
                )
            soft_limit_seen = True


def _retry_video_cancellation_recovery_after_entry_error(
    task: Any,
    *,
    job_id: str,
    provider_task_id: str,
    entry_error: Exception,
) -> NoReturn:
    """已携远端 ID 的恢复消息失败时，继续建立可持久恢复通道。"""

    logger.warning(
        "携 Provider ID 的视频取消恢复消息执行失败，继续恢复 "
        "job_id=%s task_id=%s error_type=%s",
        job_id,
        provider_task_id,
        entry_error.__class__.__name__,
    )
    recovery_error = _video_cancellation_recovery_error(
        provider_task_id,
        cause=entry_error,
    )
    _retry_video_cancellation_recovery_until_durable(
        task,
        job_id=job_id,
        provider_task_id=provider_task_id,
        recovery_error=recovery_error,
    )


def _recover_video_task_checkpoint_until_durable(
    *,
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> dict[str, str]:
    """生成协程退出后继续持有内存 ID，直到 DB 或专用消息持久成功。"""

    attempt = 0
    while True:
        attempt += 1
        try:
            run_worker_async(
                _ensure_video_task_checkpoint_durable(
                    job_id,
                    provider_task_id,
                    lease_token,
                )
            )
        except TimeLimitExceeded:
            raise
        except WorkerAsyncRuntimeStopping:
            recovered = _try_checkpoint_recovery_after_runtime_stops(
                job_id=job_id,
                provider_task_id=provider_task_id,
                lease_token=lease_token,
                attempt=attempt,
            )
            if recovered is not None:
                return recovered
        except VideoTaskCheckpointConflict:
            logger.critical(
                "视频任务 ID checkpoint 恢复冲突，保留 processing lease "
                "job_id=%s task_id=%s",
                job_id,
                provider_task_id,
            )
            return {
                "job_id": job_id,
                "provider_task_id": provider_task_id,
                "status": "conflict",
            }
        except (SoftTimeLimitExceeded, VideoTaskCheckpointUnavailable) as exc:
            logger.warning(
                "视频任务 ID checkpoint 进入 soft limit 恢复窗口 "
                "job_id=%s task_id=%s attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                exc.__class__.__name__,
            )
        except Exception as exc:  # noqa: BLE001 - 内存仍持有唯一远端 ID
            logger.warning(
                "视频任务 ID checkpoint 恢复失败 job_id=%s task_id=%s "
                "attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                exc.__class__.__name__,
            )
        else:
            return {
                "job_id": job_id,
                "provider_task_id": provider_task_id,
                "status": "checkpointed",
            }
        try:
            sleep(_TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS)
        except TimeLimitExceeded:
            raise
        except SoftTimeLimitExceeded:
            continue


# 跨模块依赖
try:
    from app.services.credits import release as _credits_release
    from app.services.credits import settle as _credits_settle
except ImportError:  # pragma: no cover - T2 未就绪
    logger.warning(
        "积分服务 app.services.credits 不可导入，视频生成 settle/release 将被跳过 — "
        "成功不结算、失败不释放，计费被旁路。请尽快接入 T2 积分模块。"
    )
    _credits_settle = None  # type: ignore[assignment]
    _credits_release = None  # type: ignore[assignment]


@app.task(
    name="app.workers.video_generation.run_video_generation",
    bind=True,
    max_retries=None,
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def run_video_generation(
    self,
    job_id: str,
    provider_task_id: str | None = None,
) -> dict:
    """视频生成任务入口（同步签名，内部在 worker 持久事件循环执行异步逻辑）。

    Args:
        job_id: VideoGenerationJob.job_id

    Returns:
        {"job_id": ..., "status": ..., "storage_key": ...}
    """
    checkpoint_holder = _VideoTaskCheckpointHolder()
    try:
        with worker_async_interrupt_hook(
            lambda interrupt: _publish_video_task_checkpoint_on_interrupt(
                checkpoint_holder,
                job_id=job_id,
                interrupt=interrupt,
            ),
            grace_timeout=_VIDEO_INTERRUPT_GRACE_SECONDS,
        ):
            result = run_worker_async(
                _run_video_generation_async(
                    job_id,
                    provider_task_id=provider_task_id,
                    checkpoint_holder=checkpoint_holder,
                )
            )
        return result
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except SoftTimeLimitExceeded as exc:
        checkpoint = checkpoint_holder.snapshot()
        if checkpoint.provider_task_id is not None and checkpoint.lease_token is not None:
            if not checkpoint.durable:
                _recover_video_task_checkpoint_until_durable(
                    job_id=job_id,
                    provider_task_id=checkpoint.provider_task_id,
                    lease_token=checkpoint.lease_token,
                )
            raise
        if provider_task_id is None:
            raise
        _retry_video_cancellation_recovery_after_entry_error(
            self,
            job_id=job_id,
            provider_task_id=provider_task_id,
            entry_error=exc,
        )
    except TimeLimitExceeded:
        raise
    except VideoTaskCheckpointRecoveryUnavailable as exc:
        return _recover_video_task_checkpoint_until_durable(
            job_id=job_id,
            provider_task_id=exc.provider_task_id,
            lease_token=exc.lease_token,
        )
    except VideoCancellationRecoveryUnavailable as exc:
        recovery_task_id = exc.provider_task_id or provider_task_id
        if recovery_task_id is None:
            raise Reject(exc, requeue=True) from exc
        _retry_video_cancellation_recovery_until_durable(
            self,
            job_id=job_id,
            provider_task_id=recovery_task_id,
            recovery_error=exc,
        )
    except Exception as exc:
        if provider_task_id is not None:
            _retry_video_cancellation_recovery_after_entry_error(
                self,
                job_id=job_id,
                provider_task_id=provider_task_id,
                entry_error=exc,
            )
        # 此处没有 claim 返回的 lease token，禁止修改任务或释放积分。
        logger.exception("视频生成任务异常 job_id=%s", job_id)
        error_type = _safe_error_type(exc, fallback="worker_internal_error")
        return {"job_id": job_id, "status": "failed", "error": error_type}


async def _run_video_task_checkpoint_async(
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> dict[str, str]:
    """专用消息只持久化 ID，不进入 Provider、取消或业务失败路径。"""

    persisted = await _persist_video_task_checkpoint_independently(
        job_id,
        provider_task_id,
        lease_token,
    )
    if not persisted:
        raise VideoTaskCheckpointUnavailable(
            "video_task_checkpoint_unavailable",
            provider_task_id=provider_task_id,
        )
    return {
        "job_id": job_id,
        "provider_task_id": provider_task_id,
        "status": "persisted",
    }


@app.task(
    name="app.workers.video_generation.persist_video_task_checkpoint",
    bind=True,
    max_retries=None,
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def persist_video_task_checkpoint_task(
    self,
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> dict[str, str]:
    """独立 checkpoint 消费者；临时故障始终携原 payload 重试。"""

    try:
        return run_worker_async(
            _run_video_task_checkpoint_async(
                job_id,
                provider_task_id,
                lease_token,
            )
        )
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except TimeLimitExceeded:
        raise
    except VideoTaskCheckpointConflict:
        logger.critical(
            "视频任务 ID checkpoint 消息冲突，停止自动重试 job_id=%s task_id=%s",
            job_id,
            provider_task_id,
        )
        return {
            "job_id": job_id,
            "provider_task_id": provider_task_id,
            "status": "conflict",
        }
    except Exception as exc:  # noqa: BLE001 - broker payload 是当前唯一可恢复事实
        try:
            raise self.retry(
                exc=exc,
                countdown=_TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS,
                kwargs={
                    "job_id": job_id,
                    "provider_task_id": provider_task_id,
                    "lease_token": lease_token,
                },
            ) from exc
        except Reject as retry_publish_error:
            # retry 消息未能发布时，原消息仍是唯一 durable 事实，必须交还 broker。
            raise Reject(retry_publish_error.reason, requeue=True) from retry_publish_error


async def _run_video_generation_async(
    job_id: str,
    *,
    provider_task_id: str | None = None,
    checkpoint_holder: _VideoTaskCheckpointHolder | None = None,
) -> dict:
    """异步执行视频生成核心逻辑。"""
    from app.core.database import async_session_factory

    async with async_session_factory() as db:
        import socket

        owner = f"celery:{socket.gethostname()}:{os.getpid()}"
        await run_video_generation_once(
            db,
            job_id,
            owner=owner,
            recovery_provider_task_id=provider_task_id,
            checkpoint_holder=checkpoint_holder,
        )
        job = await _get_video_job(db, job_id)
        return {
            "job_id": job_id,
            "status": getattr(job, "status", "missing"),
            "storage_key": ((getattr(job, "output_json", None) or {}).get("video_storage_key")),
        }


async def _get_video_job(db: Any, job_id: str):
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    return (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


def _provider_task_id_from_job(job: Any) -> str | None:
    """从权威任务快照提取可恢复的 Provider 任务 ID。"""

    authority_consistent, provider_task_id = _provider_task_id_authority_from_job(job)
    if not authority_consistent:
        logger.error(
            "视频任务 Provider ID 权威来源冲突 job_id=%s",
            getattr(job, "job_id", "unknown"),
        )
        return None
    return provider_task_id


def _provider_task_id_authority_from_job(job: Any) -> tuple[bool, str | None]:
    from app.services.video_remote_cancellation import provider_task_id_authority

    return provider_task_id_authority(
        dict(getattr(job, "output_json", None) or {}),
        dict(getattr(job, "metrics", None) or {}),
    )


async def _read_authoritative_provider_task_id(
    db: Any,
    job_id: str,
) -> tuple[bool, str | None]:
    """返回（判权是否成功、最新 Provider 任务 ID）。"""

    try:
        job = await _get_video_job(db, job_id)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 判权失败时必须保持租约和冻结积分
        logger.warning(
            "读取视频 Provider 权威进度失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False, None
    if job is None:
        logger.warning("读取视频 Provider 权威进度时任务不存在 job_id=%s", job_id)
        return False, None
    return _provider_task_id_authority_from_job(job)


async def _read_video_submission_unresolved(
    db: Any,
    job_id: str,
) -> bool | None:
    """读取提交边界状态；无法证明时返回 None，调用方必须 fail closed。"""

    from app.services.video_remote_cancellation import provider_submission_is_unresolved

    try:
        job = await _get_video_job(db, job_id)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 判权失败时禁止释放或重提
        logger.warning(
            "读取视频提交边界失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return None
    if job is None:
        return None
    return provider_submission_is_unresolved(job.output_json, job.metrics)


async def _read_video_submission_unresolved_independently(
    job_id: str,
) -> bool | None:
    from app.core.database import async_session_factory

    try:
        async with async_session_factory() as authority_db:
            return await _read_video_submission_unresolved(authority_db, job_id)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 独立会话也失败时保持冻结
        logger.warning(
            "独立读取视频提交边界失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return None


async def _read_authoritative_provider_task_id_independently(
    job_id: str,
) -> tuple[bool, str | None]:
    """原事务不可恢复时，使用独立会话读取 Provider 任务 ID。"""

    from app.core.database import async_session_factory

    try:
        async with async_session_factory() as authority_db:
            return await _read_authoritative_provider_task_id(authority_db, job_id)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 无法判权时禁止释放积分
        logger.warning(
            "独立读取视频 Provider 权威进度失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False, None


async def _cancel_provider_task_after_generation_failure(
    provider: Any,
    provider_task_id: str,
) -> bool:
    """仅在同一远端任务已确认终止时允许进入普通失败补偿。"""

    cancel = getattr(provider, "cancel", None)
    if not callable(cancel):
        logger.error(
            "视频 Provider 不具备可恢复取消通道 task_id=%s",
            provider_task_id,
        )
        return False
    try:
        result = await cancel(provider_task_id)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 远端状态未知时必须 fail closed
        logger.warning(
            "视频 Provider 异常后的远端取消失败 task_id=%s error_type=%s",
            provider_task_id,
            exc.__class__.__name__,
        )
        return False
    if result.provider_task_id != provider_task_id:
        logger.error(
            "视频 Provider 取消结果任务 ID 不匹配 expected=%s actual=%s",
            provider_task_id,
            result.provider_task_id,
        )
        return False
    if result.outcome is not VideoCancellationOutcome.CANCELED:
        logger.warning(
            "视频 Provider 未确认远端任务终止 task_id=%s outcome=%s",
            provider_task_id,
            result.outcome.value,
        )
        return False
    return True


async def _authoritative_video_attempt_succeeded(
    job_id: str,
    video_storage_key: str,
) -> bool | None:
    """用独立会话消解提交结果；读取不确定时禁止破坏性清理。"""

    from sqlalchemy import select

    from app.core.database import async_session_factory
    from app.models.video_generation_job import VideoGenerationJob

    try:
        async with async_session_factory() as db:
            result = await db.execute(
                select(VideoGenerationJob)
                .where(VideoGenerationJob.job_id == job_id)
                .with_for_update()
            )
            job = result.scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001 - 不确定终态必须 fail closed
        logger.warning(
            "核验视频任务终态失败 error_type=%s",
            exc.__class__.__name__,
        )
        return None
    return bool(
        job is not None
        and job.status == "succeeded"
        and (job.output_json or {}).get("video_storage_key") == video_storage_key
    )


async def _authoritative_last_frame_referenced(
    shot_group_id: str,
    last_frame_storage_key: str,
) -> bool | None:
    """独立读取 ShotGroup，避免删除已经成为权威引用的末帧。"""

    from sqlalchemy import select

    from app.core.database import async_session_factory
    from app.models.shot_group import ShotGroup

    try:
        async with async_session_factory() as db:
            reference_frames = (
                await db.execute(
                    select(ShotGroup.reference_frames)
                    .where(ShotGroup.shot_group_id == shot_group_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
    except Exception as exc:  # noqa: BLE001 - 读取不确定时禁止破坏性清理
        logger.warning(
            "核验视频末帧权威引用失败 error_type=%s",
            exc.__class__.__name__,
        )
        return None
    return bool(
        reference_frames
        and reference_frames.get("last_frame") == last_frame_storage_key
    )


async def _best_effort_rollback(db: Any, *, job_id: str, stage: str) -> bool:
    """回滚失败不能阻断独立判权与对象清理。"""

    try:
        await db.rollback()
        return True
    except Exception as exc:  # noqa: BLE001 - 后续必须切换独立会话判权
        logger.warning(
            "视频任务回滚失败 job_id=%s stage=%s error_type=%s",
            job_id,
            stage,
            exc.__class__.__name__,
        )
        return False


async def _persist_video_cancellation_result_independently(
    job_id: str,
    result: VideoCancellationResult,
) -> bool:
    """原会话回滚失败时，使用独立事务保存远端取消结果。"""

    from app.core.database import async_session_factory
    from app.services.video_remote_cancellation import (
        persist_provider_cancellation_result,
    )

    try:
        async with async_session_factory() as cancellation_db:
            persisted = await persist_provider_cancellation_result(
                cancellation_db,
                job_id,
                result,
            )
            if not persisted:
                await cancellation_db.rollback()
                return False
            await cancellation_db.commit()
            return True
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 本地 canceled 终态不得被覆盖
        logger.warning(
            "独立保存视频远端取消结果失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False


async def _ensure_video_cancellation_recovery_independently(
    job_id: str,
    provider_task_id: str,
) -> bool:
    """用独立事务确认或补建 pending 取消债务。"""

    from app.core.database import async_session_factory
    from app.services.video_remote_cancellation import (
        ensure_pending_provider_cancellation,
    )

    for attempt in range(1, 3):
        try:
            async with async_session_factory() as recovery_db:
                recovered = await ensure_pending_provider_cancellation(
                    recovery_db,
                    job_id,
                    provider_task_id,
                )
                if not recovered:
                    await recovery_db.rollback()
                    continue
                await recovery_db.commit()
                return True
        except (SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:  # noqa: BLE001 - 下一独立事务仍可恢复同一事实
            logger.warning(
                "独立建立视频取消恢复事实失败 job_id=%s task_id=%s "
                "attempt=%s error_type=%s",
                job_id,
                provider_task_id,
                attempt,
                exc.__class__.__name__,
            )
    return False


async def _require_video_cancellation_recovery_independently(
    job_id: str,
    provider_task_id: str,
) -> None:
    """确保独立 DB 恢复事实已落盘，否则抛出携 ID 的统一恢复信号。"""

    try:
        recovered = await _ensure_video_cancellation_recovery_independently(
            job_id,
            provider_task_id,
        )
    except SoftTimeLimitExceeded as soft_exc:
        raise _video_cancellation_recovery_error(provider_task_id) from soft_exc
    except TimeLimitExceeded:
        raise
    if not recovered:
        raise _video_cancellation_recovery_error(provider_task_id)


async def _repair_canceled_video_cancellation_recovery(
    db: Any,
    job_id: str,
    provider_task_id_hint: str | None = None,
) -> None:
    """让 canceled 任务的原消息重试能够修复缺失的远端取消债务。"""

    from app.services.video_remote_cancellation import (
        ensure_pending_provider_cancellation,
    )

    job = await _get_video_job(db, job_id)
    if job is None or getattr(job, "status", None) != "canceled":
        await db.rollback()
        return
    authority_consistent, authoritative_task_id = _provider_task_id_authority_from_job(job)
    if not authority_consistent:
        logger.error(
            "canceled 视频任务 Provider ID 权威来源冲突 job_id=%s",
            job_id,
        )
        await db.rollback()
        return
    provider_task_id = authoritative_task_id or provider_task_id_hint
    if provider_task_id is None:
        await db.rollback()
        return
    try:
        recovered = await ensure_pending_provider_cancellation(
            db,
            job_id,
            provider_task_id,
        )
        if not recovered:
            await db.rollback()
        else:
            await db.commit()
            return
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 原会话失败后切换独立事务
        logger.warning(
            "修复 canceled 视频取消事实失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="repair_cancellation_recovery",
        )
    await _require_video_cancellation_recovery_independently(
        job_id,
        provider_task_id,
    )


async def _is_video_job_canceled(job_id: str) -> bool:
    """使用独立会话读取本地终态，避免与进度持久化并发复用 AsyncSession。"""
    from sqlalchemy import select

    from app.core.database import async_session_factory
    from app.models.video_generation_job import VideoGenerationJob

    try:
        async with async_session_factory() as probe_db:
            status = (
                await probe_db.execute(
                    select(VideoGenerationJob.status).where(
                        VideoGenerationJob.job_id == job_id
                    )
                )
            ).scalar_one_or_none()
    except Exception:  # noqa: BLE001 - 探针失败不能把正常任务误判为已取消
        logger.warning("读取视频任务取消状态失败 job_id=%s", job_id, exc_info=True)
        return False
    return status == "canceled"


async def _input_version_is_current(db: Any, job: Any) -> bool:
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.episode import Episode

    episode = (
        await db.execute(
            select(Episode)
            .where(Episode.episode_id == job.episode_id)
            .options(noload(Episode.script), noload(Episode.scenes))
        )
    ).scalar_one_or_none()
    return bool(episode and int(episode.episode_version or 1) == int(job.input_version or 1))


def _safe_error_type(exc: Exception, fallback: str = "provider_or_storage_error") -> str:
    if isinstance(exc, TimeoutError):
        return "provider_timeout"
    if isinstance(exc, RuntimeError) and str(exc) in _PROVIDER_MEDIA_LIMIT_ERRORS:
        return "provider_media_invalid"
    return fallback


def _safe_error_message(error_type: str) -> str:
    return error_type


async def _delete_generated_output(
    storage_key: str | None,
    *,
    storage: Any | None = None,
) -> bool:
    """删除未形成成功任务的视频/帧；失败时写入持久化清理任务。"""
    if not storage_key:
        return True
    from app.providers.storage import get_storage

    try:
        target = storage or get_storage()
        await target.delete(storage_key)
        return True
    except Exception:  # noqa: BLE001 - 删除失败必须转入持久化补偿
        digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
        logger.warning(
            "清理失败视频产物，转入媒体存储补偿 storage_key_digest=%s",
            digest[:16],
        )

    try:
        from app.core.database import async_session_factory
        from app.services.media_storage_jobs import enqueue_delete

        async with async_session_factory() as compensation_db:
            await enqueue_delete(
                compensation_db,
                idempotency_key=f"video-delete:{digest}",
                source_key=storage_key,
            )
            await compensation_db.commit()
        return True
    except Exception:  # noqa: BLE001 - 不覆盖原始任务终态
        logger.exception("写入视频产物删除补偿失败")
        return False


async def _fail_video_with_release(
    db: Any,
    job_id: str,
    lease_token: str,
    reservation_id: str | None,
    *,
    actor_user_id: str | None,
    error_type: str,
    retryable: bool,
    error_message: str | None = None,
    output_json: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    from app.crud import video as video_crud

    if not await video_crud.heartbeat_video_job(db, job_id, lease_token):
        await db.rollback()
        return

    release_failed = bool(reservation_id and _credits_release is None)
    if reservation_id and _credits_release is not None:
        try:
            await _credits_release(
                db,
                reservation_id=reservation_id,
                reason=error_type,
                actor_user_id=actor_user_id,
                commit=False,
            )
        except Exception:
            logger.exception("视频任务失败后释放积分失败 job_id=%s", job_id)
            await db.rollback()
            release_failed = True
            if not await video_crud.heartbeat_video_job(db, job_id, lease_token):
                await db.rollback()
                return
            # rollback 会撤销本事务内的前驱终态前修改；重新读取权威行并继续。
    final_error_type = (
        "financial_cleanup"
        if release_failed or error_type == "financial_cleanup"
        else error_type
    )
    final_error_message = (
        error_message
        if error_message and final_error_type == error_type
        else _safe_error_message(final_error_type)
    )
    finished = await video_crud.finish_video_job(
        db,
        job_id,
        lease_token,
        "failed",
        output_json=output_json,
        metrics=metrics,
        error_type=final_error_type,
        error_message=final_error_message,
        retryable=retryable or release_failed or final_error_type == "financial_cleanup",
    )
    if finished:
        await _resolve_continuation_dependents(
            db,
            job_id,
            last_frame_storage_key=None,
            succeeded=False,
        )
        await db.commit()
    else:
        await db.rollback()


async def _resolve_continuation_dependents(
    db: Any,
    predecessor_job_id: str,
    *,
    last_frame_storage_key: str | None,
    succeeded: bool,
) -> int:
    """在前驱终态事务中唤醒或失败所有等待子任务。

    子任务等待期间没有 Worker lease，也没有 Outbox 记录；成功时才事务性创建
    ``video.generate`` Outbox。失败路径以任务行锁和 reservation 状态保证只释放一次。
    """
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.crud import video as video_crud
    from app.models.shot_group import ShotGroup
    from app.models.video_generation_job import VideoGenerationJob
    from app.services.task_dispatch import enqueue_task

    predecessor = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == predecessor_job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if predecessor is None:
        return 0
    predecessor_group = await db.get(ShotGroup, predecessor.shot_group_id)
    if predecessor_group is None:
        return 0
    dependents = list(
        (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.predecessor_job_id == predecessor_job_id,
                    VideoGenerationJob.status == "waiting_dependency",
                )
                .options(noload(VideoGenerationJob.script))
                .order_by(VideoGenerationJob.created_at.asc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    resolved = 0
    for child in dependents:
        child_group = await db.get(ShotGroup, child.shot_group_id)
        if (
            child_group is None
            or child.script_id != predecessor.script_id
            or child.episode_id != predecessor.episode_id
            or child.scene_no != predecessor.scene_no
            or predecessor_group.script_id != child.script_id
            or predecessor_group.episode_id != child.episode_id
            or predecessor_group.scene_no != child.scene_no
            or int(child_group.group_index or 0) <= 1
            or int(predecessor_group.group_index or 0)
            != int(child_group.group_index or 0) - 1
        ):
            continue
        dependency = dict((child.input_json or {}).get("continuation_dependency") or {})
        frozen_predecessor_group_id = dependency.get("predecessor_shot_group_id")
        if frozen_predecessor_group_id not in {None, predecessor_group.shot_group_id}:
            continue
        frozen_fallback = dependency.get("static_fallback_frame")
        static_fallback = (
            frozen_fallback.strip()
            if isinstance(frozen_fallback, str) and frozen_fallback.strip()
            else None
        )
        if static_fallback is None:
            # 兼容上线前已处于 waiting_dependency 的任务。解析仅查看当前组
            # 静态帧与同场景直接前驱的静态尾帧，绝不读取旧视频 Job 产物。
            from app.services.video_generation import _resolve_continuation_static_fallback

            static_fallback = await _resolve_continuation_static_fallback(db, child_group)
        real_tail = (
            last_frame_storage_key.strip()
            if succeeded
            and isinstance(last_frame_storage_key, str)
            and last_frame_storage_key.strip()
            else None
        )
        resolved_first_frame = real_tail or static_fallback
        if resolved_first_frame:
            frame_source = "real_tail" if real_tail is not None else "static_fallback"
            activated = await video_crud.activate_waiting_dependency_job(
                db,
                child.job_id,
                predecessor_job_id=predecessor_job_id,
                first_frame=resolved_first_frame,
                frame_source=frame_source,
                predecessor_terminal_succeeded=succeeded,
            )
            if activated is None:
                continue
            reference_frames = dict(child_group.reference_frames or {})
            reference_frames["first_frame"] = resolved_first_frame
            child_group.reference_frames = reference_frames
            dispatch = await enqueue_task(
                db,
                task_type="video.generate",
                job_id=child.job_id,
                attempt=child.attempt,
                payload={
                    "job_id": child.job_id,
                    "script_id": child.script_id,
                    "episode_id": child.episode_id,
                    "shot_group_id": child.shot_group_id,
                    "input_version": child.input_version,
                },
            )
            child.dispatch_id = dispatch.id
            resolved += 1
            continue

        reservation_id = child.reservation_id or (child.input_json or {}).get("reservation_id")
        release_failed = bool(reservation_id and _credits_release is None)
        if reservation_id and _credits_release is not None:
            try:
                await _credits_release(
                    db,
                    reservation_id=reservation_id,
                    reason="dependency_unavailable",
                    actor_user_id=child.owner_user_id,
                    commit=False,
                )
            except Exception:  # noqa: BLE001 - 不得回滚已完成的前驱终态
                logger.exception(
                    "续帧依赖不可用后释放积分失败 child_job_id=%s",
                    child.job_id,
                )
                # credits.release 在 commit=False 时不会自行回滚；与既有视频失败
                # 闭环一致，标财务清理债务，由 reconciler 按 reservation 权威事实接管。
                release_failed = True
        failed = await video_crud.fail_waiting_dependency_job(
            db,
            child.job_id,
            predecessor_job_id=predecessor_job_id,
            release_failed=release_failed,
        )
        if failed is not None:
            resolved += 1
            # 多级续帧链必须逐层终止，不能让孙任务永久停在 waiting_dependency。
            await _resolve_continuation_dependents(
                db,
                child.job_id,
                last_frame_storage_key=None,
                succeeded=False,
            )
    return resolved


async def _reconcile_waiting_video_dependency(
    db: Any,
    child_job_id: str,
) -> bool:
    """补偿一个 waiting_dependency，只信任同场景、group_index-1 的前驱任务。"""

    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.crud import video as video_crud
    from app.models.shot_group import ShotGroup
    from app.models.video_generation_job import VideoGenerationJob

    child = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == child_job_id,
                VideoGenerationJob.status == "waiting_dependency",
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if child is None:
        return False
    child_group = await db.get(ShotGroup, child.shot_group_id)
    if child_group is None or int(child_group.group_index or 0) <= 1:
        # 缺组或首组的历史异常无法在扫描中安全推断，保守保持等待。
        return False
    direct_group = (
        await db.execute(
            select(ShotGroup).where(
                ShotGroup.script_id == child.script_id,
                ShotGroup.episode_id == child.episode_id,
                ShotGroup.scene_no == child.scene_no,
                ShotGroup.group_index == int(child_group.group_index) - 1,
            )
        )
    ).scalar_one_or_none()
    if direct_group is None:
        return False

    dependency = dict((child.input_json or {}).get("continuation_dependency") or {})
    frozen_predecessor_group_id = dependency.get("predecessor_shot_group_id")
    if frozen_predecessor_group_id not in {None, direct_group.shot_group_id}:
        # 冻结拓扑与当前严格前驱冲突时不猜测、不越过中间镜头组。
        return False

    predecessor: VideoGenerationJob | None = None
    if child.predecessor_job_id is not None:
        predecessor = (
            await db.execute(
                select(VideoGenerationJob)
                .where(VideoGenerationJob.job_id == child.predecessor_job_id)
                .options(noload(VideoGenerationJob.script))
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if (
            predecessor is None
            or predecessor.script_id != child.script_id
            or predecessor.episode_id != child.episode_id
            or predecessor.scene_no != child.scene_no
            or predecessor.shot_group_id != direct_group.shot_group_id
        ):
            return False
    else:
        predecessor = (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.script_id == child.script_id,
                    VideoGenerationJob.episode_id == child.episode_id,
                    VideoGenerationJob.scene_no == child.scene_no,
                    VideoGenerationJob.shot_group_id == direct_group.shot_group_id,
                )
                .options(noload(VideoGenerationJob.script))
                .order_by(
                    VideoGenerationJob.attempt.desc(),
                    VideoGenerationJob.created_at.desc(),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if predecessor is None:
            # 批量乱序创建仍可能尚未创建前驱任务，下一轮再判定。
            return False
        child = await video_crud.bind_waiting_dependency_job(
            db,
            child.job_id,
            predecessor_job_id=predecessor.job_id,
            predecessor_shot_group_id=direct_group.shot_group_id,
        )
        if child is None:
            return False

    if predecessor.status not in {"succeeded", "failed", "canceled"}:
        return False
    tail = (predecessor.output_json or {}).get("last_frame_storage_key")
    last_frame_storage_key = tail.strip() if isinstance(tail, str) and tail.strip() else None
    return bool(
        await _resolve_continuation_dependents(
            db,
            predecessor.job_id,
            last_frame_storage_key=last_frame_storage_key,
            succeeded=predecessor.status == "succeeded",
        )
    )


async def reconcile_waiting_video_dependencies(
    db: Any,
    *,
    limit: int = 50,
) -> list[str]:
    """周期扫描依赖等待任务；每个 child 独立事务，单行异常不阻塞整批。"""

    from app.crud import video as video_crud

    if not callable(getattr(db, "execute", None)):
        return []
    candidates = await video_crud.list_waiting_dependency_jobs(db, limit=limit)
    candidate_ids = [row.job_id for row in candidates]
    # 释放列表查询事务和 ORM 快照；后续每个 child 都重新判权并提交。
    await db.rollback()
    recovered: list[str] = []
    for child_job_id in candidate_ids:
        try:
            if await _reconcile_waiting_video_dependency(db, child_job_id):
                await db.commit()
                recovered.append(child_job_id)
            else:
                await db.rollback()
        except Exception as exc:  # noqa: BLE001 - 下一轮继续补偿，不扩大故障面
            await _best_effort_rollback(
                db,
                job_id=child_job_id,
                stage="waiting_dependency_reconcile",
            )
            logger.exception(
                "续帧依赖补偿失败，保留等待状态 child_job_id=%s error_type=%s",
                child_job_id,
                exc.__class__.__name__,
            )
    return recovered


async def _resolve_continuation_dependents_best_effort(
    db: Any,
    predecessor_job_id: str,
    *,
    last_frame_storage_key: str | None,
    succeeded: bool,
) -> int:
    """非关键后置处理；失败不得把前驱已确定的终态反转。

    Worker 测试可注入只实现提交/回滚的最小会话，此时没有可查询的依赖行，
    直接跳过。真实会话异常则记录风险并交给依赖补偿扫描处理；调用方仍可提交
    已完成的视频终态与积分结算。
    """

    if not callable(getattr(db, "execute", None)):
        logger.debug(
            "视频会话不支持依赖查询，跳过续帧后置处理 job_id=%s",
            predecessor_job_id,
        )
        return 0
    try:
        resolved = await _resolve_continuation_dependents(
            db,
            predecessor_job_id,
            last_frame_storage_key=last_frame_storage_key,
            succeeded=succeeded,
        )
        await db.commit()
        return resolved
    except Exception as exc:  # noqa: BLE001 - 前驱终态和已结算产物优先
        await _best_effort_rollback(
            db,
            job_id=predecessor_job_id,
            stage="continuation_dependency_followup",
        )
        logger.exception(
            "续帧依赖后置处理失败，保留前驱终态等待补偿 job_id=%s "
            "succeeded=%s error_type=%s",
            predecessor_job_id,
            succeeded,
            exc.__class__.__name__,
        )
        return 0


async def run_video_generation_once(
    db: Any,
    job_id: str,
    *,
    owner: str,
    provider: Any | None = None,
    extract_last_frame: Callable[[Any, str, str, str], Any] | None = None,
    storage: Any | None = None,
    recovery_provider_task_id: str | None = None,
    checkpoint_holder: _VideoTaskCheckpointHolder | None = None,
) -> None:
    """执行一次视频任务；测试可注入 fake provider / 抽帧函数。"""
    from app.crud import video as video_crud
    from app.providers.video_gen import (
        VideoGenerationCancelled,
        VideoGenerationRequest,
        get_video_provider,
    )
    from app.services.task_dispatch import cancel_outbox_task

    checkpoint_state = checkpoint_holder or _VideoTaskCheckpointHolder()
    claim = None
    job: Any | None = None
    reservation_id: str | None = None
    video_storage_key: str | None = None
    last_frame_storage_key: str | None = None
    storage_attempt_id: str | None = None
    shot_group_id: str | None = None
    script_id: str | None = None
    provider_task_id: str | None = None
    task_checkpoint_observed = False
    submission_intent_persisted = False
    provider_generation_started = False
    provider_generation_completed = False
    final_commit_attempted = False
    try:
        claim = await video_crud.claim_video_job(
            db,
            job_id,
            owner=owner,
            lease_seconds=_LEASE_SECONDS,
            allow_stale_processing=False,
        )
        await db.commit()
        if claim is None:
            await _repair_canceled_video_cancellation_recovery(
                db,
                job_id,
                recovery_provider_task_id,
            )
            return
        job = await _get_video_job(db, job_id)
        if job is None:
            raise RuntimeError("video_job_missing_after_claim")
        input_json = job.input_json or {}
        output_json = job.output_json or {}
        actor_user_id = job.owner_user_id
        job_provider_key = job.provider_key
        unit_id = job.unit_id
        script_id = getattr(job, "script_id", None)
        shot_group_id = getattr(job, "shot_group_id", None)
        if not shot_group_id and str(unit_id or "").startswith("group:"):
            shot_group_id = str(unit_id).removeprefix("group:") or None
        reservation_id = job.reservation_id or input_json.get("reservation_id")
        if not await _input_version_is_current(db, job):
            await _fail_video_with_release(
                db,
                job_id,
                claim.lease_token,
                reservation_id,
                actor_user_id=actor_user_id,
                error_type="stale_input_version",
                retryable=False,
            )
            return
        first_frame = await _resolve_execution_first_frame(db, job, input_json)
        provider_task_id = _provider_task_id_from_job(job)
        storage_attempt_id = _storage_attempt_id(claim.lease_token)
        provider = provider or get_video_provider(job_provider_key)
        request_metadata = {"storage_attempt_id": storage_attempt_id}
        if provider_task_id:
            request_metadata["provider_task_id"] = provider_task_id

        async def _on_task_created(created_task_id: str) -> None:
            nonlocal provider_task_id, task_checkpoint_observed
            # 远端 ID 一旦可见，必须先保存在当前栈帧，任何后续 await 才有恢复依据。
            provider_task_id = created_task_id
            checkpoint_state.observe(created_task_id, claim.lease_token)
            try:
                await _ensure_video_task_checkpoint_durable(
                    job_id,
                    created_task_id,
                    claim.lease_token,
                )
            except BaseException as exc:
                checkpoint_state.mark_error(exc)
                raise
            checkpoint_state.mark_durable(created_task_id, claim.lease_token)
            task_checkpoint_observed = True

        async def _persist_submission_state(state: str) -> None:
            persisted = await _persist_provider_submission_state_independently(
                job_id,
                claim.lease_token,
                state=state,
                provider_key=job_provider_key,
                attempt=claim.attempt,
            )
            if not persisted:
                raise RuntimeError("video_submission_state_unavailable")

        async def _before_submit() -> None:
            nonlocal submission_intent_persisted
            await _persist_submission_state("submit_intent")
            submission_intent_persisted = True

        async def _on_submission_uncertain() -> None:
            await _persist_submission_state("submission_uncertain")

        request = VideoGenerationRequest(
            prompt=input_json.get("prompt", ""),
            reference_images=input_json.get("reference_images", []) or [],
            reference_image_names=input_json.get("reference_image_names", []) or [],
            reference_audios=input_json.get("reference_audios", []) or [],
            reference_audio_names=input_json.get("reference_audio_names", []) or [],
            model=input_json.get("model") or settings.video_gen_model or settings.seedance_model,
            job_id=job_id,
            first_frame=first_frame,
            duration_seconds=input_json.get("duration_seconds"),
            metadata=request_metadata,
            cancel_requested=lambda: _is_video_job_canceled(job_id),
            on_task_created=_on_task_created,
            before_submit=_before_submit,
            on_submission_uncertain=_on_submission_uncertain,
        )
        progress_queue: asyncio.Queue[Any] = asyncio.Queue()
        progress_stop = object()
        progress_errors: list[Exception] = []

        async def _consume_progress() -> None:
            while True:
                progress = await progress_queue.get()
                try:
                    if progress is progress_stop:
                        return
                    if not progress_errors:
                        await _persist_provider_progress(
                            db, job_id, claim.lease_token, progress
                        )
                except Exception as exc:  # noqa: BLE001 - 主流程统一清理产物和积分
                    progress_errors.append(exc)
                finally:
                    progress_queue.task_done()

        progress_consumer = asyncio.create_task(_consume_progress())

        def _on_progress(progress) -> None:
            progress_queue.put_nowait(progress)

        try:
            provider_generation_started = True
            result = await provider.generate(request, _on_progress)
            provider_generation_completed = True
            # Provider 已返回即代表 attempt 对象可能已经落盘；进度事务失败也必须
            # 保留对象键和远端任务 ID，供外层判权与持久清理。
            video_storage_key = result.video_storage_key
            provider_task_id = result.provider_job_id or provider_task_id
            await progress_queue.join()
        finally:
            progress_queue.put_nowait(progress_stop)
            await progress_consumer
        if progress_errors:
            raise progress_errors[0]
        if not await video_crud.heartbeat_video_job(db, job_id, claim.lease_token):
            await db.rollback()
            await _delete_lost_lease_outputs(
                storage_attempt_id,
                video_storage_key,
                storage=storage,
            )
            return
        current_job = await _get_video_job(db, job_id)
        if current_job is None:
            raise RuntimeError("video_job_missing_after_generation")
        attempt_cleanup_debts = (current_job.output_json or {}).get(
            _ATTEMPT_CLEANUP_DEBT_KEY
        )
        if not isinstance(attempt_cleanup_debts, dict):
            attempt_cleanup_debts = {}
        output_json = {
            "video_storage_key": result.video_storage_key,
            "reference_image_names": request.reference_image_names,
            "model": request.model,
        }
        if attempt_cleanup_debts:
            output_json[_ATTEMPT_CLEANUP_DEBT_KEY] = dict(attempt_cleanup_debts)
        if provider_task_id:
            output_json["provider_task_id"] = provider_task_id
        metrics = {
            "model": request.model,
            "provider_key": job_provider_key,
            "reference_count": len(request.reference_images),
            **(result.metrics or {}),
        }
        if provider_task_id:
            metrics["provider_task_id"] = provider_task_id
        if _credits_settle is not None and reservation_id:
            try:
                await _credits_settle(
                    db,
                    reservation_id=reservation_id,
                    actual=settings.price_video_gen,
                    actor_user_id=actor_user_id,
                    commit=False,
                )
            except (SoftTimeLimitExceeded, TimeLimitExceeded):
                raise
            except Exception:
                await db.rollback()
                output_cleaned = await _delete_generated_output(
                    video_storage_key,
                    storage=storage,
                )
                if not output_cleaned:
                    scheduled = await _schedule_video_attempt_cleanup(
                        job_id=job_id,
                        script_id=script_id,
                        shot_group_id=shot_group_id,
                        storage_attempt_id=storage_attempt_id,
                        video_storage_key=video_storage_key,
                        last_frame_storage_key=None,
                        provider_task_id=provider_task_id,
                    )
                    if not scheduled:
                        raise RuntimeError("video_attempt_cleanup_unavailable")
                await _fail_video_with_release(
                    db,
                    job_id,
                    claim.lease_token,
                    reservation_id,
                    actor_user_id=actor_user_id,
                    error_type="financial_cleanup",
                    retryable=True,
                )
                return
        try:
            if extract_last_frame is None:
                last_frame_storage_key = await _update_last_frame(
                    db,
                    unit_id,
                    result.video_storage_key,
                    storage_attempt_id,
                )
            else:
                maybe = extract_last_frame(db, unit_id, result.video_storage_key, claim.lease_token)
                if hasattr(maybe, "__await__"):
                    maybe = await maybe
                if isinstance(maybe, str):
                    last_frame_storage_key = maybe
        except (SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:
            failed_frame_key = getattr(exc, "last_frame_storage_key", None)
            if not isinstance(failed_frame_key, str):
                # 末帧是镜头衔接的辅助素材，视频对象已成功生成时不能因提帧失败
                # 而丢弃视频或释放已结算积分。下一镜缺少首帧时会按既有逻辑降级生成。
                logger.warning(
                    "视频末帧提取失败，保留已生成视频 job_id=%s unit_id=%s",
                    job_id,
                    unit_id,
                    exc_info=True,
                )
            else:
                # 已上传末帧但更新引用失败时，必须沿用失败闭环，防止产生无法归属的对象。
                failed_frame_cleanup_state = getattr(
                    exc,
                    "last_frame_cleanup_state",
                    None,
                )
                last_frame_storage_key = failed_frame_key
                rolled_back = await _best_effort_rollback(
                    db,
                    job_id=job_id,
                    stage="frame_extraction",
                )
                if rolled_back:
                    video_cleaned = await _delete_generated_output(
                        video_storage_key,
                        storage=storage,
                    )
                    if not video_cleaned:
                        schedule_kwargs: dict[str, Any] = {
                            "job_id": job_id,
                            "script_id": script_id,
                            "shot_group_id": shot_group_id,
                            "storage_attempt_id": storage_attempt_id,
                            "video_storage_key": video_storage_key,
                            "last_frame_storage_key": None,
                        }
                        if provider_task_id is not None:
                            schedule_kwargs["provider_task_id"] = provider_task_id
                        scheduled = await _schedule_video_attempt_cleanup(**schedule_kwargs)
                        if not scheduled:
                            raise RuntimeError("video_attempt_cleanup_unavailable")
                else:
                    video_cleanup_state = await _cleanup_or_schedule_video_attempt_outputs(
                        job_id=job_id,
                        script_id=script_id,
                        shot_group_id=shot_group_id,
                        storage_attempt_id=storage_attempt_id,
                        video_storage_key=video_storage_key,
                        last_frame_storage_key=None,
                        provider_task_id=provider_task_id,
                        storage=storage,
                    )
                    if video_cleanup_state is True:
                        return
                if failed_frame_cleanup_state is None:
                    await _cleanup_or_schedule_video_attempt_outputs(
                        job_id=job_id,
                        script_id=script_id,
                        shot_group_id=shot_group_id,
                        storage_attempt_id=storage_attempt_id,
                        video_storage_key=None,
                        last_frame_storage_key=last_frame_storage_key,
                        provider_task_id=provider_task_id,
                        storage=storage,
                    )
                await _fail_video_with_release(
                    db,
                    job_id,
                    claim.lease_token,
                    reservation_id,
                    actor_user_id=actor_user_id,
                    error_type="frame_extraction_failed",
                    retryable=False,
                )
                return
        finished = await video_crud.finish_video_job(
            db,
            job_id,
            claim.lease_token,
            "succeeded",
            output_json={
                **output_json,
                **(
                    {"last_frame_storage_key": last_frame_storage_key}
                    if last_frame_storage_key
                    else {}
                ),
            },
            metrics=metrics,
        )
        if not finished:
            await _best_effort_rollback(
                db,
                job_id=job_id,
                stage="finish_rejected",
            )
            await _cleanup_or_schedule_video_attempt_outputs(
                job_id=job_id,
                script_id=script_id,
                shot_group_id=shot_group_id,
                storage_attempt_id=storage_attempt_id,
                video_storage_key=video_storage_key,
                last_frame_storage_key=last_frame_storage_key,
                provider_task_id=provider_task_id,
                storage=storage,
            )
            return
        final_commit_attempted = True
        await db.commit()
        await _resolve_continuation_dependents_best_effort(
            db,
            job_id,
            last_frame_storage_key=last_frame_storage_key,
            succeeded=True,
        )
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="time_limit",
        )
        if storage_attempt_id is not None and (
            video_storage_key is not None or last_frame_storage_key is not None
        ):
            try:
                await _cleanup_or_schedule_video_attempt_outputs(
                    job_id=job_id,
                    script_id=script_id,
                    shot_group_id=shot_group_id,
                    storage_attempt_id=storage_attempt_id,
                    video_storage_key=video_storage_key,
                    last_frame_storage_key=last_frame_storage_key,
                    provider_task_id=provider_task_id,
                    storage=storage,
                )
            except Exception:  # noqa: BLE001 - Celery 必须收到原始 time limit 信号
                logger.exception(
                    "视频任务超时后的产物恢复通道建立失败 job_id=%s",
                    job_id,
                )
        raise
    except VideoTaskCheckpointConflict as exc:
        await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="task_checkpoint_conflict",
        )
        logger.critical(
            "视频任务 ID checkpoint 冲突，保留 processing lease "
            "job_id=%s task_id=%s",
            job_id,
            exc.provider_task_id,
        )
        return
    except VideoTaskCheckpointUnavailable as exc:
        await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="task_checkpoint_unavailable",
        )
        if claim is None:
            raise
        raise VideoTaskCheckpointRecoveryUnavailable(
            provider_task_id=exc.provider_task_id,
            lease_token=claim.lease_token,
        ) from exc
    except VideoGenerationSubmissionUncertain:
        await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="submission_uncertain",
        )
        if claim is None:
            raise
        try:
            await _persist_provider_submission_state_independently(
                job_id,
                claim.lease_token,
                state="submission_uncertain",
                provider_key=str(
                    getattr(provider, "provider_key", "")
                    or job_provider_key
                ),
                attempt=claim.attempt,
            )
        except (SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as persist_exc:  # noqa: BLE001 - submit_intent 已先行持久
            logger.error(
                "视频提交不确定状态补写失败，保留 processing lease "
                "job_id=%s error_type=%s",
                job_id,
                persist_exc.__class__.__name__,
            )
        logger.error(
            "视频远端提交响应不确定，禁止自动重提并保留冻结 job_id=%s",
            job_id,
        )
        try:
            reviewed = await video_crud.mark_video_submission_uncertain_for_manual_review(
                db,
                job_id,
                claim.lease_token,
                provider_key=job_provider_key,
                attempt=claim.attempt,
            )
            if reviewed:
                await cancel_outbox_task(
                    db,
                    task_type="video.generate",
                    job_id=job_id,
                    attempt=claim.attempt,
                )
                await db.commit()
                logger.warning("视频任务已转入人工核对 job_id=%s", job_id)
                return
            await db.rollback()
        except (SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception:  # noqa: BLE001 - stale reconciler 仍可接管历史遗留任务
            await _best_effort_rollback(
                db,
                job_id=job_id,
                stage="submission_uncertain_manual_review",
            )
            logger.exception("视频任务转人工核对失败 job_id=%s", job_id)
        return
    except VideoGenerationSubmitRejected as exc:
        diagnostics = sanitize_video_submit_rejection_diagnostics(exc.diagnostics)
        rolled_back = await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="submit_rejected",
        )
        if claim is None:
            raise
        provider_key = str(
            getattr(provider, "provider_key", "")
            or job_provider_key
        )
        evidence: dict[str, Any] = {
            "state": "submit_rejected",
            "provider_key": provider_key,
            "attempt": claim.attempt,
        }
        if diagnostics:
            evidence["diagnostics"] = diagnostics
        # 仅透传已白名单验证的供应商错误码；SDK 原始内容可能反射提示词、媒体 URL 或凭据。
        status_code = diagnostics.get("status_code")
        public_error_message = str(
            diagnostics.get("code")
            or (f"ProviderHttp{status_code}" if isinstance(status_code, int) else "provider_submit_rejected")
        )
        logger.warning(
            "视频远端提交被明确拒绝 job_id=%s provider_key=%s attempt=%s "
            "status_code=%s content_type=%s code=%s param=%s provider_error_type=%s request_id=%s",
            job_id,
            provider_key,
            claim.attempt,
            diagnostics.get("status_code", ""),
            diagnostics.get("content_type", ""),
            diagnostics.get("code", ""),
            diagnostics.get("param", ""),
            diagnostics.get("type", ""),
            diagnostics.get("request_id", ""),
            extra={
                "event": "video_provider_submit_rejected",
                "provider_diagnostics": diagnostics,
            },
        )
        terminal_state = {"provider_submission": evidence}
        if rolled_back:
            await _mark_failed_and_release_once(
                db,
                job_id,
                claim.lease_token,
                "provider_submit_rejected",
                error_message=public_error_message,
                output_json=terminal_state,
                metrics=terminal_state,
            )
        else:
            await _mark_failed_and_release(
                job_id,
                claim.lease_token,
                "provider_submit_rejected",
                error_message=public_error_message,
                output_json=terminal_state,
                metrics=terminal_state,
            )
        return
    except VideoGenerationRemoteTerminalError as exc:
        rolled_back = await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="provider_remote_terminal",
        )
        if claim is None:
            raise
        if rolled_back:
            authority_known, authoritative_task_id = await _read_authoritative_provider_task_id(
                db,
                job_id,
            )
        else:
            authority_known, authoritative_task_id = (
                await _read_authoritative_provider_task_id_independently(job_id)
            )
        if not authority_known or authoritative_task_id != exc.provider_task_id:
            logger.error(
                "视频远端终态无法匹配本地权威 ID，保留 processing lease "
                "job_id=%s task_id=%s",
                job_id,
                exc.provider_task_id,
            )
            return
        evidence = {
            "provider_task_id": exc.provider_task_id,
            "status": exc.terminal_status,
        }
        terminal_output = {
            "provider_task_id": exc.provider_task_id,
            _PROVIDER_REMOTE_TERMINAL_KEY: evidence,
        }
        terminal_metrics = dict(terminal_output)
        if rolled_back:
            await _mark_failed_and_release_once(
                db,
                job_id,
                claim.lease_token,
                "provider_remote_terminal",
                output_json=terminal_output,
                metrics=terminal_metrics,
            )
        else:
            await _mark_failed_and_release(
                job_id,
                claim.lease_token,
                "provider_remote_terminal",
                output_json=terminal_output,
                metrics=terminal_metrics,
            )
        return
    except VideoGenerationCancelled as exc:
        rolled_back = await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="canceled",
        )
        if exc.result.provider_task_id:
            persisted = False
            try:
                if not rolled_back:
                    persisted = await _persist_video_cancellation_result_independently(
                        job_id,
                        exc.result,
                    )
                else:
                    from app.services.video_remote_cancellation import (
                        persist_provider_cancellation_result,
                    )

                    written = await persist_provider_cancellation_result(
                        db,
                        job_id,
                        exc.result,
                    )
                    if written:
                        await db.commit()
                        persisted = True
                    else:
                        await _best_effort_rollback(
                            db,
                            job_id=job_id,
                            stage="persist_cancellation_rejected",
                        )
            except SoftTimeLimitExceeded as soft_exc:
                raise _video_cancellation_recovery_error(
                    exc.result.provider_task_id
                ) from soft_exc
            except TimeLimitExceeded:
                raise
            except Exception as persist_exc:  # noqa: BLE001 - 先保住恢复事实，禁止清理
                logger.warning(
                    "保存视频远端取消结果失败 job_id=%s task_id=%s error_type=%s",
                    job_id,
                    exc.result.provider_task_id,
                    persist_exc.__class__.__name__,
                )
                if rolled_back:
                    await _best_effort_rollback(
                        db,
                        job_id=job_id,
                        stage="persist_cancellation_failed",
                    )
            if not persisted:
                logger.warning(
                    "视频远端取消结果未写入 canceled 终态 job_id=%s task_id=%s",
                    job_id,
                    exc.result.provider_task_id,
                )
                await _require_video_cancellation_recovery_independently(
                    job_id,
                    exc.result.provider_task_id,
                )
                return
        if storage_attempt_id is not None:
            await _cleanup_or_schedule_video_attempt_outputs(
                job_id=job_id,
                script_id=script_id,
                shot_group_id=shot_group_id,
                storage_attempt_id=storage_attempt_id,
                video_storage_key=video_storage_key,
                last_frame_storage_key=last_frame_storage_key,
                provider_task_id=exc.result.provider_task_id or provider_task_id,
                storage=storage,
            )
        logger.info(
            "视频任务检测到本地取消，停止 Provider 等待 job_id=%s task_id=%s outcome=%s",
            job_id,
            exc.result.provider_task_id,
            exc.result.outcome.value,
        )
        return
    except Exception as exc:
        rolled_back = await _best_effort_rollback(
            db,
            job_id=job_id,
            stage="unhandled_error",
        )
        if claim is None:
            raise
        if provider_generation_started:
            if submission_intent_persisted:
                if rolled_back:
                    submission_unresolved = await _read_video_submission_unresolved(
                        db,
                        job_id,
                    )
                else:
                    submission_unresolved = (
                        await _read_video_submission_unresolved_independently(job_id)
                    )
                if submission_unresolved is None:
                    logger.error(
                        "视频 Provider 异常后无法判定提交边界，保留 processing lease "
                        "job_id=%s",
                        job_id,
                    )
                    return
                if submission_unresolved:
                    logger.error(
                        "视频 Provider 异常发生于未决提交后，禁止失败补偿与重提 "
                        "job_id=%s",
                        job_id,
                    )
                    return
            if rolled_back:
                authority_known, authoritative_task_id = (
                    await _read_authoritative_provider_task_id(db, job_id)
                )
            else:
                authority_known, authoritative_task_id = (
                    await _read_authoritative_provider_task_id_independently(job_id)
                )
            if not authority_known:
                logger.error(
                    "视频 Provider 异常后无法判定远端任务，保留 processing 租约 job_id=%s",
                    job_id,
                )
                return
            if task_checkpoint_observed and provider_task_id is not None:
                if authoritative_task_id is None:
                    logger.warning(
                        "视频任务 ID 仅存在于专用 broker checkpoint，等待落库后恢复 "
                        "job_id=%s task_id=%s",
                        job_id,
                        provider_task_id,
                    )
                    return
                if authoritative_task_id != provider_task_id:
                    logger.critical(
                        "视频任务 ID 内存与数据库权威来源冲突，保留 processing lease "
                        "job_id=%s memory_task_id=%s database_task_id=%s",
                        job_id,
                        provider_task_id,
                        authoritative_task_id,
                    )
                    return
            provider_task_id = authoritative_task_id or provider_task_id
            if (
                not provider_generation_completed
                and provider_task_id
                and not await _cancel_provider_task_after_generation_failure(
                    provider,
                    provider_task_id,
                )
            ):
                return
        if storage_attempt_id is not None and (
            video_storage_key is not None or last_frame_storage_key is not None
        ):
            authoritative = await _cleanup_or_schedule_video_attempt_outputs(
                job_id=job_id,
                script_id=script_id,
                shot_group_id=shot_group_id,
                storage_attempt_id=storage_attempt_id,
                video_storage_key=video_storage_key,
                last_frame_storage_key=last_frame_storage_key,
                provider_task_id=provider_task_id,
                storage=storage,
            )
            if authoritative is True or (
                authoritative is None and final_commit_attempted
            ):
                return
        error_type = _safe_error_type(exc)
        if rolled_back:
            await _mark_failed_and_release_once(
                db,
                job_id,
                claim.lease_token,
                error_type,
            )
        else:
            await _mark_failed_and_release(
                job_id,
                claim.lease_token,
                error_type,
            )


async def _resolve_execution_first_frame(db: Any, job: Any, input_json: dict) -> str | None:
    """执行时重新解析首帧，避免创建时快照在批量任务中陈旧。"""
    from sqlalchemy import select

    from app.models.shot_group import ShotGroup
    from app.services.video_generation import _resolve_first_frame

    # 续帧任务必须使用与 predecessor_job_id 同事务冻结的真实尾帧，不能在排队后
    # 被前驱的更晚 attempt 或人工编辑悄悄替换。
    if input_json.get("is_continuation") is True:
        frozen_first_frame = input_json.get("first_frame")
        return frozen_first_frame if isinstance(frozen_first_frame, str) else None

    shot_group = (
        await db.execute(
            select(ShotGroup).where(ShotGroup.shot_group_id == job.shot_group_id)
        )
    ).scalar_one_or_none()
    if shot_group is None:
        return input_json.get("first_frame")
    latest = await _resolve_first_frame(db, shot_group)
    return latest or input_json.get("first_frame")


async def _persist_provider_progress(db: Any, job_id: str, lease_token: str, progress: Any) -> None:
    """持久化 provider 进度和提交后 task id，供崩溃恢复续轮询。"""
    from app.crud import video as video_crud

    provider_task_id = getattr(progress, "provider_task_id", None)
    output_json = None
    metrics = None
    if provider_task_id:
        current = await _get_video_job(db, job_id)
        output_json = dict(getattr(current, "output_json", None) or {})
        metrics = dict(getattr(current, "metrics", None) or {})
        output_json["provider_task_id"] = provider_task_id
        metrics["provider_task_id"] = provider_task_id
    updated = await video_crud.update_video_job_progress(
        db,
        job_id,
        progress=getattr(progress, "progress", None),
        progress_stage=getattr(progress, "stage", None),
        progress_message=getattr(progress, "message", None),
        output_json=output_json,
        metrics=metrics,
        lease_token=lease_token,
    )
    if not updated:
        await db.rollback()
        return
    if not await video_crud.heartbeat_video_job(db, job_id, lease_token):
        await db.rollback()
        return
    await db.commit()


async def _load_authoritative_video_reservation(db: Any, job_id: str):
    from sqlalchemy import select

    from app.models.credit_reservation import CreditReservation

    return (
        await db.execute(
            select(CreditReservation)
            .where(
                CreditReservation.source_type == "generation_job",
                CreditReservation.source_id == job_id,
            )
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _reconcile_one_video_financial_cleanup(db: Any, job_id: str) -> bool:
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "failed",
                VideoGenerationJob.error_type == "financial_cleanup",
                VideoGenerationJob.retryable.is_(True),
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if job is None:
        await db.rollback()
        return False

    # payload/reservation_id 可能在旧任务或失败回滚中丢失；source 映射才是
    # 全生命周期唯一的权威事实，禁止因载荷缺失直接清除财务债。
    reservation = await _load_authoritative_video_reservation(db, job.job_id)
    if reservation is not None and reservation.status == "reserved":
        if _credits_release is None:
            await db.rollback()
            return False
        await _credits_release(
            db,
            reservation_id=reservation.id,
            reason="video financial cleanup",
            actor_user_id=job.owner_user_id,
            commit=False,
        )
        # release() 未抛异常不等于状态已落库；flush 后按 source 重新读取权威事实。
        await db.flush()
        reservation = await _load_authoritative_video_reservation(db, job.job_id)
        if reservation is not None and reservation.status == "reserved":
            await db.rollback()
            return False

    job.retryable = False
    await db.commit()
    return True


async def _run_video_financial_cleanup_candidate(
    db: Any,
    job_id: str,
) -> tuple[bool, bool]:
    """返回（是否恢复、当前 session 是否仍可安全复用）。"""

    try:
        return await _reconcile_one_video_financial_cleanup(db, job_id), True
    except Exception as exc:  # noqa: BLE001 - 单任务失败不得污染后续事务
        logger.warning(
            "视频财务清理失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        try:
            await db.rollback()
            return False, True
        except Exception as rollback_exc:  # noqa: BLE001 - session 已不可复用
            logger.warning(
                "视频财务清理回滚失败 job_id=%s error_type=%s",
                job_id,
                rollback_exc.__class__.__name__,
            )
            return False, False


async def reconcile_video_financial_cleanup(
    db: Any,
    *,
    limit: int = 100,
    session_factory: Callable[[], Any] | None = None,
) -> int:
    """逐任务隔离地重试视频失败后的积分释放清理。"""
    from sqlalchemy import select

    from app.models.video_generation_job import VideoGenerationJob

    candidate_ids = list(
        (
            await db.execute(
                select(VideoGenerationJob.job_id)
                .where(
                    VideoGenerationJob.status == "failed",
                    VideoGenerationJob.error_type == "financial_cleanup",
                    VideoGenerationJob.retryable.is_(True),
                )
                .order_by(
                    VideoGenerationJob.created_at.asc(),
                    VideoGenerationJob.job_id.asc(),
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    # 候选扫描只负责避开其他 reconciler 已领取的行；逐任务重新加锁并单独提交。
    await db.rollback()
    recovered = 0
    for job_id in candidate_ids:
        if session_factory is None:
            resolved, healthy = await _run_video_financial_cleanup_candidate(db, job_id)
            recovered += int(resolved)
            if not healthy:
                break
            continue

        resolved = False
        try:
            async with session_factory() as task_db:
                resolved, _ = await _run_video_financial_cleanup_candidate(
                    task_db,
                    job_id,
                )
        except Exception as exc:  # noqa: BLE001 - context 关闭失败也必须隔离
            logger.warning(
                "关闭视频财务清理会话失败 job_id=%s error_type=%s",
                job_id,
                exc.__class__.__name__,
            )
        if resolved:
            recovered += 1
    return recovered


async def reconcile_video_remote_cancellations(db: Any, *, limit: int = 10) -> int:
    """分别重试旧代际 orphan 与 canceled 当前任务的远端取消 debt。"""
    from sqlalchemy import and_, or_, select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob
    from app.services.video_remote_cancellation import (
        persist_locked_orphan_provider_cancellation_result,
        persist_locked_provider_cancellation_result,
        retryable_orphan_provider_cancellation_expression,
        retryable_orphan_provider_cancellations,
        retryable_provider_cancellation_expression,
    )

    if limit <= 0:
        return 0
    candidate_ids = list(
        (
            await db.execute(
                select(VideoGenerationJob.job_id)
                .where(
                    or_(
                        retryable_orphan_provider_cancellation_expression(),
                        and_(
                            VideoGenerationJob.status == "canceled",
                            retryable_provider_cancellation_expression(),
                        ),
                    )
                )
                .order_by(VideoGenerationJob.updated_at.asc())
                .limit(limit)
            )
        ).scalars().all()
    )
    await db.rollback()
    recovered = 0
    for job_id in candidate_ids:
        orphan_job = (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.job_id == job_id,
                    retryable_orphan_provider_cancellation_expression(),
                )
                .options(noload(VideoGenerationJob.script))
                .execution_options(populate_existing=True)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()
        orphan_snapshot: list[tuple[str, str, dict[str, Any]]] = []
        if orphan_job is not None:
            valid, orphan_snapshot = retryable_orphan_provider_cancellations(
                orphan_job.output_json,
                orphan_job.metrics,
            )
            if not valid:
                logger.error(
                    "跳过结构冲突的视频 orphan 取消 debt job_id=%s",
                    job_id,
                )
                await db.rollback()
                continue
        await db.rollback()

        for generation, task_key, expected_record in orphan_snapshot:
            job = (
                await db.execute(
                    select(VideoGenerationJob)
                    .where(
                        VideoGenerationJob.job_id == job_id,
                        retryable_orphan_provider_cancellation_expression(),
                    )
                    .options(noload(VideoGenerationJob.script))
                    .execution_options(populate_existing=True)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if job is None:
                await db.rollback()
                continue
            valid, current_orphans = retryable_orphan_provider_cancellations(
                job.output_json,
                job.metrics,
            )
            current_record = next(
                (
                    record
                    for current_generation, current_task_key, record in current_orphans
                    if current_generation == generation and current_task_key == task_key
                ),
                None,
            )
            if not valid or current_record != expected_record:
                await db.rollback()
                continue
            provider_task_id = str(expected_record.get("provider_task_id") or "")
            try:
                result = await get_video_provider(job.provider_key).cancel(provider_task_id)
            except (SoftTimeLimitExceeded, TimeLimitExceeded):
                raise
            except Exception as exc:  # noqa: BLE001 - orphan debt 留待下轮恢复
                result = VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                    detail=f"远端 orphan 取消重试失败: {type(exc).__name__}",
                )
            if result.provider_task_id != provider_task_id:
                result = VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                    detail="远端 orphan 取消响应任务不匹配",
                )
            updated = await persist_locked_orphan_provider_cancellation_result(
                db,
                job,
                generation=generation,
                task_key=task_key,
                expected_record=expected_record,
                result=result,
            )
            if not updated:
                await db.rollback()
                continue
            if result.outcome is not VideoCancellationOutcome.FAILED:
                recovered += 1
            await db.commit()

        job = (
            await db.execute(
                select(VideoGenerationJob)
                .where(
                    VideoGenerationJob.job_id == job_id,
                    VideoGenerationJob.status == "canceled",
                    retryable_provider_cancellation_expression(),
                )
                .options(noload(VideoGenerationJob.script))
                .execution_options(populate_existing=True)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()
        if job is None:
            await db.rollback()
            continue
        authority_consistent, authoritative_task_id = _provider_task_id_authority_from_job(job)
        if not authority_consistent:
            logger.error(
                "跳过 Provider ID 权威来源冲突的视频取消 debt job_id=%s",
                job_id,
            )
            await db.rollback()
            continue
        provider_task_id = authoritative_task_id or ""
        if not provider_task_id:
            result = VideoCancellationResult(
                provider_task_id="",
                outcome=VideoCancellationOutcome.FAILED,
                detail="缺少远端任务 ID",
            )
        else:
            try:
                result = await get_video_provider(job.provider_key).cancel(
                    provider_task_id
                )
            except (SoftTimeLimitExceeded, TimeLimitExceeded):
                raise
            except Exception as exc:  # noqa: BLE001 - debt 必须保留供下轮恢复
                result = VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                    detail=f"远端取消重试失败: {type(exc).__name__}",
                )
        updated = await persist_locked_provider_cancellation_result(db, job, result)
        if not updated:
            await db.rollback()
            continue
        if result.outcome is not VideoCancellationOutcome.FAILED:
            recovered += 1
        await db.commit()
    return recovered


@app.task(
    name="app.workers.video_generation.reconcile_video_remote_cancellations",
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def reconcile_video_remote_cancellations_task(limit: int = 10) -> dict:
    """Celery 独立恢复入口；不复用 canceled 的 video.generate outbox。"""

    async def _run() -> int:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await reconcile_video_remote_cancellations(db, limit=limit)

    try:
        return {"recovered": run_worker_async(_run())}
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception:  # noqa: BLE001 - 单次调度失败由下一轮重新扫描 debt
        logger.exception("视频远端取消补偿失败")
        return {"recovered": 0}


@app.task(
    name="app.workers.video_generation.reconcile_video_financial_cleanup",
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def reconcile_video_financial_cleanup_task(limit: int = 100) -> dict:
    """Celery 可调度入口；beat 接线由共享调度配置负责。"""
    async def _run() -> int:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await reconcile_video_financial_cleanup(
                db,
                limit=limit,
                session_factory=async_session_factory,
            )

    try:
        recovered = run_worker_async(_run())
        return {"recovered": recovered}
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception:  # noqa: BLE001
        logger.exception("视频财务清理重试失败")
        return {"recovered": 0}


async def _mark_failed_and_release_once(
    db: Any,
    job_id: str,
    lease_token: str,
    error_type: str,
    *,
    error_message: str | None = None,
    output_json: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    """仅在原租约仍有效时释放积分并写入失败终态。"""
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.credit_reservation import CreditReservation
    from app.models.video_generation_job import VideoGenerationJob

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "processing",
                VideoGenerationJob.lease_token == lease_token,
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        await db.rollback()
        return
    reservation_id = job.reservation_id or (job.input_json or {}).get("reservation_id")
    if not reservation_id:
        reservation_id = (
            await db.execute(
                select(CreditReservation.id).where(
                    CreditReservation.source_type == "generation_job",
                    CreditReservation.source_id == job_id,
                    CreditReservation.status == "reserved",
                )
            )
        ).scalar_one_or_none()
    await _fail_video_with_release(
        db,
        job_id,
        lease_token,
        reservation_id,
        actor_user_id=job.owner_user_id,
        error_type=error_type,
        retryable=False,
        error_message=error_message,
        output_json=output_json,
        metrics=metrics,
    )


async def _mark_failed_and_release(
    job_id: str,
    lease_token: str,
    error_type: str,
    *,
    error_message: str | None = None,
    output_json: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    """在独立会话中按 lease token 执行失败补偿。"""
    from app.core.database import async_session_factory

    async with async_session_factory() as db:
        await _mark_failed_and_release_once(
            db,
            job_id,
            lease_token,
            error_type,
            error_message=error_message,
            output_json=output_json,
            metrics=metrics,
        )


async def _download_storage_file(storage: Any, storage_key: str, local_path: str) -> None:
    """生产存储分块下载；旧测试 fake 缺少流式方法时保持兼容。"""

    limit = int(settings.max_media_read_bytes)
    get_size = getattr(storage, "get_size", None)
    if callable(get_size):
        reported_size = int(await get_size(storage_key))
        if reported_size <= 0:
            raise ValueError("媒体文件为空")
        if reported_size > limit:
            raise ValueError("媒体文件过大")

    try:
        iter_chunks = getattr(storage, "iter_chunks", None)
        if not callable(iter_chunks):
            data = await storage.get(storage_key)
            if not data:
                raise ValueError("媒体文件为空")
            if len(data) > limit:
                raise ValueError("媒体文件过大")
            await asyncio.to_thread(Path(local_path).write_bytes, data)
            return

        total = 0
        with open(local_path, "wb") as output:
            async for chunk in iter_chunks(
                storage_key,
                chunk_size=_STORAGE_CHUNK_SIZE,
                start=0,
                end=None,
            ):
                if not chunk:
                    continue
                total += len(chunk)
                if total > limit:
                    raise ValueError("媒体文件过大")
                await asyncio.to_thread(output.write, chunk)
        if total == 0:
            raise ValueError("媒体文件为空")
    except Exception:
        Path(local_path).unlink(missing_ok=True)
        raise


def _storage_attempt_id(lease_token: str) -> str:
    return hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]


def _is_attempt_storage_key(storage_key: str | None, attempt_id: str) -> bool:
    return bool(storage_key and attempt_id in storage_key)


async def _delete_lost_lease_outputs(
    attempt_id: str,
    *storage_keys: str | None,
    storage: Any | None = None,
) -> bool:
    """失租 worker 只删除名称中携带本租约 attempt id 的对象。"""

    handled = True
    for storage_key in storage_keys:
        if storage_key is None:
            continue
        if not _is_attempt_storage_key(storage_key, attempt_id):
            handled = False
            continue
        if not await _delete_generated_output(storage_key, storage=storage):
            handled = False
    return handled


def _attempt_cleanup_record(
    *,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
) -> dict[str, Any]:
    record = {
        "storage_attempt_id": storage_attempt_id,
        "shot_group_id": shot_group_id,
        "video_storage_key": video_storage_key,
        "last_frame_storage_key": last_frame_storage_key,
        "retryable": True,
    }
    if provider_task_id is not None:
        record["provider_task_id"] = provider_task_id
    return record


def _merge_video_attempt_cleanup_debt(
    output_json: dict | None,
    *,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None,
) -> dict[str, Any]:
    merged_output = dict(output_json or {})
    raw_debts = merged_output.get(_ATTEMPT_CLEANUP_DEBT_KEY)
    debts = dict(raw_debts) if isinstance(raw_debts, dict) else {}
    existing = debts.get(storage_attempt_id)
    existing_record = dict(existing) if isinstance(existing, dict) else {}
    debts[storage_attempt_id] = _attempt_cleanup_record(
        shot_group_id=shot_group_id or existing_record.get("shot_group_id"),
        storage_attempt_id=storage_attempt_id,
        video_storage_key=video_storage_key or existing_record.get("video_storage_key"),
        last_frame_storage_key=(
            last_frame_storage_key or existing_record.get("last_frame_storage_key")
        ),
        provider_task_id=provider_task_id or existing_record.get("provider_task_id"),
    )
    merged_output[_ATTEMPT_CLEANUP_DEBT_KEY] = debts
    return merged_output


def _record_stale_video_attempt_cleanup_debt(job: Any) -> bool:
    """在清除旧 lease 前，用确定性对象键原子保留 attempt 清理事实。"""

    from app.services.video_remote_cancellation import (
        OrphanProviderCancellationCapacityExceeded,
        merge_reclaimed_video_task_generation,
    )

    lease_token = getattr(job, "lease_token", None)
    job_id = getattr(job, "job_id", None)
    if not isinstance(lease_token, str) or not lease_token or not isinstance(job_id, str):
        return False
    storage_attempt_id = _storage_attempt_id(lease_token)
    shot_group_id = getattr(job, "shot_group_id", None)
    if not isinstance(shot_group_id, str):
        shot_group_id = None
    output_json = dict(getattr(job, "output_json", None) or {})
    authority_consistent, provider_task_id = _provider_task_id_authority_from_job(job)
    if not authority_consistent:
        logger.error(
            "卡死视频任务 Provider ID 权威来源冲突，保留原 lease job_id=%s",
            job_id,
        )
        return False
    video_storage_key = f"generate_images/video/{job_id}-{storage_attempt_id}.mp4"
    last_frame_storage_key = (
        "/generate_images/video/frames/"
        f"{shot_group_id}_{storage_attempt_id}_last.png"
        if shot_group_id is not None
        else None
    )
    merged_output = _merge_video_attempt_cleanup_debt(
        output_json,
        shot_group_id=shot_group_id,
        storage_attempt_id=storage_attempt_id,
        video_storage_key=video_storage_key,
        last_frame_storage_key=last_frame_storage_key,
        provider_task_id=provider_task_id,
    )
    if provider_task_id is not None:
        # 历史任务可能只在 metrics 保存远端 ID；恢复 worker 必须复用，避免重复提交。
        merged_output["provider_task_id"] = provider_task_id
    metrics = dict(getattr(job, "metrics", None) or {})
    if provider_task_id is not None:
        metrics["provider_task_id"] = provider_task_id
    try:
        merged_output, metrics = merge_reclaimed_video_task_generation(
            merged_output,
            metrics,
            lease_token,
        )
    except VideoTaskCheckpointConflict as exc:
        logger.error(
            "卡死视频任务旧代际无法持久化，保留原 lease "
            "job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False
    except OrphanProviderCancellationCapacityExceeded as exc:
        logger.error(
            "卡死视频任务旧代际容量已满，保留原 lease "
            "job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False
    job.output_json = merged_output
    job.metrics = metrics
    return True


def _pending_attempt_cleanup_debts(output_json: dict | None) -> dict[str, dict[str, Any]]:
    raw = (output_json or {}).get(_ATTEMPT_CLEANUP_DEBT_KEY)
    if not isinstance(raw, dict):
        return {}
    return {
        str(attempt_id): dict(record)
        for attempt_id, record in raw.items()
        if isinstance(record, dict) and record.get("retryable") is True
    }


async def _upsert_video_attempt_cleanup_debt(
    db: Any,
    *,
    job_id: str,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
    expected_script_id: str | None = None,
) -> bool:
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        return False
    if expected_script_id is not None and job.script_id != expected_script_id:
        raise RuntimeError("video_cleanup_debt_script_mismatch")
    if (
        video_storage_key is not None
        and job.status == "succeeded"
        and (job.output_json or {}).get("video_storage_key") == video_storage_key
    ):
        return True

    job.output_json = _merge_video_attempt_cleanup_debt(
        job.output_json,
        shot_group_id=shot_group_id,
        storage_attempt_id=storage_attempt_id,
        video_storage_key=video_storage_key,
        last_frame_storage_key=last_frame_storage_key,
        provider_task_id=provider_task_id,
    )
    return True


async def _enqueue_missing_video_job_attempt_cleanup(
    db: Any,
    *,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
) -> bool:
    """VideoJob 已被删除时，将可判定的 attempt 对象转入独立持久清理队列。"""

    from sqlalchemy import select

    from app.models.shot_group import ShotGroup
    from app.services.media_storage_jobs import enqueue_delete

    cleanup_keys: list[str] = []
    handled = True
    if video_storage_key is not None:
        if _is_attempt_storage_key(video_storage_key, storage_attempt_id):
            cleanup_keys.append(video_storage_key)
        else:
            handled = False

    if last_frame_storage_key is not None:
        if not _is_attempt_storage_key(last_frame_storage_key, storage_attempt_id):
            handled = False
        elif shot_group_id is None:
            handled = False
        else:
            reference_frames = (
                await db.execute(
                    select(ShotGroup.reference_frames)
                    .where(ShotGroup.shot_group_id == shot_group_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if not (
                isinstance(reference_frames, dict)
                and reference_frames.get("last_frame") == last_frame_storage_key
            ):
                cleanup_keys.append(last_frame_storage_key)

    for storage_key in dict.fromkeys(cleanup_keys):
        digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
        await enqueue_delete(
            db,
            idempotency_key=f"video-delete:{digest}",
            source_key=storage_key,
        )
    return handled


async def _persist_video_attempt_cleanup_debt(
    *,
    job_id: str,
    script_id: str | None,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
) -> bool:
    """按父行锁序写债务；删除已胜出时改写为无 VideoJob 外键的清理任务。"""

    from sqlalchemy import select

    from app.core.database import async_session_factory
    from app.models.script import Script
    from app.models.video_generation_job import VideoGenerationJob

    try:
        async with async_session_factory() as debt_db:
            resolved_script_id = script_id
            if resolved_script_id is None:
                resolved_script_id = (
                    await debt_db.execute(
                        select(VideoGenerationJob.script_id).where(
                            VideoGenerationJob.job_id == job_id
                        )
                    )
                ).scalar_one_or_none()
            if resolved_script_id is not None:
                # 与 Script/Episode 生命周期删除统一为 Script -> VideoJob 锁序。
                await debt_db.execute(
                    select(Script.id)
                    .where(Script.id == resolved_script_id)
                    .with_for_update()
                )
            upsert_kwargs: dict[str, Any] = {
                "db": debt_db,
                "job_id": job_id,
                "shot_group_id": shot_group_id,
                "storage_attempt_id": storage_attempt_id,
                "video_storage_key": video_storage_key,
                "last_frame_storage_key": last_frame_storage_key,
                "expected_script_id": resolved_script_id,
            }
            if provider_task_id is not None:
                upsert_kwargs["provider_task_id"] = provider_task_id
            persisted = await _upsert_video_attempt_cleanup_debt(
                **upsert_kwargs,
            )
            if not persisted:
                persisted = await _enqueue_missing_video_job_attempt_cleanup(
                    debt_db,
                    shot_group_id=shot_group_id,
                    storage_attempt_id=storage_attempt_id,
                    video_storage_key=video_storage_key,
                    last_frame_storage_key=last_frame_storage_key,
                )
            await debt_db.commit()
            return persisted
    except Exception as exc:  # noqa: BLE001 - 由 Redis 重试通道兜底
        logger.warning(
            "持久化视频 attempt 清理债务失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False


async def _clear_video_attempt_cleanup_debt(
    db: Any,
    *,
    job_id: str,
    storage_attempt_id: str,
    expected_record: dict[str, Any],
) -> bool:
    from sqlalchemy import select
    from sqlalchemy.orm import noload

    from app.models.video_generation_job import VideoGenerationJob

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        return True
    output_json = dict(job.output_json or {})
    raw_debts = output_json.get(_ATTEMPT_CLEANUP_DEBT_KEY)
    if not isinstance(raw_debts, dict):
        return True
    debts = dict(raw_debts)
    existing = debts.get(storage_attempt_id)
    if existing is None:
        return True
    if not isinstance(existing, dict) or dict(existing) != expected_record:
        return False
    debts.pop(storage_attempt_id, None)
    if debts:
        output_json[_ATTEMPT_CLEANUP_DEBT_KEY] = debts
    else:
        output_json.pop(_ATTEMPT_CLEANUP_DEBT_KEY, None)
    job.output_json = output_json
    return True


async def _clear_video_attempt_cleanup_debt_independently(
    *,
    job_id: str,
    storage_attempt_id: str,
    expected_record: dict[str, Any],
) -> bool:
    from app.core.database import async_session_factory

    try:
        async with async_session_factory() as clear_db:
            cleared = await _clear_video_attempt_cleanup_debt(
                clear_db,
                job_id=job_id,
                storage_attempt_id=storage_attempt_id,
                expected_record=expected_record,
            )
            if not cleared:
                await clear_db.rollback()
                return False
            await clear_db.commit()
            return True
    except Exception as exc:  # noqa: BLE001 - 保留任务重试直到债务可安全清除
        logger.warning(
            "清除视频 attempt 清理债务失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        return False


async def _reconcile_video_attempt_outputs(
    *,
    job_id: str | None,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    storage: Any | None = None,
) -> bool | None:
    """判定 attempt 产物归属；True 保留、False 已清理、None 稍后重试。"""

    if video_storage_key is not None:
        if job_id is None:
            return None
        authoritative = await _authoritative_video_attempt_succeeded(
            job_id,
            video_storage_key,
        )
        if authoritative is True:
            # 视频成功与 ShotGroup 末帧更新属于同一事务，成功即保护整组产物。
            return True
        if authoritative is None:
            return None
        video_cleaned = await _delete_lost_lease_outputs(
            storage_attempt_id,
            video_storage_key,
            storage=storage,
        )
        if last_frame_storage_key is None:
            return False if video_cleaned else None
        if shot_group_id is None:
            return None
        authoritative_frame = await _authoritative_last_frame_referenced(
            shot_group_id,
            last_frame_storage_key,
        )
        if authoritative_frame is True:
            return False if video_cleaned else None
        if authoritative_frame is None:
            return None
        frame_cleaned = await _delete_lost_lease_outputs(
            storage_attempt_id,
            last_frame_storage_key,
            storage=storage,
        )
        return False if video_cleaned and frame_cleaned else None

    if last_frame_storage_key is None:
        return False
    if shot_group_id is None:
        return None
    authoritative_frame = await _authoritative_last_frame_referenced(
        shot_group_id,
        last_frame_storage_key,
    )
    if authoritative_frame is True:
        return True
    if authoritative_frame is None:
        return None
    cleaned = await _delete_lost_lease_outputs(
        storage_attempt_id,
        last_frame_storage_key,
        storage=storage,
    )
    return False if cleaned else None


async def _schedule_video_attempt_cleanup(
    *,
    job_id: str | None,
    script_id: str | None,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
) -> bool:
    """同时保留 DB 债务并投递 Redis 任务，任一成功即可恢复。"""

    persisted = False
    if job_id is not None:
        persist_kwargs: dict[str, Any] = {
            "job_id": job_id,
            "script_id": script_id,
            "shot_group_id": shot_group_id,
            "storage_attempt_id": storage_attempt_id,
            "video_storage_key": video_storage_key,
            "last_frame_storage_key": last_frame_storage_key,
        }
        if provider_task_id is not None:
            persist_kwargs["provider_task_id"] = provider_task_id
        persisted = await _persist_video_attempt_cleanup_debt(**persist_kwargs)
    published = False
    try:
        task_kwargs: dict[str, Any] = {
            "job_id": job_id,
            "shot_group_id": shot_group_id,
            "storage_attempt_id": storage_attempt_id,
            "video_storage_key": video_storage_key,
            "last_frame_storage_key": last_frame_storage_key,
        }
        if provider_task_id is not None:
            task_kwargs["provider_task_id"] = provider_task_id
        reconcile_video_attempt_outputs_task.apply_async(
            kwargs=task_kwargs,
            countdown=_ATTEMPT_CLEANUP_RETRY_SECONDS,
        )
        published = True
    except Exception:  # noqa: BLE001 - 主任务终态不能被补偿投递失败覆盖
        logger.exception("投递视频 attempt 产物核验任务失败")
    if not persisted and not published:
        logger.error(
            "视频 attempt 清理债务未能持久化 job_id=%s attempt_id=%s",
            job_id,
            storage_attempt_id,
        )
    return persisted or published


async def _cleanup_or_schedule_video_attempt_outputs(
    *,
    job_id: str | None,
    script_id: str | None,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
    storage: Any | None = None,
) -> bool | None:
    authoritative = await _reconcile_video_attempt_outputs(
        job_id=job_id,
        shot_group_id=shot_group_id,
        storage_attempt_id=storage_attempt_id,
        video_storage_key=video_storage_key,
        last_frame_storage_key=last_frame_storage_key,
        storage=storage,
    )
    if authoritative is None:
        schedule_kwargs: dict[str, Any] = {
            "job_id": job_id,
            "script_id": script_id,
            "shot_group_id": shot_group_id,
            "storage_attempt_id": storage_attempt_id,
            "video_storage_key": video_storage_key,
            "last_frame_storage_key": last_frame_storage_key,
        }
        if provider_task_id is not None:
            schedule_kwargs["provider_task_id"] = provider_task_id
        scheduled = await _schedule_video_attempt_cleanup(**schedule_kwargs)
        if not scheduled:
            logger.error(
                "视频 attempt 无可恢复清理通道 job_id=%s attempt_id=%s provider_task_id=%s",
                job_id,
                storage_attempt_id,
                provider_task_id,
            )
            raise RuntimeError("video_attempt_cleanup_unavailable")
    return authoritative


@app.task(
    name="app.workers.video_generation.reconcile_video_attempt_outputs",
    bind=True,
    max_retries=None,
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def reconcile_video_attempt_outputs_task(
    self,
    *,
    job_id: str | None,
    shot_group_id: str | None,
    storage_attempt_id: str,
    video_storage_key: str | None,
    last_frame_storage_key: str | None,
    provider_task_id: str | None = None,
) -> dict[str, str]:
    """持续核验提交不确定的 attempt，只有确认非权威后才删除。"""

    expected_record = _attempt_cleanup_record(
        shot_group_id=shot_group_id,
        storage_attempt_id=storage_attempt_id,
        video_storage_key=video_storage_key,
        last_frame_storage_key=last_frame_storage_key,
        provider_task_id=provider_task_id,
    )

    async def _run() -> bool | None:
        authoritative = await _reconcile_video_attempt_outputs(
            job_id=job_id,
            shot_group_id=shot_group_id,
            storage_attempt_id=storage_attempt_id,
            video_storage_key=video_storage_key,
            last_frame_storage_key=last_frame_storage_key,
        )
        if authoritative is None:
            return None
        if job_id is not None and not await _clear_video_attempt_cleanup_debt_independently(
            job_id=job_id,
            storage_attempt_id=storage_attempt_id,
            expected_record=expected_record,
        ):
            return None
        return authoritative

    try:
        authoritative = run_worker_async(_run())
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001 - 持久任务需保留到依赖恢复
        raise self.retry(
            exc=exc,
            countdown=_ATTEMPT_CLEANUP_RETRY_SECONDS,
        ) from exc
    if authoritative is None:
        raise self.retry(countdown=_ATTEMPT_CLEANUP_RETRY_SECONDS)
    return {"status": "preserved" if authoritative else "cleaned"}


async def reconcile_video_attempt_cleanup_debts(
    db: Any,
    *,
    limit: int = 50,
) -> int:
    """周期恢复未能投递到 Redis 的 attempt 清理债务。"""

    from sqlalchemy import select

    from app.models.video_generation_job import VideoGenerationJob

    if limit <= 0:
        return 0
    candidates = list(
        (
            await db.execute(
                select(
                    VideoGenerationJob.job_id,
                    VideoGenerationJob.output_json,
                )
                .where(
                    VideoGenerationJob.output_json[
                        _ATTEMPT_CLEANUP_DEBT_KEY
                    ].as_string().is_not(None)
                )
                .order_by(VideoGenerationJob.updated_at.asc())
                .limit(limit)
            )
        ).all()
    )
    # 后续判权使用独立会话；先释放候选扫描事务，避免自锁。
    await db.rollback()
    resolved = 0
    for job_id, output_json in candidates:
        debts = _pending_attempt_cleanup_debts(output_json)
        for storage_attempt_id, record in debts.items():
            if record.get("storage_attempt_id") != storage_attempt_id:
                continue
            shot_group_id = record.get("shot_group_id")
            video_storage_key = record.get("video_storage_key")
            last_frame_storage_key = record.get("last_frame_storage_key")
            authoritative = await _reconcile_video_attempt_outputs(
                job_id=str(job_id),
                shot_group_id=(
                    str(shot_group_id) if isinstance(shot_group_id, str) else None
                ),
                storage_attempt_id=storage_attempt_id,
                video_storage_key=(
                    str(video_storage_key)
                    if isinstance(video_storage_key, str)
                    else None
                ),
                last_frame_storage_key=(
                    str(last_frame_storage_key)
                    if isinstance(last_frame_storage_key, str)
                    else None
                ),
            )
            if authoritative is None:
                continue
            cleared = await _clear_video_attempt_cleanup_debt(
                db,
                job_id=str(job_id),
                storage_attempt_id=storage_attempt_id,
                expected_record=record,
            )
            if not cleared:
                await db.rollback()
                continue
            await db.commit()
            resolved += 1
    return resolved


async def _update_last_frame(
    db,
    unit_id: str,
    video_storage_key: str,
    storage_attempt_id: str,
) -> str:
    """从视频末尾提取一帧，存为图片，更新 shot_group.reference_frames.last_frame。

    unit_id 形如 group:<shot_group_id>。用 ffmpeg 提取末尾帧：
      ffmpeg -sseek <duration-0.1> -i <video> -frames:v 1 <out.png>
    简化：用 ffmpeg 末尾取帧 (-sseof -0.1)。

    失败不阻断主流程（视频已生成成功）。
    """
    import re

    from app.models.shot_group import ShotGroup
    from app.providers.storage import get_storage

    match = re.match(r"group:(.+)", unit_id or "")
    if not match:
        raise RuntimeError("frame_extraction_failed")
    shot_group_id = match.group(1)

    storage = get_storage()
    # 先创建空临时文件，再由对象存储分块写入，避免整段视频驻留内存。
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as vf:
        video_tmp = vf.name
    frame_tmp = video_tmp.replace(".mp4", "_lastframe.png")

    frame_storage_key: str | None = None

    def _subprocess_stderr(result: Any) -> str:
        """截断并规范化命令错误输出，避免日志被异常媒体内容淹没。"""
        stderr = getattr(result, "stderr", b"")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        return str(stderr).strip()[-1000:]

    try:
        await _download_storage_file(storage, video_storage_key, video_tmp)
        # 取末尾帧：先探测时长，再 seek 到末尾前 0.1s
        # ffprobe 取时长
        probe = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", video_tmp,
            ],
            capture_output=True, text=True, timeout=30,
        )
        probe_returncode = int(getattr(probe, "returncode", 0))
        if probe_returncode != 0:
            raise RuntimeError(
                "ffprobe_failed "
                f"returncode={probe_returncode} stderr={_subprocess_stderr(probe)}"
            )
        duration = 0.0
        try:
            duration = float(probe.stdout.strip() or 0)
        except ValueError:
            duration = 0.0
        seek = max(0.0, duration - 0.1)

        frame_result = subprocess.run(
            [
                "ffmpeg", "-y", "-ss", str(seek), "-i", video_tmp,
                "-frames:v", "1", frame_tmp,
            ],
            capture_output=True, timeout=60,
        )
        frame_returncode = int(getattr(frame_result, "returncode", 0))
        if frame_returncode != 0:
            raise RuntimeError(
                "ffmpeg_frame_extraction_failed "
                f"returncode={frame_returncode} stderr={_subprocess_stderr(frame_result)}"
            )
        if not os.path.exists(frame_tmp):
            raise RuntimeError(
                "ffmpeg_frame_output_missing "
                f"duration={duration:.3f} seek={seek:.3f}"
            )
        frame_size = os.path.getsize(frame_tmp)
        if frame_size <= 0:
            raise RuntimeError("frame_extraction_failed")
        if frame_size > _MAX_LAST_FRAME_BYTES:
            raise RuntimeError("frame_extraction_failed")
        with open(frame_tmp, "rb") as f:
            frame_bytes = f.read(_MAX_LAST_FRAME_BYTES + 1)
        if not frame_bytes or len(frame_bytes) > _MAX_LAST_FRAME_BYTES:
            raise RuntimeError("frame_extraction_failed")
        frame_storage_key = (
            "/generate_images/video/frames/"
            f"{shot_group_id}_{storage_attempt_id}_last.png"
        )
        await storage.put(frame_storage_key, frame_bytes)
    except Exception as exc:  # noqa: BLE001
        if frame_storage_key is not None:
            await _delete_generated_output(frame_storage_key, storage=storage)
        logger.warning("ffmpeg 提取末帧失败 unit_id=%s", unit_id, exc_info=True)
        raise RuntimeError("frame_extraction_failed") from exc
    finally:
        for tmp in (video_tmp, frame_tmp):
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass

    if frame_storage_key is None:
        raise RuntimeError("frame_extraction_failed")

    try:
        # 更新 shot_group.reference_frames.last_frame
        from sqlalchemy import select

        sg_result = await db.execute(
            select(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
        )
        shot_group = sg_result.scalar_one_or_none()
        if shot_group is None:
            raise RuntimeError("frame_extraction_failed")
        ref_frames = dict(shot_group.reference_frames or {})
        previous_last_frame = ref_frames.get("last_frame")
        ref_frames["last_frame"] = frame_storage_key
        shot_group.reference_frames = ref_frames
        if (
            isinstance(previous_last_frame, str)
            and previous_last_frame
            and previous_last_frame != frame_storage_key
        ):
            from app.services.storage_references import (
                StorageReferenceExclusions,
                enqueue_storage_deletes,
            )

            # flush 后旧 last_frame 已从该行移除；继续扫描当前 ShotGroup，避免漏掉
            # first_frame/materials/audio_elements 等字段对同一 key 的其他引用。
            await db.flush()
            await enqueue_storage_deletes(
                db,
                {previous_last_frame},
                exclusions=StorageReferenceExclusions(),
            )
    except Exception as exc:  # noqa: BLE001 - 已落盘帧必须消解归属
        wrapped = RuntimeError("frame_extraction_failed")
        setattr(wrapped, "last_frame_storage_key", frame_storage_key)
        try:
            cleanup_state = await _cleanup_or_schedule_video_attempt_outputs(
                job_id=None,
                script_id=None,
                shot_group_id=shot_group_id,
                storage_attempt_id=storage_attempt_id,
                video_storage_key=None,
                last_frame_storage_key=frame_storage_key,
                storage=storage,
            )
        except Exception as cleanup_exc:
            setattr(cleanup_exc, "last_frame_storage_key", frame_storage_key)
            setattr(cleanup_exc, "last_frame_cleanup_state", None)
            raise cleanup_exc from exc
        logger.warning("更新视频末帧引用失败 unit_id=%s", unit_id, exc_info=True)
        setattr(wrapped, "last_frame_cleanup_state", cleanup_state)
        raise wrapped from exc
    return frame_storage_key


async def reconcile_stale_video_jobs(db: Any, *, limit: int = 50) -> list[str]:
    """将卡死视频任务恢复到事务 outbox，重复扫描不会重复重置。"""
    from sqlalchemy import select

    from app.crud import video as video_crud
    from app.models.task_dispatch_outbox import TaskDispatchOutbox
    from app.services.task_dispatch import cancel_outbox_task, enqueue_task
    from app.services.video_remote_cancellation import (
        provider_submission_is_unresolved,
        provider_submission_requires_manual_review,
    )

    if callable(getattr(db, "execute", None)):
        await reconcile_video_attempt_cleanup_debts(db, limit=limit)
    waiting_recovered = (
        await reconcile_waiting_video_dependencies(db, limit=limit)
        if callable(getattr(db, "rollback", None))
        else []
    )
    current = datetime.now(UTC)
    stale = await video_crud.list_stale_processing_jobs(db, lease_seconds=_LEASE_SECONDS, limit=limit)
    recovered: list[str] = list(waiting_recovered)
    for job in stale:
        provider_key = str(getattr(job, "provider_key", "") or "")
        attempt = int(getattr(job, "attempt", 0) or 0)
        if provider_submission_requires_manual_review(
            job.output_json,
            job.metrics,
            provider_key=provider_key,
            attempt=attempt,
        ):
            reviewed = await video_crud.mark_video_submission_uncertain_for_manual_review(
                db,
                job.job_id,
                job.lease_token or "",
                provider_key=provider_key,
                attempt=attempt,
            )
            if reviewed:
                await cancel_outbox_task(
                    db,
                    task_type="video.generate",
                    job_id=job.job_id,
                    attempt=job.attempt,
                )
                logger.warning("卡死视频任务已转人工核对 job_id=%s", job.job_id)
            else:
                logger.error("卡死视频任务无法转人工核对 job_id=%s", job.job_id)
            continue
        if provider_submission_is_unresolved(job.output_json, job.metrics):
            logger.error(
                "卡死视频任务存在未决远端提交但证据不完整，保留原 lease "
                "job_id=%s",
                job.job_id,
            )
            continue
        changed = False
        if job.status == "processing":
            if not _record_stale_video_attempt_cleanup_debt(job):
                logger.error(
                    "卡死视频任务缺少可恢复 lease job_id=%s",
                    job.job_id,
                )
                continue
            job.status = "starting"
            job.lease_owner = None
            job.lease_token = None
            job.lease_until = None
            changed = True

        outbox = (
            await db.execute(
                select(TaskDispatchOutbox).where(
                    TaskDispatchOutbox.task_type == "video.generate",
                    TaskDispatchOutbox.job_id == job.job_id,
                    TaskDispatchOutbox.attempt == job.attempt,
                )
            )
        ).scalar_one_or_none()
        if outbox is None:
            outbox = await enqueue_task(
                db,
                task_type="video.generate",
                job_id=job.job_id,
                attempt=job.attempt,
                payload={
                    "job_id": job.job_id,
                    "script_id": job.script_id,
                    "episode_id": job.episode_id,
                    "shot_group_id": job.shot_group_id,
                    "input_version": job.input_version,
                },
            )
            job.dispatch_id = outbox.id
            changed = True
        elif outbox.status == "published" or (
            outbox.status == "running"
            and (outbox.lease_until is None or outbox.lease_until <= current)
        ):
            outbox.status = "pending"
            outbox.available_at = current
            outbox.published_at = None
            outbox.lease_owner = None
            outbox.lease_token = None
            outbox.lease_until = None
            changed = True
        if changed:
            recovered.append(job.job_id)
    await db.commit()
    return recovered


@app.task(
    name="app.workers.video_generation.recover_stale_jobs",
    acks_late=True,
    reject_on_worker_lost=True,
    acks_on_failure_or_timeout=False,
)
def recover_stale_jobs() -> dict:
    """扫描租约过期任务并经事务 outbox 可靠重投。"""

    async def _scan() -> list[str]:
        from app.core.database import async_session_factory

        async with async_session_factory() as db:
            return await reconcile_stale_video_jobs(db)

    try:
        resubmitted = run_worker_async(_scan())
        return {"resubmitted": resubmitted, "count": len(resubmitted)}
    except WorkerAsyncRuntimeStopping as exc:
        _requeue_and_recycle_poisoned_video_worker(exc)
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception:  # noqa: BLE001
        logger.exception("扫描卡死视频任务失败")
        return {"resubmitted": [], "count": 0}
