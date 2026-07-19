"""分镜生成 Celery worker。

异步执行 LLM-5 分镜生成, 落库 Scene/ShotGroup/Shot。
积分三段式: reserve (预估) -> 生成 -> settle/release。

关键点:
- 每次生成都创建一条 StoryboardJob (job_id 唯一), job_id 同时作为积分预扣 source_id,
  避免同集重新生成时撞 CreditReservation 的 unique(source_type, source_id) 约束
  (episode_id 作 source_id 时一旦 captured/released 就再无法新建预约)。
- 直接使用 reserve() 的返回值作为 reservation_id, 不做二次查询
  (反查会命中多行 historical reservation 抛 MultipleResultsFound)。
- reserve/settle/release 均传 actor_user_id=user_id, 保证 CreditLog 审计字段完整。
- 全程更新 StoryboardJob.status/progress, 供前端轮询 storyboard-status 端点 (AC-VG01)。
- 失败时确保 reservation 被 release 以便重试。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import noload

from app.core.config import settings
from app.core.database import async_session_factory
from app.core.job_state import PipelineFailure
from app.crud.storyboard import (
    claim_job,
    complete_job,
    fail_job_if_current_token,
    get_job,
    heartbeat_job,
    update_job,
)
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.storyboard_job import StoryboardJob
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.services import storyboard as storyboard_service
from app.services.task_dispatch import enqueue_task
from app.workers.async_runtime import run_worker_async

logger = logging.getLogger(__name__)
STORYBOARD_OUTBOX_TASK_TYPE = "storyboard.generate"

# 按 PLAN 路径 import (celery_app 为 Phase 1 产出)
try:
    from app.workers.celery_app import celery_app
except ImportError:  # 并行模块未就绪时 fallback
    logger.warning("celery_app 未就绪, 使用 stub")
    celery_app = None  # type: ignore[assignment]

# 积分三段式 (按 PLAN 路径 import, T2 产出)
try:
    from app.services.credits import release, reserve, settle
except ImportError:  # 并行模块未就绪时 fallback
    reserve = None  # type: ignore[assignment]
    settle = None  # type: ignore[assignment]
    release = None  # type: ignore[assignment]

if celery_app is not None:
    _beat_schedule = dict(celery_app.conf.beat_schedule or {})
    _beat_schedule.setdefault(
        "storyboard-dispatch-reconcile",
        {
            "task": "app.workers.storyboard.reconcile_storyboard_dispatches",
            "schedule": 60.0,
        },
    )
    _beat_schedule.setdefault(
        "storyboard-financial-cleanup",
        {
            "task": "app.workers.storyboard.reconcile_storyboard_financial_cleanup",
            "schedule": 60.0,
        },
    )
    celery_app.conf.beat_schedule = _beat_schedule


async def _load_episode(db, episode_id: str) -> Episode | None:
    result = await db.execute(select(Episode).where(Episode.episode_id == episode_id))
    return result.scalar_one_or_none()


def _job_input_matches_episode(job: StoryboardJob, episode: Episode | None) -> bool:
    return (
        episode is not None
        and episode.episode_version == job.input_version
        and (
            job.input_client_save_seq is None
            or episode.client_save_seq == job.input_client_save_seq
        )
    )


def _job_reservation_id(job: StoryboardJob) -> str | None:
    return job.reservation_id or (job.detail or {}).get("reservation_id")


async def _lock_current_storyboard_job(
    db, job_id: str, lease_token: str
) -> StoryboardJob | None:
    """锁定仍由当前 worker 持有租约的任务行。"""
    job = await db.get(
        StoryboardJob,
        job_id,
        with_for_update=True,
        populate_existing=True,
    )
    if (
        job is None
        or job.status != "running"
        or job.lease_token != lease_token
        or job.canceled_at is not None
    ):
        return None
    return job


async def _run_claimed_storyboard_job(db, job: StoryboardJob, lease_token: str) -> dict:
    """执行已持有 lease 的分镜任务；每个关键边界检查版本与取消。"""
    episode = await _load_episode(db, job.episode_id)
    if not _job_input_matches_episode(job, episode):
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard input changed during generation")
    if job.status == "canceled" or job.canceled_at is not None:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard job canceled")
    if not await heartbeat_job(db, job.job_id, lease_token):
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard lease heartbeat failed")

    result = await storyboard_service.generate_storyboard(
        db,
        job.script_id,
        job.episode_id,
        expected_episode_version=job.input_version,
        expected_client_save_seq=job.input_client_save_seq,
    )
    if not await heartbeat_job(db, job.job_id, lease_token):
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard lease heartbeat failed")

    fresh_episode = await _load_episode(db, job.episode_id)
    if not _job_input_matches_episode(job, fresh_episode):
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard input changed before commit")
    if job.status == "canceled" or job.canceled_at is not None:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard job canceled before commit")

    locked_job = await _lock_current_storyboard_job(db, job.job_id, lease_token)
    if locked_job is None:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard lease CAS failed")
    job = locked_job
    reservation_id = _job_reservation_id(job)
    if settle is not None and reservation_id:
        await settle(
            db,
            reservation_id,
            settings.price_storyboard_per_episode,
            actor_user_id=job.triggered_by_user_id,
            commit=False,
        )
    detail = {
        "scenes": result.get("scenes", 0),
        "shot_groups": result.get("shot_groups", 0),
        "shots": result.get("shots", 0),
        "reservation_id": reservation_id,
    }
    if not await complete_job(db, job.job_id, lease_token, status="succeeded", detail=detail):
        if hasattr(db, "rollback"):
            await db.rollback()
        raise RuntimeError("storyboard lease CAS failed")
    await db.commit()
    return result


async def _release_reserved_storyboard(
    db, job: StoryboardJob, reason: str, *, lease_token: str | None = None
) -> bool:
    if lease_token is not None:
        fresh = await _lock_current_storyboard_job(db, job.job_id, lease_token)
        if fresh is None:
            return False
        job = fresh
    reservation_id = _job_reservation_id(job)
    if release is None or not reservation_id:
        return False
    await release(
        db,
        reservation_id,
        reason,
        actor_user_id=job.triggered_by_user_id,
        commit=False,
    )
    return True


async def _set_job(
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    progress_stage: str | None = None,
    progress_message: str | None = None,
    error_message: str | None = None,
    detail: dict | None = None,
) -> None:
    """在独立 session 中更新分镜任务状态 (失败路径也复用, 确保状态一定落库)。"""
    async with async_session_factory() as db:
        await update_job(
            db,
            job_id,
            status=status,
            progress=progress,
            progress_stage=progress_stage,
            progress_message=progress_message,
            error_message=error_message,
            detail=detail,
        )
        await db.commit()


async def _mark_failed_after_rollback(
    job_id: str, exc: Exception, lease_token: str
) -> bool:
    """在任务锁内原子释放积分并写失败终态。"""
    failure = PipelineFailure.from_exception(exc)
    async with async_session_factory() as db:
        job = await _lock_current_storyboard_job(db, job_id, lease_token)
        if job is None:
            await db.rollback()
            return False
        reservation_id = _job_reservation_id(job)
        release_failed = False
        if reservation_id:
            try:
                if release is None:
                    raise RuntimeError("storyboard credit release unavailable")
                await release(
                    db,
                    reservation_id,
                    "分镜生成失败",
                    actor_user_id=job.triggered_by_user_id,
                    commit=False,
                )
            except Exception:
                await db.rollback()
                job = await _lock_current_storyboard_job(db, job_id, lease_token)
                if job is None:
                    await db.rollback()
                    return False
                release_failed = True
                job.financial_cleanup_status = "pending"
                job.financial_cleanup_error = "release_failed"
        completed = await fail_job_if_current_token(
            db,
            job_id,
            lease_token,
            error_message=failure.error_message,
            error_type=failure.error_type,
            retryable=failure.retryable or release_failed,
        )
        if not completed:
            await db.rollback()
            return False
        await db.commit()
        return True


async def _run_storyboard_job_by_id(job_id: str) -> dict:
    """真实 task 安全入口：job_id -> 原子 claim -> 版本/取消栅栏 -> 终态 CAS。"""
    owner = f"storyboard-worker:{job_id}"
    async with async_session_factory() as db:
        lease_token = await claim_job(db, job_id, owner=owner)
        if lease_token is None:
            return {"job_id": job_id, "claimed": False}
        job = await get_job(db, job_id)
        if job is None:
            await db.rollback()
            return {"job_id": job_id, "claimed": False}
        await update_job(
            db,
            job_id,
            progress=10,
            progress_stage="claimed",
            progress_message="分镜任务已领取",
        )
        await db.commit()

    async with async_session_factory() as db:
        job = await get_job(db, job_id)
        if job is None:
            return {"job_id": job_id, "claimed": False}
        try:
            return await _run_claimed_storyboard_job(db, job, lease_token)
        except Exception as exc:
            if hasattr(db, "rollback"):
                await db.rollback()
            await _mark_failed_after_rollback(job_id, exc, lease_token)
            raise


async def reconcile_storyboard_dispatches(
    db,
    *,
    now: datetime | None = None,
    stale_after_seconds: int = 300,
    limit: int = 100,
) -> int:
    """恢复 storyboard stale job、published 后未启动的 outbox 与缺失 outbox。"""
    current = now or datetime.now(UTC)
    stale_cutoff = current - timedelta(seconds=stale_after_seconds)
    result = await db.execute(
        select(StoryboardJob)
        .where(
            StoryboardJob.status.in_(["pending", "running", "retry"]),
            (
                (StoryboardJob.heartbeat_at.is_(None))
                | (StoryboardJob.heartbeat_at <= stale_cutoff)
                | (StoryboardJob.lease_until <= current)
            ),
        )
        .order_by(StoryboardJob.created_at.asc())
        .limit(limit)
    )
    jobs = list(result.scalars().all())
    recovered = 0
    for job in jobs:
        outbox_result = await db.execute(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == STORYBOARD_OUTBOX_TASK_TYPE,
                TaskDispatchOutbox.job_id == job.job_id,
                TaskDispatchOutbox.attempt == job.attempt,
            )
        )
        outbox = outbox_result.scalar_one_or_none()
        if job.status == "running" and (
            job.lease_until is None or job.lease_until <= current
        ):
            job.status = "retry"
            job.lease_owner = None
            job.lease_token = None
            job.lease_until = None
            recovered += 1
        if outbox is None:
            await enqueue_task(
                db,
                task_type=STORYBOARD_OUTBOX_TASK_TYPE,
                job_id=job.job_id,
                attempt=job.attempt,
                payload={
                    "script_id": job.script_id,
                    "episode_id": job.episode_id,
                    "episode_version": job.input_version,
                    "input_version": job.input_version,
                    "input_client_save_seq": job.input_client_save_seq,
                    "user_id": job.triggered_by_user_id,
                    "job_id": job.job_id,
                },
            )
            recovered += 1
            continue
        if outbox.status in {"published", "running"}:
            outbox.status = "pending"
            outbox.lease_owner = None
            outbox.lease_token = None
            outbox.lease_until = None
            outbox.available_at = current
            recovered += 1
    return recovered


async def _load_authoritative_storyboard_reservation(
    db: Any,
    job_id: str,
) -> CreditReservation | None:
    """按不可变 source 映射锁定积分预约，兼容任务载荷丢失。"""

    return (
        await db.execute(
            select(CreditReservation)
            .where(
                CreditReservation.source_type == "storyboard",
                CreditReservation.source_id == job_id,
            )
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _reconcile_one_storyboard_financial_cleanup(db: Any, job_id: str) -> bool:
    job = (
        await db.execute(
            select(StoryboardJob)
            .where(
                StoryboardJob.job_id == job_id,
                StoryboardJob.financial_cleanup_status == "pending",
            )
            .options(noload(StoryboardJob.script))
            .execution_options(populate_existing=True)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if job is None:
        await db.rollback()
        return False

    reservation = await _load_authoritative_storyboard_reservation(db, job.job_id)
    if reservation is not None and reservation.status == "reserved":
        if release is None:
            await db.rollback()
            return False
        await release(
            db,
            reservation.id,
            "分镜失败补偿释放积分",
            actor_user_id=job.triggered_by_user_id,
            commit=False,
        )
        await db.flush()
        reservation = await _load_authoritative_storyboard_reservation(db, job.job_id)
        if reservation is not None and reservation.status == "reserved":
            await db.rollback()
            return False

    job.financial_cleanup_status = "done"
    job.financial_cleanup_error = None
    await db.commit()
    return True


async def _run_storyboard_financial_cleanup_candidate(
    db: Any,
    job_id: str,
) -> tuple[bool, bool]:
    """返回（是否恢复、当前会话是否仍可安全复用）。"""

    try:
        return await _reconcile_one_storyboard_financial_cleanup(db, job_id), True
    except Exception as exc:  # noqa: BLE001 - 单任务失败不得污染后续事务
        logger.warning(
            "分镜财务清理失败 job_id=%s error_type=%s",
            job_id,
            exc.__class__.__name__,
        )
        try:
            await db.rollback()
            return False, True
        except Exception as rollback_exc:  # noqa: BLE001 - 当前会话已不可复用
            logger.warning(
                "分镜财务清理回滚失败 job_id=%s error_type=%s",
                job_id,
                rollback_exc.__class__.__name__,
            )
            return False, False


async def reconcile_storyboard_financial_cleanup(
    db: Any,
    *,
    limit: int = 100,
    session_factory: Callable[[], Any] | None = None,
) -> int:
    """逐任务隔离地补偿分镜失败后的积分释放。"""

    candidate_ids = list(
        (
            await db.execute(
                select(StoryboardJob.job_id)
                .where(StoryboardJob.financial_cleanup_status == "pending")
                .order_by(StoryboardJob.created_at.asc(), StoryboardJob.job_id.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    await db.rollback()
    cleaned = 0
    for job_id in candidate_ids:
        if session_factory is None:
            resolved, healthy = await _run_storyboard_financial_cleanup_candidate(
                db,
                job_id,
            )
            cleaned += int(resolved)
            if not healthy:
                break
            continue

        resolved = False
        try:
            async with session_factory() as task_db:
                resolved, _ = await _run_storyboard_financial_cleanup_candidate(
                    task_db,
                    job_id,
                )
        except Exception as exc:  # noqa: BLE001 - 关闭失败也必须隔离到当前任务
            logger.warning(
                "关闭分镜财务清理会话失败 job_id=%s error_type=%s",
                job_id,
                exc.__class__.__name__,
            )
        cleaned += int(resolved)
    return cleaned


async def _run_storyboard_generation(
    script_id: str,
    episode_id: str,
    user_id: str | None,
    job_id: str,
) -> dict:
    """旧执行入口已停用；所有执行必须通过 job_id claim + lease token CAS。"""
    raise RuntimeError(
        "unsafe storyboard execution entry is disabled; use _run_storyboard_job_by_id"
    )


def _generate_storyboard_task(
    script_id: str,
    episode_id: str,
    user_id: str | None,
    job_id: str,
) -> dict:
    """Celery task 同步入口, 包装异步执行。"""
    return run_worker_async(_run_storyboard_job_by_id(job_id))


def build_storyboard_canvas(payload: dict, storyboard_task) -> object:
    """供共享 outbox publisher 接线的分镜 Celery signature helper。"""
    return storyboard_task.s(
        payload["script_id"],
        payload["episode_id"],
        payload.get("user_id"),
        payload["job_id"],
    )


# 注册 Celery task (若 celery_app 就绪)
if celery_app is not None:
    @celery_app.task(
        name="app.workers.storyboard.generate_storyboard",
        bind=True,
        acks_late=True,
        time_limit=300,
        soft_time_limit=240,
    )
    def generate_storyboard_task(
        self,
        script_id: str,
        episode_id: str,
        user_id: str | None = None,
        job_id: str | None = None,
    ) -> dict:
        """Celery task: 分镜生成 (异步执行)。

        参数:
            script_id: 剧本 ID
            episode_id: 分集 ID
            user_id: 触发用户 ID (用于积分扣减与审计)
            job_id: 分镜任务 ID (StoryboardJob.job_id, 同时为积分 source_id)
        """
        if job_id is None:
            logger.error("generate_storyboard_task 缺少 job_id, 无法跟踪状态")
            return {
                "script_id": script_id,
                "episode_id": episode_id,
                "error": "missing job_id",
            }
        return _generate_storyboard_task(script_id, episode_id, user_id, job_id)

    @celery_app.task(
        name="app.workers.storyboard.reconcile_storyboard_dispatches",
        bind=True,
        max_retries=0,
    )
    def reconcile_storyboard_dispatches_task(self) -> int:
        async def _run() -> int:
            async with async_session_factory() as db:
                recovered = await reconcile_storyboard_dispatches(db)
                await db.commit()
                return recovered

        return run_worker_async(_run())

    @celery_app.task(
        name="app.workers.storyboard.reconcile_storyboard_financial_cleanup",
        bind=True,
        max_retries=0,
    )
    def reconcile_storyboard_financial_cleanup_task(self) -> int:
        async def _run() -> int:
            async with async_session_factory() as db:
                return await reconcile_storyboard_financial_cleanup(
                    db,
                    session_factory=async_session_factory,
                )

        return run_worker_async(_run())
else:
    # fallback: 直接返回同步函数 (开发环境无 Celery 时可直调)
    def generate_storyboard_task(
        script_id: str,
        episode_id: str,
        user_id: str | None = None,
        job_id: str | None = None,
    ) -> dict:
        if job_id is None:
            logger.error("generate_storyboard_task 缺少 job_id, 无法跟踪状态")
            return {
                "script_id": script_id,
                "episode_id": episode_id,
                "error": "missing job_id",
            }
        return _generate_storyboard_task(script_id, episode_id, user_id, job_id)

    async def reconcile_storyboard_dispatches_task() -> int:
        async with async_session_factory() as db:
            recovered = await reconcile_storyboard_dispatches(db)
            await db.commit()
            return recovered

    async def reconcile_storyboard_financial_cleanup_task() -> int:
        async with async_session_factory() as db:
            return await reconcile_storyboard_financial_cleanup(
                db,
                session_factory=async_session_factory,
            )


__all__ = [
    "STORYBOARD_OUTBOX_TASK_TYPE",
    "build_storyboard_canvas",
    "generate_storyboard_task",
    "reconcile_storyboard_dispatches",
    "reconcile_storyboard_dispatches_task",
    "reconcile_storyboard_financial_cleanup",
    "reconcile_storyboard_financial_cleanup_task",
]
