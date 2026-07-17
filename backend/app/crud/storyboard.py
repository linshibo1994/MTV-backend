"""分镜任务数据访问层 — StoryboardJob 的创建/更新/查询。

为异步分镜生成提供状态机落库 (pending -> running -> succeeded|failed)，
供前端轮询 storyboard-status 端点 (AC-VG01 可观测性)。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ids import new_id
from app.core.job_state import LeaseState, claim_lease, complete_lease
from app.models.storyboard_job import StoryboardJob


async def create_job(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    episode_version: int = 1,
    input_version: int = 1,
    input_client_save_seq: int | None = None,
    attempt: int = 1,
    idempotency_key: str,
    request_hash: str,
    reservation_id: str | None = None,
    triggered_by_user_id: str | None = None,
    owner_user_id: str | None = None,
    task_id: str | None = None,
) -> StoryboardJob:
    """创建一条分镜生成任务记录，初始 status=pending, progress=0。"""
    job = StoryboardJob(
        job_id=new_id(),
        job_type="storyboard",
        script_id=script_id,
        episode_id=episode_id,
        episode_version=episode_version,
        input_version=input_version,
        input_client_save_seq=input_client_save_seq,
        attempt=attempt,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        reservation_id=reservation_id,
        triggered_by_user_id=triggered_by_user_id,
        owner_user_id=owner_user_id or triggered_by_user_id,
        task_id=task_id,
        status="pending",
        progress=0,
    )
    try:
        if hasattr(db, "begin_nested"):
            async with db.begin_nested():
                db.add(job)
                await db.flush()
        else:
            db.add(job)
            await db.flush()
    except IntegrityError:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise
    return job


async def next_attempt_for_episode(db: AsyncSession, episode_id: str) -> int:
    """在分集范围内分配单调 attempt；调用方应已持有分集行锁或处于单事务。"""
    result = await db.execute(
        select(func.coalesce(func.max(StoryboardJob.attempt), 0)).where(
            StoryboardJob.episode_id == episode_id
        )
    )
    return int(result.scalar_one()) + 1


async def get_by_idempotency_key(
    db: AsyncSession, episode_id: str, idempotency_key: str
) -> StoryboardJob | None:
    result = await db.execute(
        select(StoryboardJob).where(
            StoryboardJob.episode_id == episode_id,
            StoryboardJob.idempotency_key == idempotency_key,
        )
    )
    return result.scalar_one_or_none()


async def list_jobs_for_episode(
    db: AsyncSession, episode_id: str, *, limit: int = 50, offset: int = 0
) -> list[StoryboardJob]:
    result = await db.execute(
        select(StoryboardJob)
        .where(StoryboardJob.episode_id == episode_id)
        .order_by(StoryboardJob.created_at.desc(), StoryboardJob.job_id.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all())


async def count_jobs_for_episode(db: AsyncSession, episode_id: str) -> int:
    result = await db.execute(
        select(func.count(StoryboardJob.job_id)).where(
            StoryboardJob.episode_id == episode_id
        )
    )
    return int(result.scalar_one())


async def get_job(db: AsyncSession, job_id: str) -> StoryboardJob | None:
    """按 job_id 查询单个分镜任务。"""
    result = await db.execute(
        select(StoryboardJob).where(StoryboardJob.job_id == job_id)
    )
    return result.scalar_one_or_none()


async def get_latest_for_episode(
    db: AsyncSession, episode_id: str
) -> StoryboardJob | None:
    """查询某分集最新一条分镜任务 (按创建时间倒序)。"""
    result = await db.execute(
        select(StoryboardJob)
        .where(StoryboardJob.episode_id == episode_id)
        .order_by(StoryboardJob.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_running_for_episode(
    db: AsyncSession, episode_id: str
) -> StoryboardJob | None:
    """查询某分集是否存在进行中 (pending|running) 的分镜任务。"""
    result = await db.execute(
        select(StoryboardJob).where(
            StoryboardJob.episode_id == episode_id,
            StoryboardJob.status.in_(("pending", "running", "retry")),
        )
        .order_by(StoryboardJob.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def update_job(
    db: AsyncSession,
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    progress_stage: str | None = None,
    progress_message: str | None = None,
    error_message: str | None = None,
    detail: dict | None = None,
    task_id: str | None = None,
    reservation_id: str | None = None,
    error_type: str | None = None,
    retryable: bool | None = None,
) -> None:
    """更新分镜任务字段 (仅更新非 None 字段)。"""
    values: dict[str, Any] = {}
    if status is not None:
        values["status"] = status
    if progress is not None:
        values["progress"] = progress
    if progress_stage is not None:
        values["progress_stage"] = progress_stage
    if progress_message is not None:
        values["progress_message"] = progress_message
    if error_message is not None:
        values["error_message"] = error_message
    if detail is not None:
        values["detail"] = detail
    if task_id is not None:
        values["task_id"] = task_id
    if reservation_id is not None:
        values["reservation_id"] = reservation_id
    if error_type is not None:
        values["error_type"] = error_type
    if retryable is not None:
        values["retryable"] = retryable
    if not values:
        return
    await db.execute(
        update(StoryboardJob).where(StoryboardJob.job_id == job_id).values(**values)
    )


async def claim_job(
    db: AsyncSession,
    job_id: str,
    *,
    owner: str,
    now: datetime | None = None,
    lease_seconds: int = 300,
) -> str | None:
    """原子领取 pending/retry 或过期 running job，返回当前 lease token。"""
    current = now or datetime.now(UTC)
    row = await db.get(StoryboardJob, job_id, with_for_update=True)
    if row is None or row.canceled_at is not None:
        return None
    state = LeaseState(
        status=row.status,
        lease_owner=row.lease_owner,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        heartbeat_at=row.heartbeat_at,
    )
    token = claim_lease(state, owner, current, timedelta(seconds=lease_seconds))
    if token is None:
        return None
    row.status = state.status
    row.lease_owner = state.lease_owner
    row.lease_token = state.lease_token
    row.lease_until = state.lease_until
    row.heartbeat_at = state.heartbeat_at
    row.started_at = row.started_at or current
    await db.flush()
    return token


async def complete_job(
    db: AsyncSession,
    job_id: str,
    token: str,
    *,
    status: str,
    detail: dict | None = None,
    error_type: str | None = None,
    retryable: bool | None = None,
) -> bool:
    """仅当前 lease token 可以写终态，旧 token 不得覆盖。"""
    if status not in {"succeeded", "failed", "canceled"}:
        raise ValueError("status 必须是终态")
    row = await db.get(StoryboardJob, job_id, with_for_update=True)
    if row is None or row.status in {"succeeded", "failed", "canceled"}:
        return False
    state = LeaseState(
        status=row.status,
        lease_owner=row.lease_owner,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        heartbeat_at=row.heartbeat_at,
    )
    if status == "canceled":
        if row.status != "running" or row.lease_token != token:
            return False
    else:
        terminal = "done" if status == "succeeded" else status
        if not complete_lease(state, token, terminal):
            return False
    row.status = status
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    row.finished_at = datetime.now(UTC)
    if detail is not None:
        row.detail = detail
    if error_type is not None:
        row.error_type = error_type
    if retryable is not None:
        row.retryable = retryable
    await db.flush()
    return True


async def fail_job_if_current_token(
    db: AsyncSession,
    job_id: str,
    token: str,
    *,
    error_message: str,
    error_type: str | None = None,
    retryable: bool | None = None,
) -> bool:
    """仅当前 lease token 可以把运行中任务标记为 failed。"""
    row = await db.get(StoryboardJob, job_id, with_for_update=True)
    if row is None or row.status != "running" or row.lease_token != token:
        return False
    row.status = "failed"
    row.progress = 0
    row.progress_stage = "failed"
    row.progress_message = error_message
    row.error_message = error_message
    row.error_type = error_type
    if retryable is not None:
        row.retryable = retryable
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    row.finished_at = datetime.now(UTC)
    await db.flush()
    return True


async def heartbeat_job(
    db: AsyncSession,
    job_id: str,
    token: str,
    *,
    now: datetime | None = None,
    lease_seconds: int = 300,
) -> bool:
    """仅当前 lease token 可以续租，供长 Provider 调用期间心跳使用。"""
    from app.core.job_state import heartbeat_lease

    row = await db.get(StoryboardJob, job_id, with_for_update=True)
    if row is None:
        return False
    current = now or datetime.now(UTC)
    state = LeaseState(
        status=row.status,
        lease_owner=row.lease_owner,
        lease_token=row.lease_token,
        lease_until=row.lease_until,
        heartbeat_at=row.heartbeat_at,
    )
    if not heartbeat_lease(state, token, current, timedelta(seconds=lease_seconds)):
        return False
    row.lease_until = state.lease_until
    row.heartbeat_at = state.heartbeat_at
    await db.flush()
    return True


async def cancel_job(db: AsyncSession, job_id: str, *, reason: str | None = None) -> bool:
    """将未终态 job CAS 标记为 canceled。"""
    row = await db.get(StoryboardJob, job_id, with_for_update=True)
    if row is None:
        return False
    if row.status in {"succeeded", "failed", "canceled"}:
        return row.status == "canceled"
    row.status = "canceled"
    row.canceled_at = datetime.now(UTC)
    row.finished_at = row.canceled_at
    row.lease_owner = None
    row.lease_token = None
    row.lease_until = None
    detail = dict(row.detail or {})
    if reason:
        detail["cancel_reason"] = reason
    row.detail = detail
    await db.flush()
    return True


__all__ = [
    "create_job",
    "get_by_idempotency_key",
    "count_jobs_for_episode",
    "get_job",
    "get_latest_for_episode",
    "heartbeat_job",
    "list_jobs_for_episode",
    "next_attempt_for_episode",
    "get_running_for_episode",
    "update_job",
    "claim_job",
    "complete_job",
    "fail_job_if_current_token",
    "cancel_job",
]
