"""时间线编排数据访问层。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.core.job_state import PipelineFailure
from app.models.episode import Episode
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.timeline_render_job import TimelineRenderJob
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_timeline_arrangement import VideoTimelineArrangement

_TERMINAL_RENDER_STATUSES = {"succeeded", "failed", "canceled"}


@dataclass(frozen=True)
class RenderJobClaim:
    job_id: str
    lease_token: str
    attempt: int
    input_version: int


def _is_expired(value: datetime | None, now: datetime) -> bool:
    if value is None:
        return True
    comparable_now = now if value.tzinfo is not None else now.replace(tzinfo=None)
    return comparable_now >= value


def sanitize_render_error(exc: Exception) -> tuple[str, str, bool]:
    """将内部异常收敛为安全、稳定的错误类型与展示文案。"""

    failure = PipelineFailure.from_exception(exc)
    return failure.error_type, failure.error_message, failure.retryable


async def assert_episode_belongs(
    db: AsyncSession, *, script_id: str, episode_id: str
) -> Episode:
    """确认 episode 存在且属于 script；跨剧本按 403 处理。"""

    result = await db.execute(
        select(Episode)
        .where(Episode.episode_id == episode_id)
        .options(noload(Episode.script), noload(Episode.scenes))
    )
    episode = result.scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.script_id != script_id:
        raise ForbiddenError("分集不属于该剧本")
    return episode


async def validate_video_job_for_timeline(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    job_id: str,
    require_succeeded: bool = False,
    unit_runtime_key: str | None = None,
) -> VideoGenerationJob:
    """确认视频 job 存在、归属当前剧本/分集，并按需要求成功。"""

    result = await db.execute(
        select(VideoGenerationJob)
        .where(VideoGenerationJob.job_id == job_id)
        .options(noload(VideoGenerationJob.script))
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise NotFoundError("视频生成任务不存在")
    if job.script_id != script_id or job.episode_id != episode_id:
        raise ForbiddenError("视频生成任务不属于该剧本分集")
    if require_succeeded and job.status != "succeeded":
        raise ConflictError("视频生成任务尚未成功")
    if unit_runtime_key is not None and job.unit_runtime_key != unit_runtime_key:
        raise ConflictError("视频生成任务 unit_runtime_key 与时间线片段不一致")
    return job


async def list_arrangements(
    db: AsyncSession, script_id: str, episode_id: str
) -> list[VideoTimelineArrangement]:
    """按 script_id + episode_id 查询时间线，按 row_order 升序返回。"""

    await assert_episode_belongs(db, script_id=script_id, episode_id=episode_id)
    result = await db.execute(
        select(VideoTimelineArrangement)
        .where(
            VideoTimelineArrangement.script_id == script_id,
            VideoTimelineArrangement.episode_id == episode_id,
        )
        .order_by(VideoTimelineArrangement.row_order.asc())
    )
    return list(result.scalars().all())


async def get_arrangement(
    db: AsyncSession, arrangement_id: str
) -> VideoTimelineArrangement | None:
    """按主键查单个片段。"""

    result = await db.execute(
        select(VideoTimelineArrangement).where(
            VideoTimelineArrangement.id == arrangement_id
        ).execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def add_arrangement(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    unit_runtime_key: str,
    job_id: str,
) -> VideoTimelineArrangement:
    """添加片段到时间线末尾，row_order 取当前最大值 +1。"""

    await assert_episode_belongs(db, script_id=script_id, episode_id=episode_id)
    await validate_video_job_for_timeline(
        db,
        script_id=script_id,
        episode_id=episode_id,
        job_id=job_id,
        require_succeeded=True,
        unit_runtime_key=unit_runtime_key,
    )
    rows = await db.execute(
        select(VideoTimelineArrangement)
        .where(
            VideoTimelineArrangement.script_id == script_id,
            VideoTimelineArrangement.episode_id == episode_id,
        )
        .order_by(VideoTimelineArrangement.row_order.asc())
        .with_for_update()
    )
    current = list(rows.scalars().all())
    next_order = max((row.row_order for row in current), default=-1) + 1
    arrangement = VideoTimelineArrangement(
        id=new_id(),
        script_id=script_id,
        episode_id=episode_id,
        unit_runtime_key=unit_runtime_key,
        job_id=job_id,
        row_order=next_order,
        is_selected=True,
        trim_start_seconds=0.0,
        trim_end_seconds=None,
        version=0,
    )
    db.add(arrangement)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("时间线行序冲突，请刷新后重试") from exc
    return arrangement


async def update_arrangement(
    db: AsyncSession,
    arrangement: VideoTimelineArrangement,
    *,
    row_order: int | None = None,
    is_selected: bool | None = None,
    trim_start_seconds: float | None = None,
    trim_end_seconds: float | None = None,
    expected_version: int | None = None,
    fields_set: set[str] | None = None,
) -> VideoTimelineArrangement:
    """部分更新单个片段，支持区分 omitted 与显式 null。"""

    if expected_version is None:
        raise ConflictError("expected_version 必填")
    has = fields_set.__contains__ if fields_set is not None else (lambda name: True)

    current = await get_arrangement(db, arrangement.id)
    if current is None:
        raise NotFoundError("时间线片段不存在")
    new_values: dict[str, Any] = {}
    if has("row_order") and row_order is not None:
        new_values["row_order"] = row_order
    if has("is_selected") and is_selected is not None:
        new_values["is_selected"] = is_selected
    if has("trim_start_seconds") and trim_start_seconds is not None:
        new_values["trim_start_seconds"] = trim_start_seconds
    if has("trim_end_seconds"):
        new_values["trim_end_seconds"] = trim_end_seconds
    new_start = new_values.get("trim_start_seconds", current.trim_start_seconds)
    new_end = new_values.get("trim_end_seconds", current.trim_end_seconds)
    if (
        new_end is not None
        and new_end <= new_start
    ):
        raise ConflictError("裁出点必须晚于裁入点")
    new_values["version"] = VideoTimelineArrangement.version + 1
    try:
        result = cast(
            CursorResult[Any],
            await db.execute(
                sa_update(VideoTimelineArrangement)
                .where(
                    VideoTimelineArrangement.id == arrangement.id,
                    VideoTimelineArrangement.version == expected_version,
                )
                .values(**new_values)
                .execution_options(synchronize_session=False)
            ),
        )
        if result.rowcount != 1:
            raise ConflictError("时间线片段版本冲突，请刷新后重试")
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("时间线片段版本或行序冲突，请刷新后重试") from exc
    updated = await get_arrangement(db, arrangement.id)
    if updated is None:
        raise NotFoundError("时间线片段不存在")
    return updated


async def reorder(
    db: AsyncSession,
    items: list[dict],
    *,
    script_id: str,
    episode_id: str,
) -> list[VideoTimelineArrangement]:
    """批量重排序，重复 row_order、缺失 id 或跨范围 id 均返回 409/403。"""

    if not items:
        return []
    await assert_episode_belongs(db, script_id=script_id, episode_id=episode_id)
    ids = [str(it["id"]) for it in items]
    row_orders = [int(it["row_order"]) for it in items]
    if len(set(ids)) != len(ids) or len(set(row_orders)) != len(row_orders):
        raise ConflictError("时间线重排序存在重复 id 或 row_order")
    if any("expected_version" not in it for it in items):
        raise ConflictError("expected_version 必填")
    all_result = await db.execute(
        select(VideoTimelineArrangement)
        .where(
            VideoTimelineArrangement.script_id == script_id,
            VideoTimelineArrangement.episode_id == episode_id,
        )
        .order_by(VideoTimelineArrangement.row_order.asc())
        .with_for_update()
    )
    all_rows = list(all_result.scalars().all())
    rows = [row for row in all_rows if row.id in set(ids)]
    if len(rows) != len(ids):
        raise ForbiddenError("时间线片段不属于该剧本分集")
    rows_by_id = {row.id: row for row in rows}
    for item in items:
        row = rows_by_id[str(item["id"])]
        if int(item["expected_version"]) != int(row.version or 0):
            raise ConflictError("时间线片段版本冲突，请刷新后重试")
    max_order = max((row.row_order for row in all_rows), default=-1)
    try:
        for index, item_id in enumerate(ids, start=1):
            rows_by_id[item_id].row_order = max_order + index
        await db.flush()
        for item in items:
            row = rows_by_id[str(item["id"])]
            row.row_order = int(item["row_order"])
            row.version = (row.version or 0) + 1
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("时间线行序冲突，请刷新后重试") from exc
    return sorted(rows, key=lambda row: row.row_order)


async def remove_arrangement(
    db: AsyncSession, arrangement: VideoTimelineArrangement
) -> None:
    """删除单个片段。"""

    await db.delete(arrangement)
    await db.flush()


async def get_render_job(db: AsyncSession, job_id: str) -> TimelineRenderJob | None:
    result = await db.execute(
        select(TimelineRenderJob)
        .where(TimelineRenderJob.job_id == job_id)
        .options(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        )
    )
    return result.scalar_one_or_none()


async def find_render_job_by_idempotency(
    db: AsyncSession, *, owner_user_id: str, idempotency_key: str
) -> TimelineRenderJob | None:
    result = await db.execute(
        select(TimelineRenderJob)
        .where(
            TimelineRenderJob.owner_user_id == owner_user_id,
            TimelineRenderJob.idempotency_key == idempotency_key,
        )
        .options(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        )
    )
    return result.scalar_one_or_none()


async def create_render_job(
    db: AsyncSession,
    *,
    job_id: str,
    script_id: str,
    episode_id: str,
    owner_user_id: str,
    idempotency_key: str,
    request_hash: str,
    input_version: int,
    input_json: dict,
    output_storage_key: str,
    reservation_id: str | None = None,
    attempt: int = 1,
) -> TimelineRenderJob:
    job = TimelineRenderJob(
        job_id=job_id,
        script_id=script_id,
        episode_id=episode_id,
        owner_user_id=owner_user_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        input_version=input_version,
        attempt=attempt,
        reservation_id=reservation_id,
        status="starting",
        progress=0,
        progress_message="已排队，等待合成",
        input_json=input_json,
        output_storage_key=output_storage_key,
    )
    db.add(job)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("渲染任务幂等键冲突") from exc
    return job


async def create_retry_render_job(
    db: AsyncSession,
    *,
    source: TimelineRenderJob,
    job_id: str,
    owner_user_id: str,
    idempotency_key: str,
    output_storage_key: str,
    reservation_id: str | None,
) -> TimelineRenderJob:
    """基于 retryable 失败任务创建新的不可变 attempt。"""

    job = TimelineRenderJob(
        job_id=job_id,
        script_id=source.script_id,
        episode_id=source.episode_id,
        owner_user_id=owner_user_id,
        idempotency_key=idempotency_key,
        request_hash=source.request_hash,
        input_version=source.input_version,
        attempt=int(source.attempt or 1) + 1,
        reservation_id=reservation_id,
        status="starting",
        progress=0,
        progress_message="已排队，等待重试合成",
        input_json=dict(source.input_json or {}),
        output_storage_key=output_storage_key,
    )
    db.add(job)
    try:
        await db.flush()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("渲染任务重试冲突") from exc
    return job


async def claim_render_job(
    db: AsyncSession,
    job_id: str,
    *,
    owner: str,
    lease_seconds: int = 300,
) -> RenderJobClaim | None:
    """原子领取 starting 或租约过期的 processing render job。"""

    now = datetime.now(UTC)
    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status in _TERMINAL_RENDER_STATUSES:
        return None
    if row.status == "processing" and not _is_expired(row.leased_until, now):
        return None
    token = new_id()
    row.status = "processing"
    row.progress = max(row.progress or 0, 1)
    row.progress_stage = "processing"
    row.progress_message = "开始合成"
    row.lease_owner = owner
    row.lease_token = token
    row.leased_until = now + timedelta(seconds=lease_seconds)
    row.heartbeat_at = now
    row.started_at = row.started_at or now
    await db.flush()
    return RenderJobClaim(
        job_id=row.job_id,
        lease_token=token,
        attempt=row.attempt,
        input_version=row.input_version,
    )


async def heartbeat_render_job(
    db: AsyncSession,
    job_id: str,
    lease_token: str,
    *,
    lease_seconds: int = 300,
    progress: int | None = None,
    progress_stage: str | None = None,
    progress_message: str | None = None,
) -> bool:
    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status != "processing" or row.lease_token != lease_token:
        return False
    now = datetime.now(UTC)
    row.leased_until = now + timedelta(seconds=lease_seconds)
    row.heartbeat_at = now
    if progress is not None:
        row.progress = progress
    if progress_stage is not None:
        row.progress_stage = progress_stage
    if progress_message is not None:
        row.progress_message = progress_message
    await db.flush()
    return True


async def finish_render_job(
    db: AsyncSession,
    job_id: str,
    lease_token: str,
    *,
    status: str,
    output_storage_key: str | None = None,
    output_json: dict | None = None,
    error: Exception | None = None,
) -> bool:
    """仅当前 lease token 可以写终态。"""

    if status not in _TERMINAL_RENDER_STATUSES:
        raise ValueError("无效 TimelineRenderJob 终态")
    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status in _TERMINAL_RENDER_STATUSES:
        return False
    if row.status != "processing" or row.lease_token != lease_token:
        return False
    row.status = status
    row.finished_at = datetime.now(UTC)
    row.lease_owner = None
    row.lease_token = None
    row.leased_until = None
    if status == "succeeded":
        row.progress = 100
        row.progress_stage = "succeeded"
        row.progress_message = "合成完成"
        if output_storage_key is not None:
            row.output_storage_key = output_storage_key
        row.output_json = output_json or {"storage_key": row.output_storage_key}
    elif status == "canceled":
        row.canceled_at = row.finished_at
        row.progress_stage = "canceled"
        row.progress_message = "用户已取消合成"
    else:
        row.progress_stage = "failed"
        row.progress_message = "合成失败"
        if error is not None:
            row.error_type, row.error_message, row.retryable = sanitize_render_error(error)
    await db.flush()
    return True


def apply_render_job_terminal_state(
    row: TimelineRenderJob,
    *,
    status: str,
    output_storage_key: str | None = None,
    output_json: dict | None = None,
    error: Exception | None = None,
) -> None:
    """在已加锁的 render job 上写入终态。"""

    if status not in _TERMINAL_RENDER_STATUSES:
        raise ValueError("无效 TimelineRenderJob 终态")
    row.status = status
    row.finished_at = datetime.now(UTC)
    row.lease_owner = None
    row.lease_token = None
    row.leased_until = None
    if status == "succeeded":
        row.progress = 100
        row.progress_stage = "succeeded"
        row.progress_message = "合成完成"
        if output_storage_key is not None:
            row.output_storage_key = output_storage_key
        row.output_json = output_json or {"storage_key": row.output_storage_key}
    elif status == "canceled":
        row.canceled_at = row.finished_at
        row.progress_stage = "canceled"
        row.progress_message = "用户已取消合成"
        if output_json is not None:
            row.output_json = output_json
    else:
        row.progress_stage = "failed"
        row.progress_message = "合成失败"
        if output_json is not None:
            row.output_json = output_json
        if error is not None:
            row.error_type, row.error_message, row.retryable = sanitize_render_error(error)


async def lock_current_render_job_for_finish(
    db: AsyncSession, job_id: str, lease_token: str
) -> TimelineRenderJob | None:
    """按统一锁序先锁 job，并确认当前 lease 仍可写终态。"""

    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status in _TERMINAL_RENDER_STATUSES:
        return None
    if row.status != "processing" or row.lease_token != lease_token:
        return None
    return row


async def lock_cancelable_render_job(
    db: AsyncSession, job_id: str
) -> TimelineRenderJob | None:
    """锁定尚未进入终态、可由用户取消的 render job。"""

    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status in _TERMINAL_RENDER_STATUSES:
        return None
    return row


async def mark_locked_render_job_terminal(
    db: AsyncSession,
    row: TimelineRenderJob,
    *,
    status: str,
    output_storage_key: str | None = None,
    output_json: dict | None = None,
    error: Exception | None = None,
) -> None:
    apply_render_job_terminal_state(
        row,
        status=status,
        output_storage_key=output_storage_key,
        output_json=output_json,
        error=error,
    )
    await db.flush()


async def cancel_render_job(db: AsyncSession, job_id: str) -> TimelineRenderJob | None:
    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None:
        return None
    if row.status in _TERMINAL_RENDER_STATUSES:
        return row
    row.status = "canceled"
    row.canceled_at = datetime.now(UTC)
    row.finished_at = row.canceled_at
    row.lease_owner = None
    row.lease_token = None
    row.leased_until = None
    row.progress_stage = "canceled"
    row.progress_message = "用户已取消合成"
    await db.flush()
    return row


async def has_current_render_lease(
    db: AsyncSession, job_id: str, lease_token: str
) -> bool:
    row = await db.get(
        TimelineRenderJob,
        job_id,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    return row is not None and row.status == "processing" and row.lease_token == lease_token


async def list_stale_render_jobs(
    db: AsyncSession, *, limit: int = 50
) -> list[TimelineRenderJob]:
    now = datetime.now(UTC)
    result = await db.execute(
        select(TimelineRenderJob)
        .where(
            TimelineRenderJob.status.in_(("starting", "processing")),
            (TimelineRenderJob.leased_until.is_(None))
            | (TimelineRenderJob.leased_until <= now),
        )
        .order_by(TimelineRenderJob.updated_at.asc())
        .limit(limit)
        .options(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        )
    )
    return list(result.scalars().all())


async def list_pending_render_financial_cleanup_ids(
    db: AsyncSession, *, limit: int = 50
) -> list[str]:
    """列出终态 job 上待补偿的财务 release 债务。"""

    result = await db.execute(
        select(TimelineRenderJob)
        .where(
            TimelineRenderJob.status.in_(("failed", "canceled")),
            TimelineRenderJob.output_json.is_not(None),
        )
        .order_by(TimelineRenderJob.updated_at.asc())
        .limit(limit)
        .options(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        )
    )
    job_ids: list[str] = []
    for job in result.scalars().all():
        cleanup = (job.output_json or {}).get("financial_cleanup")
        if (
            isinstance(cleanup, dict)
            and cleanup.get("status") == "pending"
            and cleanup.get("action") == "release"
            and cleanup.get("reservation_id")
        ):
            job_ids.append(job.job_id)
    return job_ids


async def lock_pending_render_financial_cleanup(
    db: AsyncSession, job_id: str
) -> TimelineRenderJob | None:
    row = await db.get(
        TimelineRenderJob,
        job_id,
        with_for_update=True,
        options=(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        ),
    )
    if row is None or row.status not in {"failed", "canceled"}:
        return None
    cleanup = (row.output_json or {}).get("financial_cleanup")
    if (
        not isinstance(cleanup, dict)
        or cleanup.get("status") != "pending"
        or cleanup.get("action") != "release"
        or not cleanup.get("reservation_id")
    ):
        return None
    return row


async def reconcile_stale_render_jobs(
    db: AsyncSession, *, limit: int = 50
) -> int:
    """恢复 starting/过期 processing/缺 outbox 的 timeline.render dispatch。"""

    now = datetime.now(UTC)
    result = await db.execute(
        select(TimelineRenderJob)
        .where(TimelineRenderJob.status.in_(("starting", "processing")))
        .order_by(TimelineRenderJob.updated_at.asc())
        .limit(limit)
        .options(
            noload(TimelineRenderJob.script),
            noload(TimelineRenderJob.episode),
            noload(TimelineRenderJob.owner),
        )
    )
    recovered = 0
    for job in list(result.scalars().all()):
        expired_processing = (
            job.status == "processing" and _is_expired(job.leased_until, now)
        )
        starting = job.status == "starting"
        outbox = (
            await db.execute(
                select(TaskDispatchOutbox)
                .where(
                    TaskDispatchOutbox.task_type == "timeline.render",
                    TaskDispatchOutbox.job_id == job.job_id,
                    TaskDispatchOutbox.attempt == job.attempt,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        missing_outbox = outbox is None
        if not (starting or expired_processing or missing_outbox):
            continue
        if expired_processing:
            old_token = job.lease_token
            update_result = cast(
                CursorResult[Any],
                await db.execute(
                    sa_update(TimelineRenderJob)
                    .where(
                        TimelineRenderJob.job_id == job.job_id,
                        TimelineRenderJob.status == "processing",
                        TimelineRenderJob.lease_token == old_token,
                        (TimelineRenderJob.leased_until.is_(None))
                        | (TimelineRenderJob.leased_until <= now),
                    )
                    .values(
                        status="starting",
                        progress_stage="starting",
                        progress_message="等待重新合成",
                        lease_owner=None,
                        lease_token=None,
                        leased_until=None,
                    )
                    .execution_options(synchronize_session=False)
                ),
            )
            if update_result.rowcount != 1:
                continue
        if outbox is None:
            outbox = TaskDispatchOutbox(
                task_type="timeline.render",
                job_id=job.job_id,
                attempt=job.attempt,
                payload={
                    "job_id": job.job_id,
                    "script_id": job.script_id,
                    "episode_id": job.episode_id,
                    "dispatch_id": new_id(),
                    "run_id": job.job_id,
                },
                status="pending",
                available_at=now,
            )
            db.add(outbox)
        else:
            outbox.status = "pending"
            outbox.available_at = now
            outbox.published_at = None
            outbox.lease_owner = None
            outbox.lease_token = None
            outbox.lease_until = None
            outbox.last_error = None
        job.dispatch_id = outbox.id
        recovered += 1
    await db.flush()
    return recovered


def render_job_to_status(job: TimelineRenderJob) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress": job.progress,
        "progress_stage": job.progress_stage,
        "progress_message": job.progress_message,
        "error_message": job.error_message,
        "error_type": job.error_type,
        "retryable": job.retryable,
        "storage_key": job.output_storage_key if job.status == "succeeded" else None,
        "output_storage_key": job.output_storage_key,
    }
