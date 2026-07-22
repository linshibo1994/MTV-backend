"""视频生成模块数据访问层 — VideoGenerationJob / VideoProviderConfig CRUD。"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import func, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.ids import new_id
from app.models.video_generation_job import (
    VideoGenerationIdempotencyRequest,
    VideoGenerationJob,
)
from app.models.video_provider import VideoProviderConfig

# 任务处于以下状态时视为「活动中」，幂等去重时复用而非新建。
# 以 doc04 §1.19 的 starting/processing 为准；旧 doc02 的 pending/running 不再使用，
# 前端若按旧枚举匹配需显式映射为 starting/processing。
_ACTIVE_STATUSES = ("starting", "processing")

# 终态：一旦进入这些状态，进度更新不得再覆盖（cancel 与 worker 收尾互不覆盖 canceled↔succeeded）。
_TERMINAL_STATUSES = ("succeeded", "failed", "canceled")
_PRESERVED_OUTPUT_DEBT_KEYS = (
    "attempt_output_cleanup",
    "provider_cancellation",
    "orphan_provider_cancellations",
)


@dataclass(frozen=True)
class VideoJobClaim:
    job_id: str
    lease_token: str
    attempt: int
    input_version: int


@dataclass(frozen=True)
class VideoIdempotencyBinding:
    request_hash: str
    job: VideoGenerationJob


def _merge_latest_video_json(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any]:
    """以数据库最新值为底合并，恢复债务不得被调用方旧快照覆盖。"""

    current_json = dict(current or {})
    merged = {**current_json, **dict(incoming or {})}
    for key in _PRESERVED_OUTPUT_DEBT_KEYS:
        if key in current_json:
            merged[key] = current_json[key]
        else:
            merged.pop(key, None)
    return merged


async def _lock_video_job_for_update(
    db: AsyncSession,
    job_id: str,
) -> VideoGenerationJob | None:
    return (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def create_video_job(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    scene_no: int,
    shot_group_id: str,
    owner_user_id: str | None,
    unit_id: str,
    unit_runtime_key: str,
    provider_key: str,
    job_type: str,
    model: str,
    idempotency_key: str,
    request_hash: str,
    input_version: int,
    attempt: int,
    reservation_id: str | None,
    input_json: dict,
) -> VideoGenerationJob:
    """创建视频生成任务，初始 status=starting、progress=0。

    积分 reserve 由 service 层负责，CRUD 只落库。
    """
    job = VideoGenerationJob(
        job_id=new_id(),
        script_id=script_id,
        episode_id=episode_id,
        shot_group_id=shot_group_id,
        owner_user_id=owner_user_id,
        scene_no=scene_no,
        unit_id=unit_id,
        unit_runtime_key=unit_runtime_key,
        provider_key=provider_key,
        job_type=job_type,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        input_version=input_version,
        attempt=attempt,
        reservation_id=reservation_id,
        status="starting",
        progress=0,
        progress_stage=None,
        progress_message=None,
        error_message=None,
        input_json=input_json,
        output_json=None,
        metrics={"model": model},
    )
    db.add(job)
    await db.flush()
    return job


async def get_video_job(db: AsyncSession, job_id: str) -> VideoGenerationJob | None:
    """按 job_id 取单个视频生成任务。"""
    result = await db.execute(
        select(VideoGenerationJob)
        .where(VideoGenerationJob.job_id == job_id)
        .options(noload(VideoGenerationJob.script))
    )
    return result.scalar_one_or_none()


async def find_idempotency_binding(
    db: AsyncSession, *, script_id: str, idempotency_key: str
) -> VideoIdempotencyBinding | None:
    result = await db.execute(
        select(VideoGenerationIdempotencyRequest.request_hash, VideoGenerationJob)
        .join(
            VideoGenerationJob,
            VideoGenerationJob.job_id == VideoGenerationIdempotencyRequest.job_id,
        )
        .where(
            VideoGenerationIdempotencyRequest.script_id == script_id,
            VideoGenerationIdempotencyRequest.idempotency_key == idempotency_key,
        )
        .options(noload(VideoGenerationJob.script))
        .limit(1)
    )
    row = result.one_or_none()
    if row is None:
        return None
    request_hash, job = row
    return VideoIdempotencyBinding(request_hash=request_hash, job=job)


async def bind_idempotency_request(
    db: AsyncSession,
    *,
    script_id: str,
    idempotency_key: str,
    request_hash: str,
    job_id: str,
) -> VideoGenerationIdempotencyRequest:
    binding = VideoGenerationIdempotencyRequest(
        script_id=script_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        job_id=job_id,
    )
    db.add(binding)
    await db.flush()
    return binding


async def list_video_jobs(
    db: AsyncSession,
    *,
    script_id: str | None = None,
    episode_id: str | None = None,
    limit: int = 30,
) -> list[VideoGenerationJob]:
    """按剧本/分集过滤视频生成任务，按创建时间倒序。

    limit 上限由调用方约束（API 层 500）。
    """
    stmt = select(VideoGenerationJob).order_by(
        VideoGenerationJob.created_at.desc()
    ).limit(limit)
    if script_id is not None:
        stmt = stmt.where(VideoGenerationJob.script_id == script_id)
    if episode_id is not None:
        stmt = stmt.where(VideoGenerationJob.episode_id == episode_id)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def count_video_jobs(
    db: AsyncSession,
    *,
    script_id: str | None = None,
    episode_id: str | None = None,
) -> int:
    """按剧本/分集统计匹配总数（不分页），供列表 total 字段使用。"""
    stmt = select(func.count(VideoGenerationJob.job_id))
    if script_id is not None:
        stmt = stmt.where(VideoGenerationJob.script_id == script_id)
    if episode_id is not None:
        stmt = stmt.where(VideoGenerationJob.episode_id == episode_id)
    result = await db.execute(stmt)
    return int(result.scalar() or 0)


async def find_active_job_by_unit(
    db: AsyncSession, unit_id: str
) -> VideoGenerationJob | None:
    """按 unit_id（group:<shot_group_id>）查最近一个活动中的任务。

    幂等去重：同一镜头组若已有 pending/running 任务，复用而非新建（doc 08 §4.2）。
    """
    stmt = (
        select(VideoGenerationJob)
        .where(
            VideoGenerationJob.unit_id == unit_id,
            VideoGenerationJob.status.in_(_ACTIVE_STATUSES),
        )
        .order_by(VideoGenerationJob.created_at.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def next_attempt_for_unit(db: AsyncSession, unit_id: str) -> int:
    """为同一视频生成单元分配新的不可变 attempt。"""
    result = await db.execute(
        select(func.coalesce(func.max(VideoGenerationJob.attempt), 0)).where(
            VideoGenerationJob.unit_id == unit_id
        )
    )
    return int(result.scalar() or 0) + 1


async def update_video_job_progress(
    db: AsyncSession,
    job_id: str,
    *,
    status: str | None = None,
    progress: int | None = None,
    progress_stage: str | None = None,
    progress_message: str | None = None,
    error_message: str | None = None,
    output_json: dict | None = None,
    metrics: dict | None = None,
    error_type: str | None = None,
    retryable: bool | None = None,
    expected_version: int | None = None,
    lease_token: str | None = None,
) -> bool:
    """更新视频生成任务进度/状态（worker 与 API 共用）。

    仅更新非 None 字段，避免覆盖。附带 version +1 乐观锁递增。
    CAS 与状态守卫：WHERE job_id=:id AND version=:old_version
    AND status NOT IN ('succeeded','failed','canceled')；affected==0 说明被并发覆盖
    或已进入终态，不覆盖终态（cancel 与 worker 收尾不互相覆盖 canceled↔succeeded）。
    返回 True 表示更新命中，False 表示并发覆盖/终态保护未写入。
    """
    current = await _lock_video_job_for_update(db, job_id)
    if current is None or current.status in _TERMINAL_STATUSES:
        return False
    if expected_version is not None and current.version != expected_version:
        return False
    if lease_token is not None and (
        current.status != "processing" or current.lease_token != lease_token
    ):
        return False

    values: dict = {"version": VideoGenerationJob.version + 1}
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
    if error_type is not None:
        values["error_type"] = error_type
    if retryable is not None:
        values["retryable"] = retryable
    if output_json is not None:
        values["output_json"] = _merge_latest_video_json(
            current.output_json,
            output_json,
        )
    if metrics is not None:
        values["metrics"] = _merge_latest_video_json(current.metrics, metrics)
    # 状态守卫：绝不覆盖已进入终态的任务
    conds = [
        VideoGenerationJob.job_id == job_id,
        VideoGenerationJob.status.not_in(_TERMINAL_STATUSES),
    ]
    # CAS：调用方传入期望的旧版本号时，增加乐观锁条件防并发覆写
    if expected_version is not None:
        conds.append(VideoGenerationJob.version == expected_version)
    else:
        conds.append(VideoGenerationJob.version == current.version)
    if lease_token is not None:
        conds.extend(
            [
                VideoGenerationJob.status == "processing",
                VideoGenerationJob.lease_token == lease_token,
            ]
        )
    result = cast(
        CursorResult[Any],
        await db.execute(update(VideoGenerationJob).where(*conds).values(**values)),
    )
    return result.rowcount == 1


# ===== Worker 租约/并发保护（doc08 §2.1 乐观锁 + §4.2 租约恢复）=====


async def claim_video_job(
    db: AsyncSession,
    job_id: str,
    *,
    owner: str,
    now: datetime | None = None,
    lease_seconds: int = 900,
    allow_stale_processing: bool = True,
) -> VideoJobClaim | None:
    """原子领取可执行任务；worker 入口可禁用过期 processing 直接接管。"""
    current = now or datetime.now(UTC)
    claimable = VideoGenerationJob.status == "starting"
    if allow_stale_processing:
        claimable = or_(
            claimable,
            (VideoGenerationJob.status == "processing")
            & (VideoGenerationJob.lease_until.is_not(None))
            & (VideoGenerationJob.lease_until <= current),
        )
    row = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                claimable,
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    from app.services.video_remote_cancellation import provider_submission_is_unresolved

    if provider_submission_is_unresolved(row.output_json, row.metrics):
        return None
    from app.core.ids import new_id

    token = new_id()
    result = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == row.job_id,
                VideoGenerationJob.status == row.status,
                VideoGenerationJob.lease_token == row.lease_token,
            )
            .values(
                status="processing",
                progress=max(row.progress or 0, 5),
                progress_stage="preparing",
                progress_message="准备视频生成参数",
                lease_owner=owner,
                lease_token=token,
                lease_until=current + timedelta(seconds=lease_seconds),
                heartbeat_at=current,
                started_at=row.started_at or current,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    await db.flush()
    if result.rowcount != 1:
        return None
    return VideoJobClaim(
        job_id=row.job_id,
        lease_token=token,
        attempt=row.attempt,
        input_version=row.input_version,
    )


async def claim_job_lease(
    db: AsyncSession,
    job_id: str,
    *,
    owner: str,
    lease_seconds: int = 900,
) -> bool:
    return await claim_video_job(db, job_id, owner=owner, lease_seconds=lease_seconds) is not None


async def heartbeat_video_job(
    db: AsyncSession,
    job_id: str,
    lease_token: str,
    *,
    now: datetime | None = None,
    lease_seconds: int = 900,
) -> bool:
    current = now or datetime.now(UTC)
    result = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "processing",
                VideoGenerationJob.lease_token == lease_token,
            )
            .values(
                heartbeat_at=current,
                lease_until=current + timedelta(seconds=lease_seconds),
            )
        ),
    )
    await db.flush()
    return result.rowcount == 1


async def finish_video_job(
    db: AsyncSession,
    job_id: str,
    lease_token: str,
    status: str,
    *,
    output_json: dict | None = None,
    metrics: dict | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
) -> bool:
    if status not in _TERMINAL_STATUSES:
        raise ValueError("无效视频任务终态")
    current = await _lock_video_job_for_update(db, job_id)
    if (
        current is None
        or current.status != "processing"
        or current.lease_token != lease_token
    ):
        return False
    values: dict = {
        "status": status,
        "lease_owner": None,
        "lease_token": None,
        "lease_until": None,
        "finished_at": datetime.now(UTC),
        "version": VideoGenerationJob.version + 1,
    }
    if status == "succeeded":
        values.update(
            progress=100,
            progress_stage="succeeded",
            progress_message="视频生成完成",
        )
    elif status == "failed":
        values.update(
            progress_stage="failed",
            progress_message="视频生成失败",
        )
    elif status == "canceled":
        values.update(
            progress_stage="canceled",
            progress_message="用户取消",
            canceled_at=datetime.now(UTC),
        )
    if output_json is not None:
        values["output_json"] = _merge_latest_video_json(
            current.output_json,
            output_json,
        )
    if metrics is not None:
        values["metrics"] = _merge_latest_video_json(current.metrics, metrics)
    if error_type is not None:
        values["error_type"] = error_type
    if error_message is not None:
        values["error_message"] = error_message[:1024]
    if retryable is not None:
        values["retryable"] = retryable
    result = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "processing",
                VideoGenerationJob.lease_token == lease_token,
                VideoGenerationJob.version == current.version,
            )
            .values(**values)
        ),
    )
    await db.flush()
    return result.rowcount == 1


async def list_stale_processing_jobs(
    db: AsyncSession, *, lease_seconds: int = 900, limit: int = 50
) -> list[VideoGenerationJob]:
    """扫描租约过期但仍处于 starting/processing 的卡死任务。

    doc08 §4.2：worker 崩溃后任务可经租约恢复。这些任务的 leased_until 已过期，
    可由恢复扫描重新领取租约并补跑。
    """
    current = datetime.now(UTC)
    starting_cutoff = current - timedelta(seconds=lease_seconds)
    stmt = (
        select(VideoGenerationJob)
        .where(
            or_(
                (VideoGenerationJob.status == "processing")
                & (VideoGenerationJob.lease_until.is_not(None))
                & (VideoGenerationJob.lease_until < current),
                (VideoGenerationJob.status == "starting")
                & (VideoGenerationJob.updated_at < starting_cutoff),
            )
        )
        .order_by(VideoGenerationJob.updated_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


# ===== 供应商配置 =====

async def get_active_provider(
    db: AsyncSession, job_type: str = "all_reference", *, commit_on_seed: bool = False
) -> VideoProviderConfig | None:
    """取当前激活的视频供应商配置。无激活记录时返回 None（调用方兜底默认）。

    若配置表为空（ensure_provider_configs 从未被启动钩子调用），惰性幂等播种一次
    再重查，避免 active-provider 接口恒走默认分支、切换 supplier 失效（doc08）。
    """
    result = await db.execute(
        select(VideoProviderConfig).where(
            VideoProviderConfig.job_type == job_type,
            VideoProviderConfig.is_active.is_(True),
        )
    )
    cfg = result.scalar_one_or_none()
    if cfg is not None:
        return cfg
    # 表为空 → 幂等播种后重查
    any_row = await db.execute(
        select(VideoProviderConfig.id).where(VideoProviderConfig.job_type == job_type).limit(1)
    )
    if any_row.scalar_one_or_none() is None:
        await ensure_provider_configs(db)
        if commit_on_seed:
            await db.commit()
        else:
            await db.flush()
    result = await db.execute(
        select(VideoProviderConfig).where(
            VideoProviderConfig.job_type == job_type,
            VideoProviderConfig.is_active.is_(True),
        )
    )
    return result.scalar_one_or_none()


async def get_provider_by_key(
    db: AsyncSession, provider_key: str, job_type: str = "all_reference"
) -> VideoProviderConfig | None:
    """按 provider_key + job_type 取配置（无论是否激活）。"""
    result = await db.execute(
        select(VideoProviderConfig).where(
            VideoProviderConfig.provider_key == provider_key,
            VideoProviderConfig.job_type == job_type,
        )
    )
    return result.scalar_one_or_none()


async def set_active_provider(
    db: AsyncSession, *, provider_key: str, job_type: str = "all_reference"
) -> VideoProviderConfig:
    """切换激活供应商：先将同 job_type 的全部配置置为 inactive，再激活目标。

    若目标配置不存在则抛错（调用方处理）。返回激活后的配置。
    """
    # 同 job_type 全部置为 inactive
    await db.execute(
        update(VideoProviderConfig)
        .where(VideoProviderConfig.job_type == job_type)
        .values(is_active=False)
    )
    # 激活目标
    await db.execute(
        update(VideoProviderConfig)
        .where(
            VideoProviderConfig.provider_key == provider_key,
            VideoProviderConfig.job_type == job_type,
        )
        .values(is_active=True)
    )
    await db.flush()
    cfg = await get_provider_by_key(db, provider_key, job_type)
    return cfg  # type: ignore[return-value]


async def ensure_provider_configs(db: AsyncSession) -> None:
    """幂等初始化视频供应商配置，默认由 video_default_provider 决定激活项。

    由应用启动或首次访问触发；已存在不重复插入。
    """
    from app.core.config import settings

    provider_models = {
        "jianyu": settings.seedance_model,
        "modelsai": settings.seedance_model,
        "bailian": settings.dashscope_video_model,
        "openai": settings.openai_video_model,
        "gemini": settings.google_video_model,
    }
    for key, model in provider_models.items():
        existing = await get_provider_by_key(db, key, "all_reference")
        if existing is None:
            cfg = VideoProviderConfig(
                provider_key=key,
                job_type="all_reference",
                model=model,
                is_active=(key == settings.video_default_provider),
            )
            db.add(cfg)
    await db.flush()
