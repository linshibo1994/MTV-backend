"""分镜模块 API 路由。

端点:
- POST /scripts/<sid>/episodes/<eid>/generate-storyboard  触发 LLM 分镜生成 (异步)
- GET  /scripts/<sid>/episodes/<eid>/shot-hierarchy      分镜层级 (场景->镜头组->镜头)
- POST /scripts/<sid>/episodes/<eid>/shot-groups/merge   自动组合本集全部小分镜
- PATCH /scripts/<sid>/episodes/<eid>/shot-groups/<gid>  编辑正式镜头组
- DELETE /scripts/<sid>/episodes/<eid>/shot-groups/<gid> 删除正式镜头组及关联视频
- GET  /shots/<sid>/episodes/<eid>/video-workbench        视频工作台 (含 unit_id/materials)
- GET  /shots/<script_id>/cover                           视频项目封面 (首个镜头首帧)
"""

from __future__ import annotations

import json
import logging
from hashlib import sha256

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.errors import ConflictError, ForbiddenError, InsufficientCreditsError
from app.core.permissions import Action, check_script_access
from app.crud.shot import get_cover, get_shot_hierarchy, get_video_workbench
from app.crud.storyboard import (
    cancel_job,
    count_jobs_for_episode,
    create_job,
    get_by_idempotency_key,
    get_job,
    get_latest_for_episode,
    get_running_for_episode,
    list_jobs_for_episode,
    next_attempt_for_episode,
    update_job,
)
from app.models.credit import CreditAccount
from app.models.episode import Episode
from app.models.script import Script
from app.models.storyboard_job import STORYBOARD_IDEMPOTENCY_KEY_MAX_LENGTH, StoryboardJob
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.schemas.shot import (
    CoverOut,
    GenerateStoryboardResponse,
    ShotGroupMergeIn,
    ShotGroupUpdateIn,
    ShotHierarchyOut,
    StoryboardJobEditIn,
    StoryboardJobListOut,
    StoryboardStatusOut,
    VideoWorkbenchOut,
)
from app.services.audit import best_effort_log as audit_log
from app.services.credits import release, reserve
from app.services.shot_group_delete import (
    cancel_active_video_jobs_for_shot_group,
)
from app.services.shot_group_delete import (
    delete_shot_group as delete_shot_group_safely,
)
from app.services.shot_group_edit import update_shot_group
from app.services.shot_group_merge import merge_shots_into_group
from app.services.task_dispatch import enqueue_task

logger = logging.getLogger(__name__)

router = APIRouter()

STORYBOARD_OUTBOX_TASK_TYPE = "storyboard.generate"


def _require_idempotency_key(raw: str) -> str:
    key = (raw or "").strip()
    if not key:
        raise ConflictError("Idempotency-Key 必填")
    return key


def storyboard_request_hash(
    *, script_id: str, episode_id: str, episode_version: int, client_save_seq: int
) -> str:
    """规范化客户端创建载荷，得到稳定幂等 hash。"""
    payload = {
        "episode_id": episode_id,
        "episode_version": episode_version,
        "client_save_seq": client_save_seq,
        "script_id": script_id,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def storyboard_outbox_payload(
    *,
    script_id: str,
    episode_id: str,
    episode_version: int,
    client_save_seq: int,
    user_id: str,
    job_id: str,
) -> dict:
    """分镜 outbox payload 签名；共享 publisher 由主会话串行接线。"""
    return {
        "script_id": script_id,
        "episode_id": episode_id,
        "episode_version": episode_version,
        "input_version": episode_version,
        "input_client_save_seq": client_save_seq,
        "user_id": user_id,
        "job_id": job_id,
    }


def _job_status_out(job) -> StoryboardStatusOut:
    return StoryboardStatusOut(
        job_id=job.job_id,
        task_id=job.task_id,
        episode_id=job.episode_id,
        script_id=job.script_id,
        status=job.status,
        progress=job.progress,
        progress_stage=job.progress_stage,
        progress_message=job.progress_message,
        error_message=job.error_message,
        detail=job.detail,
    )


# ---------------------------------------------------------------------------
# 辅助: 校验剧本与分集归属
# ---------------------------------------------------------------------------


async def _validate_script_episode(
    db: AsyncSession, user: User, script_id: str, episode_id: str,
    action: Action = Action.READ,
    expected_episode_version: int | None = None,
) -> tuple[Script, Episode]:
    """校验剧本访问权限 + 分集归属, 返回 (script, episode)。

    若传入 expected_episode_version, 则比对 episode.episode_version,
    不匹配时抛 ConflictError (08§2.1/AC-SE03 乐观锁: 过期版本不覆盖)。
    """
    script_result = await db.execute(select(Script).where(Script.id == script_id))
    script = script_result.scalar_one_or_none()
    if script is None:
        raise HTTPException(status_code=404, detail="剧本不存在")
    await check_script_access(db, user, script, action=action)

    ep_result = await db.execute(
        select(Episode).where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
    )
    episode = ep_result.scalar_one_or_none()
    if episode is None:
        any_ep_result = await db.execute(
            select(Episode).where(Episode.episode_id == episode_id)
        )
        if any_ep_result.scalar_one_or_none() is not None:
            raise ForbiddenError("分集不属于当前剧本")
        raise HTTPException(status_code=404, detail="分集不存在")

    # 乐观锁: 客户端持有的 episode_version 与服务端不一致则拒绝 (避免并发覆盖)
    if (
        expected_episode_version is not None
        and episode.episode_version != expected_episode_version
    ):
        raise ConflictError("episode_version 冲突")

    return script, episode


async def _get_authorized_storyboard_job(
    db: AsyncSession,
    user: User,
    script_id: str,
    episode_id: str,
    job_id: str,
    *,
    action: Action,
):
    """全局 job 不存在返回 404；存在但不属于当前 script/episode 返回 403。"""
    await _validate_script_episode(db, user, script_id, episode_id, action=action)
    job = await get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="分镜任务不存在")
    if job.script_id != script_id or job.episode_id != episode_id:
        raise ForbiddenError("分镜任务不属于当前分集")
    return job


# ---------------------------------------------------------------------------
# POST /scripts/<sid>/episodes/<eid>/generate-storyboard
# ---------------------------------------------------------------------------


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/generate-storyboard",
    response_model=GenerateStoryboardResponse,
    summary="触发分镜生成",
    description="为指定分集触发 LLM-5 分镜生成 (异步 Celery 任务)。产出 场景->镜头组->镜头。",
)
async def generate_storyboard(
    script_id: str,
    episode_id: str,
    episode_version: int,
    idempotency_key: str = Header(
        ...,
        alias="Idempotency-Key",
        max_length=STORYBOARD_IDEMPOTENCY_KEY_MAX_LENGTH,
    ),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> GenerateStoryboardResponse:
    """触发分镜生成。

    需要写权限。投递 Celery worker 异步执行, 返回 task_id/job_id 供前端轮询。

    - episode_version: 客户端持有的乐观锁版本, 与服务端不一致时返回 409 冲突 (AC-SE03)。
    - 并发去重: 同一分集已有 pending/running 分镜任务时拒绝重复投递 (08§4.2)。
    """
    # 校验权限 + 分集归属 + 乐观锁版本
    script, episode = await _validate_script_episode(
        db, user, script_id, episode_id,
        action=Action.RUN_AI,
        expected_episode_version=episode_version,
    )

    if not episode.script_text:
        raise ConflictError("分集无剧本内容, 无法生成分镜")

    idempotency_key = _require_idempotency_key(idempotency_key)
    request_hash = storyboard_request_hash(
        script_id=script_id,
        episode_id=episode_id,
        episode_version=episode.episode_version,
        client_save_seq=episode.client_save_seq,
    )
    existing_job = await get_by_idempotency_key(db, episode_id, idempotency_key)
    if existing_job is not None:
        if existing_job.request_hash != request_hash:
            raise ConflictError("Idempotency-Key 已用于不同载荷")
        return GenerateStoryboardResponse(
            task_id=existing_job.task_id or "",
            job_id=existing_job.job_id,
            episode_id=episode_id,
            script_id=script_id,
            status=existing_job.status,
            accepted=True,
            message="分镜生成任务已存在",
        )

    # 并发去重: 同一分集已有进行中的分镜任务则拒绝 (08§4.2 幂等不产生重复作业)
    running_job = await get_running_for_episode(db, episode_id)
    if running_job is not None:
        raise ConflictError("该分集已有分镜生成任务进行中")

    account_result = await db.execute(
        select(CreditAccount).where(
            CreditAccount.owner_type == "user",
            CreditAccount.owner_id == user.user_id,
            CreditAccount.status == "active",
        )
    )
    account = account_result.scalar_one_or_none()
    if account is None:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise InsufficientCreditsError("缺少有效积分账户")

    try:
        attempt = await next_attempt_for_episode(db, episode_id)
        job = await create_job(
            db,
            script_id=script_id,
            episode_id=episode_id,
            episode_version=episode.episode_version,
            input_version=episode.episode_version,
            input_client_save_seq=episode.client_save_seq,
            attempt=attempt,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            triggered_by_user_id=user.user_id,
            owner_user_id=user.user_id,
        )
    except IntegrityError:
        existing_job = await get_by_idempotency_key(db, episode_id, idempotency_key)
        if existing_job is not None and existing_job.request_hash == request_hash:
            return GenerateStoryboardResponse(
                task_id=existing_job.task_id or "",
                job_id=existing_job.job_id,
                episode_id=episode_id,
                script_id=script_id,
                status=existing_job.status,
                accepted=True,
                message="分镜生成任务已存在",
            )
        raise ConflictError("Idempotency-Key 已用于不同载荷") from None

    try:
        reservation_id = await reserve(
            db,
            account.id,
            "storyboard",
            job.job_id,
            settings.price_storyboard_per_episode,
            actor_user_id=user.user_id,
            commit=False,
        )
        job.reservation_id = reservation_id
        job.detail = {"reservation_id": reservation_id}
        dispatch = await enqueue_task(
            db,
            task_type=STORYBOARD_OUTBOX_TASK_TYPE,
            job_id=job.job_id,
            attempt=job.attempt,
            payload=storyboard_outbox_payload(
                script_id=script_id,
                episode_id=episode_id,
                episode_version=episode.episode_version,
                client_save_seq=episode.client_save_seq,
                user_id=user.user_id,
                job_id=job.job_id,
            ),
        )
        job.task_id = dispatch.id
        await db.commit()
    except Exception:
        if hasattr(db, "rollback"):
            await db.rollback()
        raise

    return GenerateStoryboardResponse(
        task_id=dispatch.id,
        job_id=job.job_id,
        episode_id=episode_id,
        script_id=script_id,
        status=job.status,
        accepted=True,
        message="分镜生成任务已投递",
    )


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/cancel",
    response_model=StoryboardStatusOut,
    summary="取消分镜生成任务",
)
async def cancel_storyboard(
    script_id: str,
    episode_id: str,
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StoryboardStatusOut:
    job = await _get_authorized_storyboard_job(
        db, user, script_id, episode_id, job_id, action=Action.RUN_AI
    )
    locked_job = await db.get(
        StoryboardJob,
        job_id,
        with_for_update=True,
        populate_existing=True,
    )
    if locked_job is None:
        raise ConflictError("分镜任务状态已变化")
    job = locked_job
    if job.status not in {"succeeded", "failed", "canceled"}:
        if job.reservation_id:
            await release(
                db,
                job.reservation_id,
                "用户取消分镜任务",
                actor_user_id=user.user_id,
                commit=False,
            )
        outbox_result = await db.execute(
            select(TaskDispatchOutbox).where(
                TaskDispatchOutbox.task_type == STORYBOARD_OUTBOX_TASK_TYPE,
                TaskDispatchOutbox.job_id == job_id,
                TaskDispatchOutbox.attempt == job.attempt,
                TaskDispatchOutbox.status.in_(["pending", "retry", "running"]),
            )
        )
        for row in outbox_result.scalars().all():
            row.status = "canceled"
            row.lease_owner = None
            row.lease_token = None
            row.lease_until = None
        if not await cancel_job(db, job_id, reason="user requested"):
            await db.rollback()
            raise ConflictError("分镜任务状态已变化")
    await db.commit()
    return _job_status_out(job)


@router.get(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard-jobs",
    response_model=StoryboardJobListOut,
    summary="分镜任务列表",
)
async def list_storyboard_jobs(
    script_id: str,
    episode_id: str,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StoryboardJobListOut:
    await _validate_script_episode(db, user, script_id, episode_id, action=Action.READ)
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    items = await list_jobs_for_episode(db, episode_id, limit=safe_limit, offset=safe_offset)
    total = await count_jobs_for_episode(db, episode_id)
    return StoryboardJobListOut(items=[_job_status_out(job) for job in items], total=total)


@router.get(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}",
    response_model=StoryboardStatusOut,
    summary="分镜任务详情",
)
async def storyboard_job_detail(
    script_id: str,
    episode_id: str,
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StoryboardStatusOut:
    job = await _get_authorized_storyboard_job(
        db, user, script_id, episode_id, job_id, action=Action.READ
    )
    return _job_status_out(job)


@router.patch(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}",
    response_model=StoryboardStatusOut,
    summary="编辑分镜任务展示信息",
)
async def edit_storyboard_job(
    script_id: str,
    episode_id: str,
    job_id: str,
    payload: StoryboardJobEditIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StoryboardStatusOut:
    job = await _get_authorized_storyboard_job(
        db, user, script_id, episode_id, job_id, action=Action.RUN_AI
    )
    if job.status in {"running", "pending"}:
        raise ConflictError("分镜任务运行中，暂不可编辑")
    await update_job(
        db,
        job_id,
        progress_message=payload.progress_message,
        detail=payload.detail,
    )
    await db.commit()
    refreshed = await get_job(db, job_id)
    return _job_status_out(refreshed or job)


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard/{job_id}/retry",
    response_model=GenerateStoryboardResponse,
    summary="重试分镜任务",
)
async def retry_storyboard_job(
    script_id: str,
    episode_id: str,
    job_id: str,
    idempotency_key: str = Header(
        ...,
        alias="Idempotency-Key",
        max_length=STORYBOARD_IDEMPOTENCY_KEY_MAX_LENGTH,
    ),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> GenerateStoryboardResponse:
    old_job = await _get_authorized_storyboard_job(
        db, user, script_id, episode_id, job_id, action=Action.RUN_AI
    )
    if old_job.status not in {"failed", "canceled"}:
        raise ConflictError("只有失败或取消的分镜任务可重试")
    return await generate_storyboard(
        script_id,
        episode_id,
        old_job.input_version,
        idempotency_key,
        db=db,
        user=user,
    )


# ---------------------------------------------------------------------------
# GET /scripts/<sid>/episodes/<eid>/storyboard-status
# ---------------------------------------------------------------------------


@router.get(
    "/scripts/{script_id}/episodes/{episode_id}/storyboard-status",
    response_model=StoryboardStatusOut,
    summary="分镜生成状态",
    description="返回该分集最新一条分镜生成任务的状态/进度, 供前端轮询 (AC-VG01)。",
)
async def storyboard_status(
    script_id: str,
    episode_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StoryboardStatusOut:
    """获取分镜生成任务状态 (前端轮询)。"""
    await _validate_script_episode(db, user, script_id, episode_id)
    job = await get_latest_for_episode(db, episode_id)
    if job is None:
        raise HTTPException(
            status_code=404, detail="该分集尚无分镜生成任务记录"
        )
    return StoryboardStatusOut(
        job_id=job.job_id,
        task_id=job.task_id,
        episode_id=job.episode_id,
        script_id=job.script_id,
        status=job.status,
        progress=job.progress,
        progress_stage=job.progress_stage,
        progress_message=job.progress_message,
        error_message=job.error_message,
        detail=job.detail,
    )


# ---------------------------------------------------------------------------
# GET /scripts/<sid>/episodes/<eid>/shot-hierarchy
# ---------------------------------------------------------------------------


@router.get(
    "/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy",
    response_model=ShotHierarchyOut,
    summary="分镜层级",
    description="返回 Episode -> Scenes -> Shot Groups -> Shots 层级结构。",
)
async def shot_hierarchy(
    script_id: str,
    episode_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ShotHierarchyOut:
    """获取分镜层级结构。"""
    await _validate_script_episode(db, user, script_id, episode_id)
    return await get_shot_hierarchy(db, script_id, episode_id)


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge",
    response_model=VideoWorkbenchOut,
    summary="自动组合全部分镜",
    description=(
        "接收当前分集的全部分镜 ID，按场景与服务端叙事顺序自动组合为多个 "
        "3～15 秒的视频生成单元。"
    ),
)
async def merge_shot_group(
    script_id: str,
    episode_id: str,
    payload: ShotGroupMergeIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoWorkbenchOut:
    """原子生成整集正式镜头组并返回可直接替换页面状态的完整工作台。"""

    await _validate_script_episode(
        db,
        user,
        script_id,
        episode_id,
        action=Action.EDIT,
        expected_episode_version=payload.expected_episode_version,
    )
    result = await merge_shots_into_group(
        db,
        script_id=script_id,
        episode_id=episode_id,
        shot_ids=payload.shot_ids,
        expected_episode_version=payload.expected_episode_version,
    )
    await audit_log(
        db,
        actor=user,
        action="shot_group_merge",
        resource_type="episode",
        resource_id=episode_id,
        before={"shot_count": len(payload.shot_ids)},
        after={
            "episode_version": result.episode_version,
            "shot_group_ids": list(result.shot_group_ids),
            "removed_shot_group_ids": list(result.removed_shot_group_ids),
            "already_grouped": result.already_grouped,
        },
    )
    workbench = await get_video_workbench(db, script_id, episode_id)
    await db.commit()
    return workbench


@router.patch(
    "/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}",
    response_model=VideoWorkbenchOut,
    summary="编辑正式镜头组",
    description=(
        "部分更新合并后的正式镜头组时长、文生视频提示词、视觉素材或音频元素；"
        "成功后递增分集版本并返回完整视频工作台。"
    ),
)
async def edit_shot_group(
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    payload: ShotGroupUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoWorkbenchOut:
    """编辑正式镜头组并返回前端可直接替换的完整工作台。"""

    await _validate_script_episode(
        db,
        user,
        script_id,
        episode_id,
        action=Action.EDIT,
        expected_episode_version=payload.expected_episode_version,
    )
    fields_set = set(payload.model_fields_set)
    result = await update_shot_group(
        db,
        script_id=script_id,
        episode_id=episode_id,
        shot_group_id=shot_group_id,
        expected_episode_version=payload.expected_episode_version,
        fields_set=fields_set,
        duration_seconds=payload.duration_seconds,
        prompt=payload.prompt,
        materials=(
            payload.materials.model_dump(exclude_none=True)
            if payload.materials is not None
            else None
        ),
        audio_elements=(
            [item.model_dump(exclude_none=True) for item in payload.audio_elements]
            if payload.audio_elements is not None
            else None
        ),
    )
    await audit_log(
        db,
        actor=user,
        action="shot_group_update",
        resource_type="shot_group",
        resource_id=shot_group_id,
        before={"episode_version": payload.expected_episode_version},
        after={
            "episode_version": result.episode_version,
            "changed_fields": list(result.changed_fields),
        },
    )
    workbench = await get_video_workbench(db, script_id, episode_id)
    await db.commit()
    return workbench


@router.delete(
    "/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}",
    response_model=VideoWorkbenchOut,
    summary="删除正式镜头组",
    description=(
        "删除已合并的镜头组、其子分镜、关联视频任务及时间线片段。"
        "正在生成的视频会先尝试走既有取消闭环；远端状态未知或存在清理债务时拒绝删除。"
    ),
)
async def delete_shot_group(
    script_id: str,
    episode_id: str,
    shot_group_id: str,
    expected_episode_version: int = Query(..., ge=1),
    confirm: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoWorkbenchOut:
    """经明确确认后删除镜头组，并返回可直接替换的最新视频工作台。"""

    await _validate_script_episode(
        db,
        user,
        script_id,
        episode_id,
        action=Action.EDIT,
        expected_episode_version=expected_episode_version,
    )
    if not confirm:
        raise ConflictError(
            "删除镜头组会同时清理组内分镜、视频任务与时间线片段；"
            "请使用 confirm=true 明确确认"
        )

    try:
        canceled_job_ids = await cancel_active_video_jobs_for_shot_group(
            db,
            script_id=script_id,
            episode_id=episode_id,
            shot_group_id=shot_group_id,
            actor_user_id=user.user_id,
        )
        result = await delete_shot_group_safely(
            db,
            script_id=script_id,
            episode_id=episode_id,
            shot_group_id=shot_group_id,
            expected_episode_version=expected_episode_version,
        )
        await audit_log(
            db,
            actor=user,
            action="shot_group_delete",
            resource_type="shot_group",
            resource_id=shot_group_id,
            before={
                "episode_version": expected_episode_version,
                "canceled_video_job_ids": list(canceled_job_ids),
            },
            after={
                "episode_version": result.episode_version,
                "removed_shot_count": result.removed_shot_count,
                "removed_video_job_ids": list(result.removed_video_job_ids),
                "removed_timeline_arrangement_count": result.removed_timeline_arrangement_count,
                "scheduled_storage_delete_count": result.scheduled_storage_delete_count,
            },
        )
        workbench = await get_video_workbench(db, script_id, episode_id)
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return workbench


# ---------------------------------------------------------------------------
# GET /shots/<sid>/episodes/<eid>/video-workbench
# ---------------------------------------------------------------------------


@router.get(
    "/shots/{script_id}/episodes/{episode_id}/video-workbench",
    response_model=VideoWorkbenchOut,
    summary="视频工作台",
    description="返回含 unit_id/unit_runtime_key/materials/audio_elements 的工作台数据。",
)
async def video_workbench(
    script_id: str,
    episode_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoWorkbenchOut:
    """获取视频工作台数据。"""
    await _validate_script_episode(db, user, script_id, episode_id)
    return await get_video_workbench(db, script_id, episode_id)


# ---------------------------------------------------------------------------
# GET /shots/<script_id>/cover
# ---------------------------------------------------------------------------


@router.get(
    "/shots/{script_id}/cover",
    response_model=CoverOut,
    summary="视频项目封面",
    description="返回首个镜头首帧的 storage_key 作为视频项目封面。",
)
async def shot_cover(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> CoverOut:
    """获取视频项目封面 (首个镜头首帧)。"""
    script_result = await db.execute(select(Script).where(Script.id == script_id))
    script = script_result.scalar_one_or_none()
    if script is None:
        raise HTTPException(status_code=404, detail="剧本不存在")
    await check_script_access(db, user, script, action=Action.READ)
    return await get_cover(db, script_id)


__all__ = ["router"]
