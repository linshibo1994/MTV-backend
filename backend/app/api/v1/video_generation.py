"""视频生成模块 API 路由 — /video-generation* /video-generation-jobs。

端点:
- POST /video-generation-jobs                创建视频生成任务（绑定 shot_group_id）
- GET  /video-generation-jobs                列表轮询（script_id, episode_id, limit）
- GET  /video-generation/<job_id>            单任务状态
- GET  /video-generation/active-provider     当前激活供应商+模型
- POST /video-generation/active-provider     切换供应商（管理员）

router 已在 app/api/router.py 挂载到 /v3api 前缀下。
"""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.errors import BadRequestError, ConflictError, ForbiddenError
from app.crud import video as video_crud
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.user import User
from app.models.video_generation_job import VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH
from app.schemas.video import (
    ActiveProviderOut,
    ActiveProviderUpdateIn,
    ReferenceImageAuditIn,
    ReferenceImageAuditOut,
    VideoJobBatchCreateIn,
    VideoJobBatchOut,
    VideoJobCreateIn,
    VideoJobFailedItem,
    VideoJobListOut,
    VideoJobOut,
)
from app.services.audit import best_effort_log as audit_log
from app.services.reference_aggregator import AggregatedReferences
from app.services.runtime_models import resolve_env_video_model, resolve_video_model

router = APIRouter()
logger = logging.getLogger(__name__)


def _derive_batch_idempotency_keys(prefix: str, count: int) -> list[str]:
    keys = [f"{prefix}:{idx}" for idx in range(count)]
    if any(len(key) > VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH for key in keys):
        raise BadRequestError(
            f"批量 Idempotency-Key 派生后超过 {VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH} 字符"
        )
    return keys


def _effective_env_video_provider() -> ActiveProviderOut:
    """返回环境配置实际用于创建视频任务的规范化 Provider 与模型。"""
    runtime_model = resolve_env_video_model(settings)
    return ActiveProviderOut(
        provider_key=runtime_model.provider,
        job_type="all_reference",
        model=runtime_model.model,
    )


async def _load_and_check_script(
    db: AsyncSession, user: User, script_id: str, *, require_write: bool = False
) -> Script:
    """加载剧本并执行资源级授权（doc 08 §4.1）。

    未登录/无权访问由 check_script_access 抛 401/403；不存在抛 404。
    """
    from app.core.permissions import Action, check_script_access

    script_result = await db.execute(
        select(Script)
        .where(Script.id == script_id)
        .options(
            noload(Script.episodes),
            noload(Script.subjects),
            noload(Script.parent_subjects),
            noload(Script.materials),
        )
    )
    script = script_result.scalar_one_or_none()
    if script is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="剧本不存在")
    await check_script_access(
        db, user, script, action=Action.EDIT if require_write else Action.READ
    )
    return script


@router.post(
    "/video-generation-jobs",
    response_model=VideoJobOut,
    status_code=status.HTTP_201_CREATED,
    summary="创建视频生成任务",
)
async def create_video_generation_job(
    body: VideoJobCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    idempotency_key: str | None = Header(
        None,
        alias="Idempotency-Key",
        max_length=VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH,
        description="幂等键；同一键重复创建返回已存在任务而非新建（doc 08 §4.2）",
    ),
) -> VideoJobOut:
    """创建视频生成任务 — 绑定 shot_group_id，自动汇聚参考图 → 积分预扣 → 异步 worker。

    幂等:
    - 优先按 Idempotency-Key 头去重；
    - 缺省时按 shot_group_id 查活动任务（pending/running）复用，避免重复冻结。
    """
    # 校验剧本访问权限（写）
    await _load_and_check_script(db, user, body.script_id, require_write=True)

    from app.services.video_generation import create_video_job

    job = await create_video_job(
        db,
        script_id=body.script_id,
        episode_id=body.episode_id,
        shot_group_id=body.shot_group_id,
        user_id=user.user_id,
        idempotency_key=idempotency_key,
        prompt=body.prompt,
        style_id=body.style_id,
    )
    return VideoJobOut.model_validate(job)


async def _current_reference_image_audit_state(
    db: AsyncSession, group: ShotGroup
) -> tuple[AggregatedReferences, dict]:
    """与视频创建共用最终参考图解析：主体图 + 当前链式首帧。"""
    from app.services.reference_aggregator import aggregate_references
    from app.services.reference_image_audit import current_audit_state, include_first_frame
    from app.services.video_generation import _resolve_first_frame

    references = await aggregate_references(db, group.shot_group_id)
    include_first_frame(references, await _resolve_first_frame(db, group))
    return references, current_audit_state(group.reference_image_audit, references)


@router.get(
    "/video-generation/reference-image-audit",
    response_model=ReferenceImageAuditOut,
    summary="获取视频参考图审核状态",
)
async def get_video_reference_image_audit(
    script_id: str = Query(..., description="剧本 ID"),
    episode_id: str = Query(..., description="分集 ID"),
    shot_group_id: str = Query(..., description="镜头组 ID"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ReferenceImageAuditOut:
    await _load_and_check_script(db, user, script_id, require_write=False)
    group = (
        await db.execute(
            select(ShotGroup).where(
                ShotGroup.shot_group_id == shot_group_id,
                ShotGroup.script_id == script_id,
                ShotGroup.episode_id == episode_id,
            )
        )
    ).scalar_one_or_none()
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在或不属于当前分集")
    from app.services.reference_image_audit import audit_not_required_state, public_audit_state

    references, audit_state = await _current_reference_image_audit_state(db, group)
    if not (await resolve_video_model(db, settings)).reference_image_audit_required:
        audit_state = audit_not_required_state(references)
    return ReferenceImageAuditOut(shot_group_id=group.shot_group_id, **public_audit_state(audit_state))


@router.post(
    "/video-generation/reference-image-audit",
    response_model=ReferenceImageAuditOut,
    summary="审核视频生成参考图",
)
async def audit_video_reference_images(
    body: ReferenceImageAuditIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ReferenceImageAuditOut:
    """手动审核最终会提交给视频模型的全部参考图。

    审核快照保存到镜头组，并由当前参考图清单的指纹绑定；素材或首帧变化后，工作台
    自动显示 pending，创建视频任务也会再次强制校验，不能复用陈旧的通过结果。
    """
    await _load_and_check_script(db, user, body.script_id, require_write=True)
    result = await db.execute(
        select(ShotGroup)
        .where(
            ShotGroup.shot_group_id == body.shot_group_id,
            ShotGroup.script_id == body.script_id,
            ShotGroup.episode_id == body.episode_id,
        )
        .with_for_update()
    )
    group = result.scalar_one_or_none()
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在或不属于当前分集")

    from app.services.reference_image_audit import (
        audit_not_required_state,
        audit_references,
        public_audit_state,
        reference_image_audit_failure_message,
    )

    references, current_state = await _current_reference_image_audit_state(db, group)
    if not (await resolve_video_model(db, settings)).reference_image_audit_required:
        return ReferenceImageAuditOut(
            shot_group_id=group.shot_group_id,
            **public_audit_state(audit_not_required_state(references)),
        )
    if group.unit_id != f"group:{group.shot_group_id}":
        raise ConflictError("请先完成镜头组合并，再审核参考图")
    try:
        audit_state = await audit_references(references)
    except RuntimeError as exc:
        # 失败是可观察且可重试的状态，绝不降级为通过；不暴露 SDK/上游响应细节。
        failure_message = reference_image_audit_failure_message(exc)
        error_code = (
            str(exc)
            if str(exc).startswith("reference_image_audit_")
            else "reference_image_audit_unexpected"
        )
        logger.warning(
            "reference_image_audit_failed code=%s shot_group_id=%s",
            error_code,
            group.shot_group_id,
        )
        current_state.update({
            "status": "failed",
            "audited_at": datetime.now(UTC).isoformat(),
            "message": failure_message,
        })
        group.reference_image_audit = current_state
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=failure_message,
        ) from exc
    group.reference_image_audit = audit_state
    await db.commit()
    return ReferenceImageAuditOut(shot_group_id=group.shot_group_id, **public_audit_state(audit_state))


@router.get(
    "/video-generation-jobs",
    response_model=VideoJobListOut,
    summary="视频生成任务列表",
)
async def list_video_generation_jobs(
    script_id: str = Query(..., description="剧本 ID"),
    episode_id: str | None = Query(None, description="分集 ID"),
    limit: int = Query(30, ge=1, le=500, description="返回条数上限"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoJobListOut:
    """按剧本/分集过滤视频生成任务，按创建时间倒序（前端轮询）。

    资源级授权：按 script_id 加载剧本并 check_script_access，未通过则 403。
    total 为匹配全集总数（非当前页条数），供前端分页计数。
    """
    await _load_and_check_script(db, user, script_id, require_write=False)

    jobs = await video_crud.list_video_jobs(
        db, script_id=script_id, episode_id=episode_id, limit=limit
    )
    total = await video_crud.count_video_jobs(
        db, script_id=script_id, episode_id=episode_id
    )
    items = [VideoJobOut.model_validate(j) for j in jobs]
    return VideoJobListOut(items=items, total=total)


@router.get(
    "/video-generation/{job_id}",
    response_model=VideoJobOut,
    summary="视频生成任务详情",
)
async def get_video_generation_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoJobOut:
    """取单个视频生成任务状态（轮询 status/progress）。

    资源级授权：通过 job.script_id 加载剧本并 check_script_access，
    防止任意已登录用户猜 ID 读取他人剧本任务（doc 08 §4.1）。
    """
    job = await video_crud.get_video_job(db, job_id)
    if job is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="视频生成任务不存在")
    await _load_and_check_script(db, user, job.script_id, require_write=False)
    return VideoJobOut.model_validate(job)


@router.post(
    "/video-generation/{job_id}/cancel",
    response_model=VideoJobOut,
    summary="取消视频生成任务",
)
async def cancel_video_generation_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> VideoJobOut:
    """取消视频生成任务 — 仅 starting/processing 可取消，置 canceled 并释放冻结积分。

    doc04 §1.19 / doc08 §2.5：canceled 为终态。资源级授权：通过 job.script_id
    加载剧本并 check_script_access(require_write=True)。
    """
    job = await video_crud.get_video_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="视频生成任务不存在")
    await _load_and_check_script(db, user, job.script_id, require_write=True)

    from app.services.video_generation import cancel_video_job

    cancelled = await cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job_id,
        actor_user_id=user.user_id,
    )
    return VideoJobOut.model_validate(cancelled)


@router.get(
    "/video-generation/check-materials",
    summary="检查镜头组素材就绪情况",
)
async def check_materials(
    shot_group_id: str = Query(..., description="镜头组 ID"),
    script_id: str = Query(..., description="剧本 ID"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict:
    """检查镜头组视频生成所需素材是否就绪（doc02 §5.9 工作台批量勾选前置）。

    返回 {missing_subjects:[...], missing_audio:[...], ready:bool}。
    缺失主体/音频供前端标红提示；缺失参考音频可降级为模型原生声音，
    因此不影响 ready，缺失视觉主体仍判定素材未就绪。
    资源级授权：按 script_id 校验剧本读权限。
    """
    group_result = await db.execute(
        select(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
    )
    shot_group = group_result.scalar_one_or_none()
    if shot_group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在")
    if shot_group.script_id != script_id:
        raise ForbiddenError("镜头组不属于当前剧本")
    if shot_group.unit_id != f"group:{shot_group.shot_group_id}":
        raise ConflictError("请先完成镜头组合并，再检查视频素材")

    await _load_and_check_script(db, user, script_id, require_write=False)

    from app.services.reference_aggregator import aggregate_references

    aggregated = await aggregate_references(db, shot_group_id)
    return {
        "missing_subjects": aggregated.missing_subjects,
        "missing_audio": aggregated.missing_audio,
        "ready": not aggregated.missing_subjects,
    }


@router.post(
    "/video-generation-jobs/batch",
    response_model=VideoJobBatchOut,
    status_code=status.HTTP_201_CREATED,
    summary="批量创建视频生成任务",
)
async def batch_create_video_generation_jobs(
    body: VideoJobBatchCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    idempotency_key: str | None = Header(
        None,
        alias="Idempotency-Key",
        max_length=VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH,
        description="幂等键前缀，批量任务按 <prefix>:<idx> 派生",
    ),
) -> VideoJobBatchOut:
    """批量创建视频生成任务 — 接受多个 shot_group_id，循环创建（doc02 §5.9）。

    每个 shot_group 单独 try：单个失败记录 error 不中断整体，返回含 success/failed
    两个列表。成功项为完整 VideoJobOut（可继续轮询/取消）；失败项仅含 shot_group_id
    与 error_message，避免空 job_id 伪记录混入 success 列表导致前端无法分组、且空
    job_id 无法被后续轮询/取消端点使用。
    资源级授权：按 body.script_id 校验剧本写权限（统一校验一次）。
    """
    await _load_and_check_script(db, user, body.script_id, require_write=True)
    if not idempotency_key or not idempotency_key.strip():
        raise BadRequestError("Idempotency-Key 必填")
    unknown_prompt_ids = set(body.prompts or {}) - set(body.shot_group_ids)
    if unknown_prompt_ids:
        raise BadRequestError("prompts 包含非目标镜头组")
    derived_keys = _derive_batch_idempotency_keys(
        idempotency_key, len(body.shot_group_ids)
    )

    from app.services.video_generation import create_video_job

    success: list[VideoJobOut] = []
    failed: list[VideoJobFailedItem] = []
    for sg_id, key in zip(body.shot_group_ids, derived_keys, strict=True):
        try:
            job = await create_video_job(
                db,
                script_id=body.script_id,
                episode_id=body.episode_id,
                shot_group_id=sg_id,
                user_id=user.user_id,
                idempotency_key=key,
                prompt=(body.prompts or {}).get(sg_id),
                style_id=body.style_id,
            )
            success.append(VideoJobOut.model_validate(job))
        except HTTPException as exc:
            await db.rollback()
            # 单个失败：记入 failed 列表但不中断批量
            failed.append(VideoJobFailedItem(
                shot_group_id=sg_id,
                error_message=str(exc.detail)[:1024],
            ))
    return VideoJobBatchOut(success=success, failed=failed)


@router.get(
    "/video-generation/active-provider",
    response_model=ActiveProviderOut,
    summary="当前激活视频供应商",
)
async def get_active_provider(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ActiveProviderOut:
    """取当前激活的视频供应商 + 模型 {provider_key, job_type, model}。

    env 模式返回任务创建实际采用的环境配置；database 模式返回数据库激活配置。
    """
    if settings.video_provider_selection_mode == "env":
        return _effective_env_video_provider()

    cfg = await video_crud.get_active_provider(db, "all_reference", commit_on_seed=True)
    if cfg is None:
        return ActiveProviderOut(
            provider_key=settings.video_default_provider,
            job_type="all_reference",
            model=settings.seedance_model,
        )
    return ActiveProviderOut.model_validate(cfg)


@router.post(
    "/video-generation/active-provider",
    response_model=ActiveProviderOut,
    summary="切换视频供应商（管理员）",
)
async def set_active_provider(
    body: ActiveProviderUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ActiveProviderOut:
    """切换激活视频供应商 — 仅管理员可操作。

    支持豆包别名 jianyu/modelsai、mgrouter AICC、阿里百炼 bailian、OpenAI Sora 与 Gemini Veo。
    """
    if not user.is_admin:
        raise ForbiddenError("仅管理员可切换视频供应商")
    if settings.video_provider_selection_mode == "env":
        raise ConflictError("视频供应商由环境配置锁定，无法通过 API 切换")

    # 存量数据库可能已有旧供应商配置但缺少后续新增项，切换前幂等补齐。
    await video_crud.ensure_provider_configs(db)
    existing = await video_crud.get_provider_by_key(db, body.provider_key, body.job_type)
    if existing is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"供应商配置不存在: {body.provider_key}",
        )

    current = await video_crud.get_active_provider(db, body.job_type)
    cfg = await video_crud.set_active_provider(
        db, provider_key=body.provider_key, job_type=body.job_type
    )
    await db.commit()
    response = ActiveProviderOut.model_validate(cfg)
    await audit_log(
        db,
        actor=user,
        action="provider_switch",
        resource_type="video_provider",
        resource_id=body.provider_key,
        before=(
            {
                "provider_key": current.provider_key,
                "job_type": current.job_type,
                "model": current.model,
            }
            if current is not None
            else None
        ),
        after=response.model_dump(),
        commit=True,
    )
    return response


_STATIC_VIDEO_PATHS = {
    "/video-generation/active-provider": 0,
    "/video-generation/check-materials": 1,
}
router.routes.sort(
    key=lambda route: (
        0
        if getattr(route, "path", "") in _STATIC_VIDEO_PATHS
        else 1,
        _STATIC_VIDEO_PATHS.get(getattr(route, "path", ""), 99),
    )
)
