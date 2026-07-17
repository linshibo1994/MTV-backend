"""T3 剧本模块 API 路由 — /scripts/*。

端点清单（PLAN 4.3 / TASK T3.2）:
  GET    /scripts/page                      列表（scope personal|team）
  POST   /scripts                            新建剧本
  GET    /scripts/{script_id}                详情
  GET    /scripts/{script_id}/episodes       分集列表
  POST   /scripts/{script_id}/episodes       新增分集
  PUT    /scripts/{script_id}/episodes/{eid}  保存（乐观锁 + 触发流水线）
  GET    /scripts/{script_id}/episodes/{eid}/breakdown
  GET    /scripts/{script_id}/pipeline-status
  GET    /scripts/{script_id}/extract-subjects-status
  GET    /scripts/news-broadcast/jobs        返回 []（本期不实现）

约定:
  - 错误统一 {"detail":...}，用 app/core/errors.py 异常
  - 鉴权依赖 get_current_user
  - 每个 api 文件定义 router = APIRouter()，由 app/api/router.py 统一 include 到主路由
  - 资源级授权: 改 URL id 时服务端重新校验归属（permissions.check_script_access）
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ConflictError, ForbiddenError
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.episode import Episode
from app.models.user import User
from app.schemas.script import (
    BreakdownOut,
    BreakdownShot,
    BreakdownShotGroup,
    BreakdownSubject,
    EpisodeCreateIn,
    EpisodeListItem,
    EpisodeOut,
    EpisodeSaveIn,
    ExtractEpisodeStatus,
    ExtractSubjectsStatusOut,
    LifecycleDeleteOut,
    LifecycleDependencySummary,
    PipelineStageOut,
    PipelineStatusOut,
    ScriptCopyIn,
    ScriptCreateIn,
    ScriptListItem,
    ScriptOut,
    ScriptPageOut,
    ScriptUpdateIn,
)
from app.services.audit import best_effort_log as audit_log
from app.services.pipeline import get_stage
from app.services.pipeline.orchestrator import get_pipeline_status, trigger_pipeline

router = APIRouter(prefix="/scripts", tags=["scripts"])


# --------------------------------------------------------------------------- #
# 列表与详情
# --------------------------------------------------------------------------- #
@router.get("/page", response_model=ScriptPageOut)
async def list_scripts_page(
    scope: str = Query("personal", pattern="^(personal|team)$"),
    limit: int = Query(30, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ScriptPageOut:
    """剧本分页列表，团队项目带 current_user_project_role。"""
    items, total = await script_crud.list_scripts(db, scope, limit, offset, user)

    # 批量取所有者用户名
    owner_ids = list({it.owner_user_id for it in items})
    owner_map: dict[str, str] = {}
    if owner_ids:
        result = await db.execute(select(User).where(User.user_id.in_(owner_ids)))
        for u in result.scalars().all():
            owner_map[u.user_id] = u.username

    list_items: list[ScriptListItem] = []
    for it in items:
        role = None
        if it.scope == "team":
            role = await script_crud.get_project_role(db, it.id, user.user_id)
            # 非项目成员不可见团队剧本（08§1.2: 需显式成员关系）
            if role is None:
                continue
        list_items.append(
            ScriptListItem(
                id=it.id,
                title=it.title,
                scope=it.scope,  # type: ignore[arg-type]
                owner_user_id=it.owner_user_id,
                owner_username=owner_map.get(it.owner_user_id),
                org_id=it.org_id,
                created_by_user_id=it.created_by_user_id,
                current_user_project_role=role,
                updated_at=it.updated_at,
            )
        )
    return ScriptPageOut(items=list_items, total=total)


@router.post("", response_model=ScriptOut, status_code=201)
async def create_script(
    body: ScriptCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ScriptOut:
    """新建剧本。"""
    script = await script_crud.create_script(db, body.title, body.scope, user)
    return ScriptOut.model_validate(script)


@router.get("/news-broadcast/jobs", response_model=list)
async def list_news_broadcast_jobs(
    user: User = Depends(get_current_user),
) -> list:
    """新闻播报特殊剧本类型，本期不实现，返回空列表。"""
    return []


@router.get("/{script_id}", response_model=ScriptOut)
async def get_script(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ScriptOut:
    """剧本详情。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    return ScriptOut.model_validate(script)


@router.patch("/{script_id}", response_model=ScriptOut)
async def rename_script(
    script_id: str,
    body: ScriptUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ScriptOut:
    """重命名剧本。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)
    before = {"title": script.title}
    try:
        await script_crud.rename_script(db, script, body.title, commit=False)
        await audit_log(
            db,
            actor=user,
            action="script_rename",
            resource_type="script",
            resource_id=script_id,
            before=before,
            after={"title": body.title},
        )
        await db.commit()
        await db.refresh(script)
    except Exception:
        await db.rollback()
        raise
    return ScriptOut.model_validate(script)


@router.post("/{script_id}/copy", response_model=ScriptOut, status_code=201)
async def copy_script(
    script_id: str,
    body: ScriptCopyIn | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ScriptOut:
    """复制剧本与分集文本；派生任务、媒体和审核资产不会进入副本。"""
    source = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, source, action=Action.EDIT)
    payload = body or ScriptCopyIn()
    target_scope = payload.scope or source.scope
    if target_scope != source.scope and source.scope == "team":
        role = await script_crud.get_project_role(db, source.id, user.user_id)
        if role not in {"owner", "manager"}:
            raise ForbiddenError("仅项目所有者或管理员可跨范围复制剧本")
    try:
        target = await script_crud.copy_script(
            db,
            source,
            user,
            title=payload.title,
            scope=target_scope,
            commit=False,
        )
        await audit_log(
            db,
            actor=user,
            action="script_copy",
            resource_type="script",
            resource_id=target.id,
            before={"source_script_id": source.id},
            after={"title": target.title, "scope": target.scope},
        )
        await db.commit()
        await db.refresh(target)
    except Exception:
        await db.rollback()
        raise
    return ScriptOut.model_validate(target)


def _dependency_conflict(
    detail: str, summary: LifecycleDependencySummary
) -> ConflictError:
    values = summary.model_dump()
    counts = "、".join(f"{key}={value}" for key, value in values.items() if value)
    return ConflictError(f"{detail}（{counts}）；请使用 confirm=true 明确确认")


def _reject_active_jobs(summary: LifecycleDependencySummary) -> None:
    if summary.active_job_count:
        raise ConflictError(
            f"仍有 {summary.active_job_count} 个活动、待投递或重试任务，"
            "请先取消或等待任务进入终态后再删除"
        )


@router.delete("/{script_id}", response_model=LifecycleDeleteOut)
async def delete_script(
    script_id: str,
    confirm: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> LifecycleDeleteOut:
    """删除剧本；派生资源存在时必须显式确认。"""
    # FOR UPDATE 父行锁会与并发子资源 FK 插入互斥；锁内重查后再删除，
    # 避免活动任务在依赖检查与批量删除之间提交。
    script = await script_crud.get_script_for_update(db, script_id)
    await check_script_access(db, user, script, action=Action.DELETE)
    summary = await script_crud.get_script_dependency_summary(db, script_id)
    _reject_active_jobs(summary)
    if summary.has_dependencies and not confirm:
        raise _dependency_conflict("剧本存在依赖资源", summary)
    before = {
        "title": script.title,
        "scope": script.scope,
        "dependencies": summary.model_dump(),
    }
    try:
        await script_crud.delete_script(db, script, commit=False)
        await audit_log(
            db,
            actor=user,
            action="script_delete",
            resource_type="script",
            resource_id=script_id,
            before=before,
            after=None,
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return LifecycleDeleteOut(detail="剧本已删除", dependencies=summary)


# --------------------------------------------------------------------------- #
# 分集
# --------------------------------------------------------------------------- #
@router.get("/{script_id}/episodes", response_model=list[EpisodeListItem])
async def list_episodes(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> list[EpisodeListItem]:
    """分集列表。

    按文档 04 §2.3 契约返回裸数组 Episode[]（非 TASK T0.3 的 {items,total} 包裹）；
    该端点是文档明示的裸数组例外，仅此一处不遵循包裹约定。
    """
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    episodes = await script_crud.list_episodes(db, script_id)
    return [EpisodeListItem.model_validate(e) for e in episodes]


@router.post("/{script_id}/episodes", response_model=EpisodeListItem, status_code=201)
async def create_episode(
    script_id: str,
    body: EpisodeCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> EpisodeListItem:
    """新增分集。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)
    episode = await script_crud.create_episode(
        db, script_id, body.title, body.script_text, body.episode_no
    )
    return EpisodeListItem.model_validate(episode)


@router.put("/{script_id}/episodes/{episode_id}", response_model=EpisodeListItem)
async def save_episode(
    script_id: str,
    episode_id: str,
    body: EpisodeSaveIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> EpisodeListItem:
    """保存分集（乐观锁：episode_version + client_save_seq，冲突 409），并触发流水线。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)

    db_get = getattr(db, "get", None)
    before = await db_get(Episode, episode_id) if db_get is not None else None
    if before is None or before.script_id != script_id:
        before = await script_crud.get_episode(db, script_id, episode_id)
    before_snapshot = {
        "title": before.title,
        "episode_no": before.episode_no,
        "episode_version": before.episode_version,
        "client_save_seq": getattr(before, "client_save_seq", body.client_save_seq),
    }
    try:
        if body.script_text is not None:
            await check_script_access(db, user, script, action=Action.RUN_AI)
        episode = await script_crud.save_episode(
            db,
            script_id,
            episode_id,
            body.script_text,
            body.episode_version,
            body.client_save_seq,
            body.title,
            getattr(body, "episode_no", None),
            commit=False,
        )
        await audit_log(
            db,
            actor=user,
            action="episode_update",
            resource_type="episode",
            resource_id=episode_id,
            before=before_snapshot,
            after={
                "title": episode.title,
                "episode_no": episode.episode_no,
                "episode_version": episode.episode_version,
                "client_save_seq": getattr(
                    episode, "client_save_seq", body.client_save_seq + 1
                ),
            },
        )
        if body.script_text is not None:
            await trigger_pipeline(db, script_id, episode_id)
        else:
            await db.commit()
    except Exception:
        await db.rollback()
        raise

    return EpisodeListItem.model_validate(episode)


@router.delete(
    "/{script_id}/episodes/{episode_id}", response_model=LifecycleDeleteOut
)
async def delete_episode(
    script_id: str,
    episode_id: str,
    confirm: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> LifecycleDeleteOut:
    """删除分集；返回并审计删除前依赖摘要。"""
    # 锁序固定为 Script -> Episode，和任务创建的 FK 父行顺序一致。
    script = await script_crud.get_script_for_update(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)
    episode = await script_crud.get_episode_for_update(db, script_id, episode_id)
    summary = await script_crud.get_episode_dependency_summary(db, script_id, episode_id)
    _reject_active_jobs(summary)
    if summary.has_dependencies and not confirm:
        raise _dependency_conflict("分集存在依赖资源", summary)
    before = {
        "title": episode.title,
        "episode_no": episode.episode_no,
        "dependencies": summary.model_dump(),
    }
    try:
        await script_crud.delete_episode(db, script_id, episode_id, commit=False)
        await audit_log(
            db,
            actor=user,
            action="episode_delete",
            resource_type="episode",
            resource_id=episode_id,
            before=before,
            after=None,
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return LifecycleDeleteOut(detail="分集已删除", dependencies=summary)


@router.get(
    "/{script_id}/episodes/{episode_id}/breakdown", response_model=BreakdownOut
)
async def get_breakdown(
    script_id: str,
    episode_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> BreakdownOut:
    """分集 breakdown: {episode, subjects[], shots[], shot_groups[]}。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)

    data = await script_crud.get_breakdown(db, script_id, episode_id)
    subjects = [BreakdownSubject.model_validate(s) for s in data["subjects"]]
    return BreakdownOut(
        episode=EpisodeOut.model_validate(data["episode"]),
        subjects=subjects,
        shots=[BreakdownShot.model_validate(item) for item in data["shots"]],
        shot_groups=[BreakdownShotGroup.model_validate(item) for item in data["shot_groups"]],
    )


# --------------------------------------------------------------------------- #
# 流水线状态
# --------------------------------------------------------------------------- #
@router.get("/{script_id}/pipeline-status", response_model=PipelineStatusOut)
async def get_pipeline_status_endpoint(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PipelineStatusOut:
    """5 阶段流水线状态 + running/failed/all_done。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)

    status = await get_pipeline_status(db, script_id)
    stages = [PipelineStageOut(**s) for s in status["stages"]]
    return PipelineStatusOut(
        stages=stages,
        running=status["running"],
        failed=status["failed"],
        all_done=status["all_done"],
    )


@router.get(
    "/{script_id}/extract-subjects-status",
    response_model=ExtractSubjectsStatusOut,
)
async def get_extract_subjects_status(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ExtractSubjectsStatusOut:
    """主体提取与合并状态（含 merge_job_id，前端轮询）。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)

    episodes = await script_crud.list_episodes(db, script_id)
    extract_stage = await get_stage(db, script_id, "extract")
    merge_stage = await get_stage(db, script_id, "merge")

    extract_detail = (extract_stage.detail if extract_stage else None) or {}
    per_eps = {
        str(ep.get("episode_id")): ep
        for ep in (extract_detail.get("episodes", []) or [])
    }

    ep_status: list[ExtractEpisodeStatus] = []
    extracted_count = 0
    for ep in episodes:
        info = per_eps.get(str(ep.episode_id), {})
        ext_status = str(info.get("extract_status", "pending"))
        extracted = ext_status == "succeeded"
        if extracted:
            extracted_count += 1
        ep_status.append(
            ExtractEpisodeStatus(
                episode_id=ep.episode_id,
                episode_no=ep.episode_no,
                title=ep.title,
                extract_status=ext_status,
                extracted=extracted,
            )
        )

    total = len(episodes)
    # merge job id：取最新 merge 阶段任务记录
    from app.models.pipeline_job import PipelineJob

    merge_job_result = await db.execute(
        select(PipelineJob)
        .where(
            PipelineJob.script_id == script_id,
            PipelineJob.stage == "merge",
        )
        .order_by(
            PipelineJob.created_at.desc(),
            PipelineJob.run_id.desc(),
            PipelineJob.attempt.desc(),
        )
        .limit(1)
    )
    merge_job = merge_job_result.scalar_one_or_none()
    merge_job_id = merge_job.id if merge_job else None
    raw_merge_status = (merge_stage.status if merge_stage else "pending") or "pending"
    # 阶段状态原始值为 done/pending/running/failed，按文档 02 §2.3.2 / 04 §2.3
    # 完成态需映射为 succeeded，保持前端轮询契约一致
    merge_status = {
        "done": "succeeded",
        "failed": "failed",
        "running": "running",
        "pending": "pending",
    }.get(raw_merge_status, raw_merge_status)

    return ExtractSubjectsStatusOut(
        total_episodes=total,
        extracted_episodes=extracted_count,
        all_extracted=total > 0 and extracted_count == total,
        episodes=ep_status,
        merge_status=merge_status,
        merge_job_id=merge_job_id,
        has_active_merge=merge_status == "running",
    )
