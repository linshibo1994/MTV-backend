"""剧本/分集/主体数据访问层。

- list_scripts: 支持 scope personal/team，团队项目带 current_user_project_role
- save_episode: 带乐观锁（episode_version + client_save_seq），冲突 409
- get_breakdown: 返回 {episode, subjects, shots, shot_groups}
"""

import logging
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import HTTPException
from sqlalchemy import delete, false, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import ConflictError, ForbiddenError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_upload import MediaUpload
from app.models.parent_subject import ParentSubject
from app.models.pipeline_job import PipelineJob
from app.models.pipeline_stage import PipelineStage
from app.models.project_member import ProjectMember
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_timeline_arrangement import VideoTimelineArrangement
from app.schemas.script import LifecycleDependencySummary
from app.services.storage_references import (
    StorageReferenceExclusions,
    acquire_storage_reference_barrier,
    collect_storage_keys,
    decide_storage_key_deletes,
    enqueue_storage_deletes,
    normalize_storage_keys,
)
from app.services.task_dispatch import cancel_outbox_task
from app.services.video_cleanup_policy import has_retryable_video_cleanup_debt

logger = logging.getLogger(__name__)

_VIDEO_ACTIVE_STATUSES = frozenset(
    {"pending", "ready", "retry", "waiting_dependency", "starting", "processing"}
)
_STORYBOARD_ACTIVE_STATUSES = frozenset({"pending", "ready", "retry", "running"})
_PIPELINE_ACTIVE_STATUSES = frozenset({"pending", "ready", "retry", "running"})

# --------------------------------------------------------------------------- #
# 内部辅助
# --------------------------------------------------------------------------- #


class NotFoundError(HTTPException):
    """资源不存在 — 404 {"detail":...}。"""

    def __init__(self, detail: str = "资源不存在") -> None:
        super().__init__(status_code=404, detail=detail)


async def _load_script_or_404(db: AsyncSession, script_id: str) -> Script:
    result = await db.execute(select(Script).where(Script.id == script_id))
    script = result.scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    return script


async def get_script_for_update(db: AsyncSession, script_id: str) -> Script:
    """锁定剧本父行，使依赖检查与删除不会被并发子资源插入穿透。"""
    result = await db.execute(
        select(Script).where(Script.id == script_id).with_for_update()
    )
    script = result.scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    return script


async def get_project_role(db: AsyncSession, script_id: str, user_id: str) -> str | None:
    result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.script_id == script_id,
            ProjectMember.user_id == user_id,
        )
    )
    member = result.scalar_one_or_none()
    return member.project_role if member else None


# --------------------------------------------------------------------------- #
# Script
# --------------------------------------------------------------------------- #
async def list_scripts(
    db: AsyncSession,
    scope: str,
    limit: int,
    offset: int,
    user: User,
) -> tuple[list[Script], int]:
    """分页列出剧本。

    - personal: 仅当前用户拥有的剧本
    - team: 当前用户可见的团队项目（成员或管理员可见）
    返回 (items, total)。
    """
    base = select(Script).where(Script.scope == scope)
    count_base = select(func.count(Script.id)).where(Script.scope == scope)
    if scope == "personal":
        base = base.where(Script.owner_user_id == user.user_id)
        count_base = count_base.where(Script.owner_user_id == user.user_id)
    elif scope == "team":
        # 团队项目：仅统计当前用户为成员的剧本，保证 total 与 items 一致
        active_org_id = getattr(user, "active_org_id", None)
        member_script_ids = select(ProjectMember.script_id).where(
            ProjectMember.user_id == user.user_id
        )
        base = base.where(Script.id.in_(member_script_ids))
        count_base = count_base.where(Script.id.in_(member_script_ids))
        if active_org_id is None:
            base = base.where(false())
            count_base = count_base.where(false())
        else:
            base = base.where(Script.org_id == active_org_id)
            count_base = count_base.where(Script.org_id == active_org_id)

    total_result = await db.execute(count_base)
    total = int(total_result.scalar() or 0)

    stmt = (
        base.order_by(Script.updated_at.desc())
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(stmt)
    items = list(result.scalars().all())
    return items, total


async def get_script(db: AsyncSession, script_id: str) -> Script:
    return await _load_script_or_404(db, script_id)


async def create_script(
    db: AsyncSession,
    title: str,
    scope: str,
    user: User,
    *,
    visual_style: str | None = None,
    unify_style: bool = True,
    commit: bool = True,
) -> Script:
    active_org_id = getattr(user, "active_org_id", None)
    if scope == "team" and not active_org_id:
        # 团队项目必须有所属组织，否则无法归属
        raise HTTPException(status_code=400, detail="团队项目需选择活动组织")
    script = Script(
        id=new_id(),
        title=title,
        scope=scope,
        owner_user_id=user.user_id,
        org_id=active_org_id if scope == "team" else None,
        created_by_user_id=user.user_id,
        visual_style=visual_style,
        unify_style=unify_style,
    )
    db.add(script)
    # 团队项目：创建者为 owner 成员
    if scope == "team":
        # 先写入父行，避免未声明 ORM relationship 时成员行抢先触发外键约束。
        await db.flush()
        db.add(
            ProjectMember(
                id=new_id(),
                script_id=script.id,
                user_id=user.user_id,
                project_role="owner",
            )
        )
    if commit:
        await db.commit()
        await db.refresh(script)
    else:
        await db.flush()
    return script


async def rename_script(
    db: AsyncSession,
    script: Script,
    title: str | None,
    *,
    visual_style: str | None = None,
    visual_style_provided: bool = False,
    unify_style: bool | None = None,
    commit: bool = True,
) -> Script:
    """重命名剧本，并允许调用方把审计纳入同一事务。"""
    if title is not None:
        script.title = title
    if visual_style_provided:
        script.visual_style = visual_style
    if unify_style is not None:
        script.unify_style = unify_style
    if commit:
        await db.commit()
        await db.refresh(script)
    else:
        await db.flush()
    return script


async def copy_script(
    db: AsyncSession,
    source: Script,
    user: User,
    *,
    title: str | None = None,
    scope: str | None = None,
    commit: bool = True,
) -> Script:
    """仅复制剧本和分集文本，所有派生任务、媒体与审核资产均不复制。"""
    target_scope = scope or source.scope
    suffix = "（副本）"
    target_title = title or f"{source.title[: 512 - len(suffix)]}{suffix}"
    target = await create_script(
        db,
        target_title,
        target_scope,
        user,
        visual_style=source.visual_style,
        unify_style=source.unify_style,
        commit=False,
    )
    episodes = await list_episodes(db, source.id)
    for episode in episodes:
        db.add(
            Episode(
                episode_id=new_id(),
                script_id=target.id,
                episode_no=episode.episode_no,
                title=episode.title,
                script_text=episode.script_text,
                schema_version=episode.schema_version,
                episode_version=1,
                client_save_seq=0,
            )
        )
    if commit:
        await db.commit()
        await db.refresh(target)
    else:
        await db.flush()
    return target


async def _count(db: AsyncSession, model: type, *criteria: Any) -> int:
    result = await db.execute(select(func.count()).select_from(model).where(*criteria))
    return int(result.scalar_one() or 0)


async def _enqueue_storage_deletes(
    db: AsyncSession,
    storage_keys: set[str],
    *,
    exclusions: StorageReferenceExclusions | None = None,
    asset_ids_by_key: dict[str, str] | None = None,
    upload_ids_by_key: dict[str, str] | None = None,
    protected_storage_keys: set[str] | None = None,
) -> None:
    """兼容既有内部调用；实际清理由全局判权服务统一实现。"""
    await enqueue_storage_deletes(
        db,
        storage_keys,
        exclusions=exclusions or StorageReferenceExclusions(),
        asset_ids_by_key=asset_ids_by_key,
        upload_ids_by_key=upload_ids_by_key,
        protected_storage_keys=protected_storage_keys,
    )


async def _collect_script_storage_keys(
    db: AsyncSession,
    script_id: str,
    uploads: list[MediaUpload],
) -> tuple[
    set[str],
    dict[str, str],
    dict[str, str],
    StorageReferenceExclusions,
]:
    """收集删除整个剧本时归属于该剧本的全部已知媒体对象。"""
    keys: set[str] = set()
    asset_ids_by_key: dict[str, str] = {}
    upload_ids_by_key: dict[str, str] = {}
    exclusions = StorageReferenceExclusions()

    asset_rows = (
        await db.execute(
            select(AssetEntry.id, AssetEntry.storage_key).where(
                AssetEntry.script_id == script_id
            )
        )
    ).all()
    for asset_id, storage_key in asset_rows:
        exclusions.asset_entry_ids.add(asset_id)
        collect_storage_keys(storage_key, keys)
        asset_ids_by_key[storage_key] = asset_id

    episode_rows = (
        await db.execute(
            select(Episode.episode_id, Episode.script_file_storage_key).where(
                Episode.script_id == script_id
            )
        )
    ).all()
    for episode_id, storage_key in episode_rows:
        exclusions.episode_ids.add(episode_id)
        collect_storage_keys(storage_key, keys)

    generation_rows = (
        await db.execute(
            select(
                GenerationJob.job_id,
                GenerationJob.output_json,
                GenerationJob.metrics,
            ).where(GenerationJob.script_id == script_id)
        )
    ).all()
    for job_id, output_json, metrics in generation_rows:
        exclusions.generation_job_ids.add(job_id)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(metrics, keys)

    video_rows = (
        await db.execute(
            select(
                VideoGenerationJob.job_id,
                VideoGenerationJob.input_json,
                VideoGenerationJob.output_json,
                VideoGenerationJob.metrics,
            ).where(VideoGenerationJob.script_id == script_id)
        )
    ).all()
    for job_id, input_json, output_json, metrics in video_rows:
        exclusions.video_generation_job_ids.add(job_id)
        collect_storage_keys(input_json, keys)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(metrics, keys)

    timeline_rows = (
        await db.execute(
            select(
                TimelineRenderJob.job_id,
                TimelineRenderJob.input_json,
                TimelineRenderJob.output_json,
                TimelineRenderJob.output_storage_key,
            ).where(TimelineRenderJob.script_id == script_id)
        )
    ).all()
    for job_id, input_json, output_json, output_storage_key in timeline_rows:
        exclusions.timeline_render_job_ids.add(job_id)
        collect_storage_keys(input_json, keys)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(output_storage_key, keys)

    storyboard_rows = (
        await db.execute(
            select(StoryboardJob.job_id, StoryboardJob.detail).where(
                StoryboardJob.script_id == script_id
            )
        )
    ).all()
    for job_id, detail in storyboard_rows:
        exclusions.storyboard_job_ids.add(job_id)
        collect_storage_keys(detail, keys)

    pipeline_rows = (
        await db.execute(
            select(PipelineJob.id, PipelineJob.output_json).where(
                PipelineJob.script_id == script_id
            )
        )
    ).all()
    for job_id, output_json in pipeline_rows:
        exclusions.pipeline_job_ids.add(job_id)
        collect_storage_keys(output_json, keys)

    material_ids = select(Materials.id).where(Materials.script_id == script_id)
    canvas_node_rows = (
        await db.execute(
            select(CanvasNode.id, CanvasNode.data).where(
                CanvasNode.material_id.in_(material_ids)
            )
        )
    ).all()
    for node_id, data in canvas_node_rows:
        exclusions.canvas_node_ids.add(node_id)
        collect_storage_keys(data, keys)
    canvas_edge_rows = (
        await db.execute(
            select(CanvasEdge.id, CanvasEdge.data).where(
                CanvasEdge.material_id.in_(material_ids)
            )
        )
    ).all()
    for edge_id, data in canvas_edge_rows:
        exclusions.canvas_edge_ids.add(edge_id)
        collect_storage_keys(data, keys)

    shot_group_rows = (
        await db.execute(
            select(
                ShotGroup.shot_group_id,
                ShotGroup.reference_frames,
                ShotGroup.materials,
                ShotGroup.audio_elements,
            ).where(ShotGroup.script_id == script_id)
        )
    ).all()
    for group_id, reference_frames, materials, audio_elements in shot_group_rows:
        exclusions.shot_group_ids.add(group_id)
        collect_storage_keys(reference_frames, keys)
        collect_storage_keys(materials, keys)
        collect_storage_keys(audio_elements, keys)

    for upload in uploads:
        exclusions.media_upload_ids.add(upload.id)
        collect_storage_keys(upload.temp_storage_key, keys)
        collect_storage_keys(upload.final_storage_key, keys)
        upload_ids_by_key[upload.temp_storage_key] = upload.id
        upload_ids_by_key[upload.final_storage_key] = upload.id
    return keys, asset_ids_by_key, upload_ids_by_key, exclusions


async def _collect_episode_storage_keys(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    deleted_subject_ids: set[str],
    generation_criterion: Any,
    uploads: list[MediaUpload],
) -> tuple[
    set[str],
    dict[str, str],
    dict[str, str],
    StorageReferenceExclusions,
]:
    """收集删除单集时会失去业务引用的媒体对象。"""
    keys: set[str] = set()
    asset_ids_by_key: dict[str, str] = {}
    upload_ids_by_key: dict[str, str] = {}
    exclusions = StorageReferenceExclusions(episode_ids={episode_id})

    if deleted_subject_ids:
        asset_rows = (
            await db.execute(
                select(AssetEntry.id, AssetEntry.storage_key).where(
                    AssetEntry.script_id == script_id,
                    AssetEntry.subject_id.in_(deleted_subject_ids),
                )
            )
        ).all()
        for asset_id, storage_key in asset_rows:
            exclusions.asset_entry_ids.add(asset_id)
            collect_storage_keys(storage_key, keys)
            asset_ids_by_key[storage_key] = asset_id

    episode_key = (
        await db.execute(
            select(Episode.script_file_storage_key).where(
                Episode.script_id == script_id,
                Episode.episode_id == episode_id,
            )
        )
    ).scalar_one_or_none()
    collect_storage_keys(episode_key, keys)

    generation_rows = (
        await db.execute(
            select(
                GenerationJob.job_id,
                GenerationJob.output_json,
                GenerationJob.metrics,
            ).where(generation_criterion)
        )
    ).all()
    for job_id, output_json, metrics in generation_rows:
        exclusions.generation_job_ids.add(job_id)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(metrics, keys)

    video_rows = (
        await db.execute(
            select(
                VideoGenerationJob.job_id,
                VideoGenerationJob.input_json,
                VideoGenerationJob.output_json,
                VideoGenerationJob.metrics,
            ).where(VideoGenerationJob.episode_id == episode_id)
        )
    ).all()
    for job_id, input_json, output_json, metrics in video_rows:
        exclusions.video_generation_job_ids.add(job_id)
        collect_storage_keys(input_json, keys)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(metrics, keys)

    timeline_rows = (
        await db.execute(
            select(
                TimelineRenderJob.job_id,
                TimelineRenderJob.input_json,
                TimelineRenderJob.output_json,
                TimelineRenderJob.output_storage_key,
            ).where(TimelineRenderJob.episode_id == episode_id)
        )
    ).all()
    for job_id, input_json, output_json, output_storage_key in timeline_rows:
        exclusions.timeline_render_job_ids.add(job_id)
        collect_storage_keys(input_json, keys)
        collect_storage_keys(output_json, keys)
        collect_storage_keys(output_storage_key, keys)

    storyboard_rows = (
        await db.execute(
            select(StoryboardJob.job_id, StoryboardJob.detail).where(
                StoryboardJob.episode_id == episode_id
            )
        )
    ).all()
    for job_id, detail in storyboard_rows:
        exclusions.storyboard_job_ids.add(job_id)
        collect_storage_keys(detail, keys)

    pipeline_rows = (
        await db.execute(
            select(PipelineJob.id, PipelineJob.output_json).where(
                PipelineJob.episode_id == episode_id
            )
        )
    ).all()
    for job_id, output_json in pipeline_rows:
        exclusions.pipeline_job_ids.add(job_id)
        collect_storage_keys(output_json, keys)

    shot_group_rows = (
        await db.execute(
            select(
                ShotGroup.shot_group_id,
                ShotGroup.reference_frames,
                ShotGroup.materials,
                ShotGroup.audio_elements,
            ).where(ShotGroup.episode_id == episode_id)
        )
    ).all()
    for group_id, reference_frames, materials, audio_elements in shot_group_rows:
        exclusions.shot_group_ids.add(group_id)
        collect_storage_keys(reference_frames, keys)
        collect_storage_keys(materials, keys)
        collect_storage_keys(audio_elements, keys)

    for upload in uploads:
        exclusions.media_upload_ids.add(upload.id)
        collect_storage_keys(upload.temp_storage_key, keys)
        collect_storage_keys(upload.final_storage_key, keys)
        upload_ids_by_key[upload.temp_storage_key] = upload.id
        upload_ids_by_key[upload.final_storage_key] = upload.id
    return keys, asset_ids_by_key, upload_ids_by_key, exclusions


def _has_retryable_financial_cleanup_debt(
    status: str,
    error_type: str | None,
    retryable: bool,
) -> bool:
    """Generation/Video reconciler 共享的权威财务补偿谓词。"""
    return (
        status == "failed"
        and error_type == "financial_cleanup"
        and retryable is True
    )


def _has_retryable_video_cleanup_debt(
    status: str,
    output_json: dict[str, Any] | None,
    error_type: str | None,
    retryable: bool,
) -> bool:
    """兼容既有内部调用，实际判定由视频清理策略统一维护。"""

    return has_retryable_video_cleanup_debt(
        status,
        output_json,
        error_type,
        retryable,
    )


def _has_pending_storyboard_financial_cleanup(
    financial_cleanup_status: str | None,
) -> bool:
    """分镜 reconciler 只以专用 pending 标记作为待清债事实。"""
    return financial_cleanup_status == "pending"


def _has_pending_timeline_financial_cleanup(
    status: str,
    output_json: dict[str, Any] | None,
) -> bool:
    """与 timeline reconciler 的候选及锁后校验保持完全一致。"""
    if status not in {"failed", "canceled"} or not isinstance(output_json, dict):
        return False
    cleanup = output_json.get("financial_cleanup")
    return (
        isinstance(cleanup, dict)
        and cleanup.get("status") == "pending"
        and cleanup.get("action") == "release"
        and bool(cleanup.get("reservation_id"))
    )


def _generation_jobs_for_episode(
    episode_id: str,
    deleted_subject_ids: set[str],
) -> Any:
    """覆盖显式 episode_id 及随独占主体删除的历史图片任务。"""
    criterion = GenerationJob.episode_id == episode_id
    if deleted_subject_ids:
        criterion = criterion | (
            GenerationJob.episode_id.is_(None)
            & GenerationJob.subject_id.in_(deleted_subject_ids)
        )
    return criterion


async def _deleted_subject_ids_for_episode(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    episode_no: int,
    subjects: list[Subject],
) -> set[str]:
    """计算删除分集时会真正硬删、而非共享或归档保留的主体。"""
    exclusive_subject_ids = {
        subject.id
        for subject in subjects
        if episode_no in (subject.episode_nos or [])
        and not [no for no in (subject.episode_nos or []) if no != episode_no]
    }
    if not exclusive_subject_ids:
        return set()
    retained_subject_ids = set(
        (
            await db.execute(
                select(GenerationJob.subject_id).where(
                    GenerationJob.script_id == script_id,
                    GenerationJob.subject_id.in_(exclusive_subject_ids),
                    GenerationJob.episode_id.is_not(None),
                    GenerationJob.episode_id != episode_id,
                )
            )
        ).scalars()
    )
    return exclusive_subject_ids - retained_subject_ids


async def _count_blocking_generation_jobs(
    db: AsyncSession,
    criterion: Any,
) -> int:
    rows = (
        await db.execute(
            select(
                GenerationJob.status,
                GenerationJob.error_type,
                GenerationJob.retryable,
            ).where(criterion)
        )
    ).all()
    return sum(
        status in _VIDEO_ACTIVE_STATUSES
        or _has_retryable_financial_cleanup_debt(status, error_type, retryable)
        for status, error_type, retryable in rows
    )


async def _count_blocking_storyboard_jobs(
    db: AsyncSession,
    criterion: Any,
) -> int:
    rows = (
        await db.execute(
            select(
                StoryboardJob.status,
                StoryboardJob.financial_cleanup_status,
            ).where(criterion)
        )
    ).all()
    return sum(
        status in _STORYBOARD_ACTIVE_STATUSES
        or _has_pending_storyboard_financial_cleanup(financial_cleanup_status)
        for status, financial_cleanup_status in rows
    )


async def _count_blocking_timeline_jobs(
    db: AsyncSession,
    criterion: Any,
) -> int:
    """以 Python 检查 JSON，保持 SQLite 与 PostgreSQL 行为一致。"""
    rows = (
        await db.execute(
            select(
                TimelineRenderJob.status,
                TimelineRenderJob.output_json,
            ).where(criterion)
        )
    ).all()
    return sum(
        status in _VIDEO_ACTIVE_STATUSES
        or _has_pending_timeline_financial_cleanup(status, output_json)
        for status, output_json in rows
    )


async def _count_blocking_video_jobs(db: AsyncSession, criterion: Any) -> int:
    """以 Python 检查 JSON，保持 SQLite 与 PostgreSQL 行为一致。"""
    rows = (
        await db.execute(
            select(
                VideoGenerationJob.status,
                VideoGenerationJob.output_json,
                VideoGenerationJob.error_type,
                VideoGenerationJob.retryable,
            ).where(criterion)
        )
    ).all()
    return sum(
        status in _VIDEO_ACTIVE_STATUSES
        or _has_retryable_video_cleanup_debt(
            status,
            output_json,
            error_type,
            retryable,
        )
        for status, output_json, error_type, retryable in rows
    )


async def _count_blocking_pipeline_jobs(
    db: AsyncSession,
    criterion: Any,
) -> int:
    """按 reconciler 的 source 约定识别失败 run 遗留的 reserved 预约。"""
    rows = (
        await db.execute(
            select(
                PipelineJob.status,
                PipelineJob.run_id,
            ).where(criterion)
        )
    ).all()
    failed_run_ids = {run_id for status, run_id in rows if status == "failed"}
    reserved_run_ids: set[str] = set()
    if failed_run_ids:
        reserved_run_ids = set(
            (
                await db.execute(
                    select(CreditReservation.source_id).where(
                        CreditReservation.source_type == "pipeline",
                        CreditReservation.source_id.in_(failed_run_ids),
                        CreditReservation.status == "reserved",
                    )
                )
            ).scalars()
        )
    return sum(
        status in _PIPELINE_ACTIVE_STATUSES
        or (status == "failed" and run_id in reserved_run_ids)
        for status, run_id in rows
    )


def _raise_financial_cleanup_conflict(kind: str, debt_count: int) -> None:
    if debt_count:
        raise ConflictError(
            f"仍有 {debt_count} 个{kind}任务存在可重试的财务清理债务，"
            "请等待后台清理完成后再删除"
        )


async def _lock_generation_jobs_for_delete(
    db: AsyncSession,
    criterion: Any,
) -> list[GenerationJob]:
    jobs = list(
        (
            await db.execute(
                select(GenerationJob)
                .where(criterion)
                .order_by(GenerationJob.job_id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    _raise_financial_cleanup_conflict(
        "图片生成",
        sum(
            _has_retryable_financial_cleanup_debt(
                job.status,
                job.error_type,
                job.retryable,
            )
            for job in jobs
        ),
    )
    return jobs


async def _lock_storyboard_jobs_for_delete(
    db: AsyncSession,
    criterion: Any,
) -> list[StoryboardJob]:
    jobs = list(
        (
            await db.execute(
                select(StoryboardJob)
                .where(criterion)
                .order_by(StoryboardJob.job_id.asc())
                .options(noload(StoryboardJob.script))
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    _raise_financial_cleanup_conflict(
        "分镜生成",
        sum(
            _has_pending_storyboard_financial_cleanup(
                job.financial_cleanup_status
            )
            for job in jobs
        ),
    )
    return jobs


async def _lock_timeline_jobs_for_delete(
    db: AsyncSession,
    criterion: Any,
) -> list[TimelineRenderJob]:
    jobs = list(
        (
            await db.execute(
                select(TimelineRenderJob)
                .where(criterion)
                .order_by(TimelineRenderJob.job_id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    _raise_financial_cleanup_conflict(
        "时间线渲染",
        sum(
            _has_pending_timeline_financial_cleanup(
                job.status,
                job.output_json,
            )
            for job in jobs
        ),
    )
    return jobs


async def _lock_video_jobs_for_delete(
    db: AsyncSession,
    criterion: Any,
) -> list[VideoGenerationJob]:
    """锁定并刷新视频行，阻止删除穿透并发写入的外部清理债务。"""
    jobs = list(
        (
            await db.execute(
                select(VideoGenerationJob)
                .where(criterion)
                .order_by(VideoGenerationJob.job_id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    debt_count = sum(
        _has_retryable_video_cleanup_debt(
            job.status,
            job.output_json,
            job.error_type,
            job.retryable,
        )
        for job in jobs
    )
    if debt_count:
        raise ConflictError(
            f"仍有 {debt_count} 个视频任务存在待人工核对或可重试的远端取消、"
            "媒体清理债务或财务清理债务，"
            "请等待后台清理完成后再删除"
        )
    return jobs


async def _lock_pipeline_jobs_for_delete(
    db: AsyncSession,
    criterion: Any,
) -> list[PipelineJob]:
    """锁定流水线行后按 run source 锁预约，避免删除丢失可恢复冻结积分。"""
    jobs = list(
        (
            await db.execute(
                select(PipelineJob)
                .where(criterion)
                .options(noload(PipelineJob.script))
                .order_by(PipelineJob.run_id.asc(), PipelineJob.id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    failed_run_ids = {job.run_id for job in jobs if job.status == "failed"}
    reservations: list[CreditReservation] = []
    if failed_run_ids:
        reservations = list(
            (
                await db.execute(
                    select(CreditReservation)
                    .where(
                        CreditReservation.source_type == "pipeline",
                        CreditReservation.source_id.in_(failed_run_ids),
                    )
                    .order_by(
                        CreditReservation.source_id.asc(),
                        CreditReservation.id.asc(),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
    _raise_financial_cleanup_conflict(
        "流水线",
        sum(reservation.status == "reserved" for reservation in reservations),
    )
    return jobs


async def get_script_dependency_summary(
    db: AsyncSession, script_id: str
) -> LifecycleDependencySummary:
    """汇总硬删除剧本会清理的派生资源。"""
    shot_group_ids = select(ShotGroup.shot_group_id).where(ShotGroup.script_id == script_id)
    job_count = sum(
        (
            await _count(db, PipelineJob, PipelineJob.script_id == script_id),
            await _count(db, StoryboardJob, StoryboardJob.script_id == script_id),
            await _count(db, GenerationJob, GenerationJob.script_id == script_id),
            await _count(db, TimelineRenderJob, TimelineRenderJob.script_id == script_id),
        )
    )
    active_job_count = sum(
        (
            await _count_blocking_pipeline_jobs(
                db,
                PipelineJob.script_id == script_id,
            ),
            await _count_blocking_storyboard_jobs(
                db,
                StoryboardJob.script_id == script_id,
            ),
            await _count_blocking_generation_jobs(
                db,
                GenerationJob.script_id == script_id,
            ),
            await _count_blocking_video_jobs(
                db, VideoGenerationJob.script_id == script_id
            ),
            await _count_blocking_timeline_jobs(
                db,
                TimelineRenderJob.script_id == script_id,
            ),
        )
    )
    return LifecycleDependencySummary(
        episode_count=await _count(db, Episode, Episode.script_id == script_id),
        subject_count=await _count(db, Subject, Subject.script_id == script_id),
        asset_count=await _count(db, AssetEntry, AssetEntry.script_id == script_id),
        shot_group_count=await _count(db, ShotGroup, ShotGroup.script_id == script_id),
        shot_count=await _count(db, Shot, Shot.shot_group_id.in_(shot_group_ids)),
        video_count=await _count(
            db, VideoGenerationJob, VideoGenerationJob.script_id == script_id
        ),
        job_count=job_count,
        active_job_count=active_job_count,
        canvas_count=await _count(db, Materials, Materials.script_id == script_id),
    )


async def _cancel_lifecycle_dispatches(
    db: AsyncSession,
    *,
    video_criterion: Any,
    generation_criterion: Any,
    timeline_criterion: Any,
    storyboard_criterion: Any,
    pipeline_criterion: Any,
) -> None:
    """按固定表序锁定、复检债务，再收敛无外键活动投递。"""
    video_jobs = await _lock_video_jobs_for_delete(db, video_criterion)
    generation_jobs = await _lock_generation_jobs_for_delete(
        db,
        generation_criterion,
    )
    timeline_jobs = await _lock_timeline_jobs_for_delete(db, timeline_criterion)
    storyboard_jobs = await _lock_storyboard_jobs_for_delete(
        db,
        storyboard_criterion,
    )
    pipeline_jobs = await _lock_pipeline_jobs_for_delete(db, pipeline_criterion)
    pipeline_run_ids = {job.run_id for job in pipeline_jobs}

    for task_type, jobs in (
        ("video.generate", video_jobs),
        ("generation.generate_images", generation_jobs),
        ("timeline.render", timeline_jobs),
        ("storyboard.generate", storyboard_jobs),
    ):
        for job in jobs:
            await cancel_outbox_task(
                db,
                task_type=task_type,
                job_id=job.job_id,
                attempt=job.attempt,
            )

    for run_id in pipeline_run_ids:
        await cancel_outbox_task(
            db,
            task_type="pipeline.dag",
            job_id=run_id,
        )


async def _cancel_script_dispatches(db: AsyncSession, script_id: str) -> None:
    """剧本硬删除前锁定任务并收敛全部活动投递。"""
    await _cancel_lifecycle_dispatches(
        db,
        video_criterion=VideoGenerationJob.script_id == script_id,
        generation_criterion=GenerationJob.script_id == script_id,
        timeline_criterion=TimelineRenderJob.script_id == script_id,
        storyboard_criterion=StoryboardJob.script_id == script_id,
        pipeline_criterion=PipelineJob.script_id == script_id,
    )


async def _cancel_episode_dispatches(
    db: AsyncSession,
    *,
    episode_id: str,
    generation_criterion: Any,
) -> None:
    """分集硬删除沿用与剧本删除相同的任务锁序和投递收敛。"""
    await _cancel_lifecycle_dispatches(
        db,
        video_criterion=VideoGenerationJob.episode_id == episode_id,
        generation_criterion=generation_criterion,
        timeline_criterion=TimelineRenderJob.episode_id == episode_id,
        storyboard_criterion=StoryboardJob.episode_id == episode_id,
        pipeline_criterion=PipelineJob.episode_id == episode_id,
    )


async def delete_script(db: AsyncSession, script: Script, *, commit: bool = True) -> None:
    """按外键拓扑硬删除剧本及派生数据。"""
    script_id = script.id
    locked_script = (
        await db.execute(select(Script).where(Script.id == script_id).with_for_update())
    ).scalar_one_or_none()
    if locked_script is None:
        raise NotFoundError("剧本不存在")
    await acquire_storage_reference_barrier(db)
    await _cancel_script_dispatches(db, script_id)
    material_ids = select(Materials.id).where(Materials.script_id == script_id)
    subject_ids = select(Subject.id).where(Subject.script_id == script_id)
    asset_ids = select(AssetEntry.id).where(AssetEntry.script_id == script_id)
    shot_group_ids = select(ShotGroup.shot_group_id).where(ShotGroup.script_id == script_id)

    locked_uploads = list(
        (
            await db.execute(
                select(MediaUpload)
                .where(MediaUpload.owner_script_id == script_id)
                .order_by(MediaUpload.id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    (
        storage_keys,
        asset_ids_by_key,
        upload_ids_by_key,
        exclusions,
    ) = await _collect_script_storage_keys(db, script_id, locked_uploads)
    decision = await decide_storage_key_deletes(
        db,
        storage_keys,
        exclusions=exclusions,
    )
    protected_keys = set(decision.protected_keys)
    deletion_keys = set(decision.deletable_keys)
    now = datetime.now(UTC)
    for upload in locked_uploads:
        final_key = next(
            iter(normalize_storage_keys({upload.final_storage_key})),
            "",
        )
        temp_key = next(
            iter(normalize_storage_keys({upload.temp_storage_key})),
            "",
        )
        if upload.asset_id:
            asset_ids_by_key.setdefault(upload.final_storage_key, upload.asset_id)
        if final_key in protected_keys:
            if upload.status == "staging":
                deletion_keys.discard(temp_key)
            continue
        upload.status = "deleted"
        upload.completed_at = now
    await enqueue_storage_deletes(
        db,
        deletion_keys,
        exclusions=exclusions,
        asset_ids_by_key=asset_ids_by_key,
        upload_ids_by_key=upload_ids_by_key,
        protected_storage_keys=protected_keys,
    )

    # 先断开 Subject.image_entry_id 与资产版本自引用，再删除叶子资源。
    await db.execute(
        update(Subject).where(Subject.script_id == script_id).values(image_entry_id=None)
    )
    await db.execute(
        update(AssetEntry)
        .where(AssetEntry.superseded_by_id.in_(asset_ids))
        .values(status="deleted", is_approved=False, superseded=False, superseded_by_id=None)
    )
    for model, criterion in (
        (VideoTimelineArrangement, VideoTimelineArrangement.script_id == script_id),
        (TimelineRenderJob, TimelineRenderJob.script_id == script_id),
        (VideoGenerationJob, VideoGenerationJob.script_id == script_id),
        (StoryboardJob, StoryboardJob.script_id == script_id),
        (GenerationJob, GenerationJob.script_id == script_id),
        (Shot, Shot.shot_group_id.in_(shot_group_ids)),
        (ShotGroup, ShotGroup.script_id == script_id),
        (
            Scene,
            Scene.episode_id.in_(
                select(Episode.episode_id).where(Episode.script_id == script_id)
            ),
        ),
        (PipelineJob, PipelineJob.script_id == script_id),
        (PipelineStage, PipelineStage.script_id == script_id),
        (CanvasEdge, CanvasEdge.material_id.in_(material_ids)),
        (CanvasNode, CanvasNode.material_id.in_(material_ids)),
        (AssetEntry, AssetEntry.script_id == script_id),
        (Subject, Subject.id.in_(subject_ids)),
        (ParentSubject, ParentSubject.script_id == script_id),
        (Materials, Materials.script_id == script_id),
        (ProjectMember, ProjectMember.script_id == script_id),
        (Episode, Episode.script_id == script_id),
    ):
        await db.execute(delete(model).where(criterion))
    await db.execute(delete(Script).where(Script.id == script_id))
    if commit:
        await db.commit()
    else:
        await db.flush()


# --------------------------------------------------------------------------- #
# Episode
# --------------------------------------------------------------------------- #
async def list_episodes(db: AsyncSession, script_id: str) -> list[Episode]:
    result = await db.execute(
        select(Episode)
        .where(Episode.script_id == script_id)
        .order_by(Episode.episode_no.asc())
    )
    return list(result.scalars().all())


async def get_episode(
    db: AsyncSession, script_id: str, episode_id: str
) -> Episode:
    return await _load_episode_for_script(db, script_id, episode_id)


async def get_episode_for_update(
    db: AsyncSession, script_id: str, episode_id: str
) -> Episode:
    """在剧本父行之后锁定分集，串行化依赖检查、任务创建与删除。"""
    result = await db.execute(
        select(Episode)
        .where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
        )
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    episode = result.scalar_one_or_none()
    if episode is None:
        # 保留跨剧本资源使用 403 的既有 IDOR 契约。
        return await _load_episode_for_script(db, script_id, episode_id)
    return episode


async def _load_episode_for_script(
    db: AsyncSession, script_id: str, episode_id: str
) -> Episode:
    """按全局 episode_id 加载，并校验其归属的剧本。"""
    result = await db.execute(
        select(Episode).where(Episode.episode_id == episode_id)
    )
    episode = result.scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.script_id != script_id:
        raise ForbiddenError("无权访问该分集")
    return episode


async def create_episode(
    db: AsyncSession,
    script_id: str,
    title: str,
    script_text: str | None,
    episode_no: int | None,
) -> Episode:
    # 自动取最大集号 + 1
    if episode_no is None:
        max_result = await db.execute(
            select(func.max(Episode.episode_no)).where(Episode.script_id == script_id)
        )
        max_no = max_result.scalar()
        episode_no = int(max_no or 0) + 1

    episode = Episode(
        episode_id=new_id(),
        script_id=script_id,
        episode_no=episode_no,
        title=title,
        script_text=script_text,
        schema_version=1,
        episode_version=1,
        client_save_seq=0,
    )
    db.add(episode)
    try:
        await db.commit()
    except IntegrityError as exc:
        # (script_id, episode_no) 唯一约束冲突 → 409 而非 500
        await db.rollback()
        raise ConflictError(f"集号已存在: {episode_no}") from exc
    await db.refresh(episode)
    return episode


async def save_episode(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
    script_text: str | None,
    episode_version: int,
    client_save_seq: int,
    title: str | None = None,
    episode_no: int | None = None,
    *,
    commit: bool = True,
) -> Episode:
    """带乐观锁的分集保存。

    WHERE episode_version=? AND client_save_seq=?，affected==0 则 409。
    成功后 episode_version 与 client_save_seq 均递增。
    """
    if script_text is not None:
        # 自动流水线会在同一事务插入子任务；必须先锁 Script，再触碰 Episode。
        locked_script_id = (
            await db.execute(
                select(Script.id)
                .where(Script.id == script_id)
                .with_for_update(read=True, key_share=True)
            )
        ).scalar_one_or_none()
        if locked_script_id is None:
            raise NotFoundError("剧本不存在")
        bind = getattr(db, "bind", None)
        if bind is None:
            bind = getattr(getattr(db, "session", None), "bind", None)
        if getattr(getattr(bind, "dialect", None), "name", None) == "postgresql":
            await db.execute(
                select(
                    func.pg_advisory_xact_lock(
                        func.hashtext("pipeline_trigger"),
                        func.hashtext(script_id),
                    )
                )
            )
    await _load_episode_for_script(db, script_id, episode_id)

    # 原子更新，避免并发覆盖
    values: dict = {
        "episode_version": episode_version + 1,
        "client_save_seq": client_save_seq + 1,
    }
    if script_text is not None:
        values["script_text"] = script_text
    if title is not None:
        values["title"] = title
    if episode_no is not None:
        values["episode_no"] = episode_no

    stmt = (
        update(Episode)
        .where(
            Episode.episode_id == episode_id,
            Episode.script_id == script_id,
            Episode.episode_version == episode_version,
            Episode.client_save_seq == client_save_seq,
        )
        .values(**values)
    )
    try:
        result = cast(CursorResult[Any], await db.execute(stmt))
    except IntegrityError as exc:
        raise ConflictError(f"集号已存在: {episode_no}") from exc
    if result.rowcount == 0:
        await _load_episode_for_script(db, script_id, episode_id)
        raise ConflictError("版本冲突，请刷新后重试")
    if commit:
        await db.commit()
    else:
        await db.flush()
    # 重新加载以拿到递增后的版本号
    episode = await get_episode(db, script_id, episode_id)
    return episode


async def get_episode_dependency_summary(
    db: AsyncSession, script_id: str, episode_id: str
) -> LifecycleDependencySummary:
    """返回分集关联的主体、镜头、资产、视频与任务数量。"""
    episode = await _load_episode_for_script(db, script_id, episode_id)
    subjects = list(
        (
            await db.execute(select(Subject).where(Subject.script_id == script_id))
        ).scalars().all()
    )
    related_subject_ids = [
        subject.id
        for subject in subjects
        if episode.episode_no in (subject.episode_nos or [])
    ]
    deleted_subject_ids = await _deleted_subject_ids_for_episode(
        db,
        script_id=script_id,
        episode_id=episode_id,
        episode_no=episode.episode_no,
        subjects=subjects,
    )
    generation_criterion = _generation_jobs_for_episode(
        episode_id,
        deleted_subject_ids,
    )
    shot_group_ids = select(ShotGroup.shot_group_id).where(
        ShotGroup.episode_id == episode_id
    )
    job_count = sum(
        (
            await _count(db, PipelineJob, PipelineJob.episode_id == episode_id),
            await _count(db, StoryboardJob, StoryboardJob.episode_id == episode_id),
            await _count(db, GenerationJob, generation_criterion),
            await _count(db, TimelineRenderJob, TimelineRenderJob.episode_id == episode_id),
        )
    )
    active_job_count = sum(
        (
            await _count_blocking_pipeline_jobs(
                db,
                PipelineJob.episode_id == episode_id,
            ),
            await _count_blocking_storyboard_jobs(
                db,
                StoryboardJob.episode_id == episode_id,
            ),
            await _count_blocking_generation_jobs(
                db,
                generation_criterion,
            ),
            await _count_blocking_video_jobs(
                db, VideoGenerationJob.episode_id == episode_id
            ),
            await _count_blocking_timeline_jobs(
                db,
                TimelineRenderJob.episode_id == episode_id,
            ),
        )
    )
    return LifecycleDependencySummary(
        subject_count=len(related_subject_ids),
        asset_count=(
            await _count(db, AssetEntry, AssetEntry.subject_id.in_(deleted_subject_ids))
            if deleted_subject_ids
            else 0
        ),
        shot_group_count=await _count(
            db, ShotGroup, ShotGroup.episode_id == episode_id
        ),
        shot_count=await _count(db, Shot, Shot.shot_group_id.in_(shot_group_ids)),
        video_count=await _count(
            db, VideoGenerationJob, VideoGenerationJob.episode_id == episode_id
        ),
        job_count=job_count,
        active_job_count=active_job_count,
    )


async def delete_episode(
    db: AsyncSession,
    script_id: str,
    episode_id: str,
    *,
    commit: bool = True,
) -> Episode:
    """删除分集派生数据；共享主体仅移除该集号，单集主体及其资产一并删除。"""
    from app.crud import asset as asset_crud

    # 统一锁序为 Script -> Episode -> Video -> Generation -> Timeline -> Storyboard -> Pipeline。
    await get_script_for_update(db, script_id)
    episode = await get_episode_for_update(db, script_id, episode_id)
    await acquire_storage_reference_barrier(db)
    subjects = list(
        (
            await db.execute(select(Subject).where(Subject.script_id == script_id))
        ).scalars().all()
    )
    related = [s for s in subjects if episode.episode_no in (s.episode_nos or [])]
    deleted_subject_ids = await _deleted_subject_ids_for_episode(
        db,
        script_id=script_id,
        episode_id=episode_id,
        episode_no=episode.episode_no,
        subjects=subjects,
    )
    generation_criterion = _generation_jobs_for_episode(
        episode_id,
        deleted_subject_ids,
    )
    await _cancel_episode_dispatches(
        db,
        episode_id=episode_id,
        generation_criterion=generation_criterion,
    )
    shot_group_ids = select(ShotGroup.shot_group_id).where(
        ShotGroup.episode_id == episode_id
    )

    locked_uploads: list[MediaUpload] = []
    if deleted_subject_ids:
        locked_uploads = list(
            (
                await db.execute(
                    select(MediaUpload)
                    .where(MediaUpload.subject_id.in_(deleted_subject_ids))
                    .order_by(MediaUpload.id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
    (
        storage_keys,
        asset_ids_by_key,
        upload_ids_by_key,
        exclusions,
    ) = await _collect_episode_storage_keys(
        db,
        script_id=script_id,
        episode_id=episode_id,
        deleted_subject_ids=deleted_subject_ids,
        generation_criterion=generation_criterion,
        uploads=locked_uploads,
    )
    decision = await decide_storage_key_deletes(
        db,
        storage_keys,
        exclusions=exclusions,
    )
    protected_keys = set(decision.protected_keys)
    deletion_keys = set(decision.deletable_keys)
    now = datetime.now(UTC)
    for upload in locked_uploads:
        final_key = next(
            iter(normalize_storage_keys({upload.final_storage_key})),
            "",
        )
        temp_key = next(
            iter(normalize_storage_keys({upload.temp_storage_key})),
            "",
        )
        if upload.asset_id:
            asset_ids_by_key.setdefault(upload.final_storage_key, upload.asset_id)
        if final_key in protected_keys:
            if upload.status == "staging":
                deletion_keys.discard(temp_key)
            continue
        upload.status = "deleted"
        upload.completed_at = now
    await enqueue_storage_deletes(
        db,
        deletion_keys,
        exclusions=exclusions,
        asset_ids_by_key=asset_ids_by_key,
        upload_ids_by_key=upload_ids_by_key,
        protected_storage_keys=protected_keys,
    )

    for model, criterion in (
        (VideoTimelineArrangement, VideoTimelineArrangement.episode_id == episode_id),
        (TimelineRenderJob, TimelineRenderJob.episode_id == episode_id),
        (VideoGenerationJob, VideoGenerationJob.episode_id == episode_id),
        (StoryboardJob, StoryboardJob.episode_id == episode_id),
        (GenerationJob, generation_criterion),
        (Shot, Shot.shot_group_id.in_(shot_group_ids)),
        (ShotGroup, ShotGroup.episode_id == episode_id),
        (Scene, Scene.episode_id == episode_id),
        (PipelineJob, PipelineJob.episode_id == episode_id),
    ):
        await db.execute(delete(model).where(criterion))

    # 其他显式分集任务仍通过非空 FK 引用的主体转为归档，不能连同资产删除。
    for subject in related:
        remaining = [no for no in (subject.episode_nos or []) if no != episode.episode_no]
        if remaining:
            subject.episode_nos = remaining
        elif subject.id not in deleted_subject_ids:
            subject.episode_nos = []
            subject.is_active = False
        else:
            await asset_crud.delete_subject(
                db,
                subject,
                parent_delete_prepared=True,
            )

    await db.execute(delete(Episode).where(Episode.episode_id == episode_id))
    if commit:
        await db.commit()
    else:
        await db.flush()
    return episode


# --------------------------------------------------------------------------- #
# Breakdown
# --------------------------------------------------------------------------- #
async def get_breakdown(
    db: AsyncSession, script_id: str, episode_id: str
) -> dict:
    """返回实际 episode/subject/scene/shot_group/shot 层级。

    subjects 为该剧本全部主体（breakdown 视角，含 episode_nos），
    shots/shot_groups 为空（分镜生成阶段产出，此阶段尚未生成）。
    """
    episode = await get_episode(db, script_id, episode_id)

    subjects_result = await db.execute(
        select(Subject)
        .where(
            Subject.script_id == script_id,
            Subject.is_active.is_(True),
        )
        .order_by(Subject.type.asc(), Subject.name.asc())
    )
    subjects = [
        subject
        for subject in subjects_result.scalars().all()
        if episode.episode_no in (subject.episode_nos or [])
    ]

    scenes_result = await db.execute(
        select(Scene).where(Scene.episode_id == episode_id).order_by(Scene.scene_no.asc())
    )
    scenes = list(scenes_result.scalars().all())
    groups_result = await db.execute(
        select(ShotGroup)
        .where(ShotGroup.episode_id == episode_id)
        .order_by(ShotGroup.scene_no.asc(), ShotGroup.group_no.asc())
    )
    shot_groups = list(groups_result.scalars().all())
    group_ids = [item.shot_group_id for item in shot_groups]
    shots: list[Shot] = []
    if group_ids:
        shots_result = await db.execute(
            select(Shot)
            .where(Shot.shot_group_id.in_(group_ids))
            .order_by(Shot.scene_no.asc(), Shot.group_no.asc(), Shot.display_no.asc())
        )
        shots = list(shots_result.scalars().all())

    return {
        "episode": episode,
        "subjects": subjects,
        "scenes": scenes,
        "shots": shots,
        "shot_groups": shot_groups,
    }


# --------------------------------------------------------------------------- #
# 主体/父主体查询（pipeline-status / extract-status 复用）
# --------------------------------------------------------------------------- #
async def list_subjects(db: AsyncSession, script_id: str) -> list[Subject]:
    result = await db.execute(
        select(Subject)
        .where(
            Subject.script_id == script_id,
            Subject.is_active.is_(True),
        )
        .order_by(Subject.type.asc(), Subject.name.asc())
    )
    return list(result.scalars().all())


async def list_parent_subjects(
    db: AsyncSession, script_id: str
) -> list[ParentSubject]:
    result = await db.execute(
        select(ParentSubject)
        .where(ParentSubject.script_id == script_id)
        .order_by(ParentSubject.type.asc(), ParentSubject.name.asc())
    )
    return list(result.scalars().all())


async def replace_subjects(
    db: AsyncSession, script_id: str, merged_subjects: list[dict]
) -> list[Subject]:
    """用合并后的主体列表整体替换该剧本的主体（merge 阶段调用）。

    同名同类主体复用既有 subject_id（先查再 upsert），避免
    image_entry_id / AssetEntry.subject_id 等外键引用孤立。
    """
    # 先加载既有主体，按 (type, name) 建立复用索引
    old_result = await db.execute(
        select(Subject).where(Subject.script_id == script_id)
    )
    old_subjects = list(old_result.scalars().all())
    existing_index: dict[tuple[str, str], Subject] = {
        (s.type, s.name): s for s in old_subjects
    }
    existing_by_id = {subject.id: subject for subject in old_subjects}

    # 被资产引用的主体必须保留，避免破坏 T4 的历史资产生命周期。
    from app.models.asset_entry import AssetEntry

    referenced_ids: set[str] = set()
    if old_subjects:
        referenced_result = await db.execute(
            select(AssetEntry.subject_id).where(
                AssetEntry.subject_id.in_([subject.id for subject in old_subjects])
            )
        )
        referenced_ids = set(referenced_result.scalars().all())

    # 本轮保留的主体 id 集合，用于删除真正被淘汰且无资产引用的旧主体
    kept_ids: set[str] = set()

    new_subjects: list[Subject] = []
    for item in merged_subjects:
        key = (item["type"], item["name"])
        staged = "base_subject_version" in item
        staged_id = item.get("id")
        if staged and item.get("base_subject_version") is not None:
            if not isinstance(staged_id, str):
                raise ConflictError("流水线暂存主体缺少有效 ID")
            subject = existing_by_id.get(staged_id)
            if subject is None or (subject.type, subject.name) != key:
                raise ConflictError("主体在流水线发布前已被修改")
            if (
                int(subject.subject_version or 1)
                != int(item["base_subject_version"])
                or int(subject.enrich_version or 1)
                != int(item["base_enrich_version"])
            ):
                raise ConflictError("主体版本已变化，拒绝发布旧流水线结果")
        elif staged:
            if staged_id in existing_by_id or key in existing_index:
                raise ConflictError("主体在流水线发布前已由其他事务创建")
            subject = None
        else:
            subject = existing_index.get(key)
        if subject is not None:
            # 复用既有 subject_id，仅更新可变字段
            subject.description = item.get("description")
            subject.is_child = bool(item.get("is_child", True))
            subject.is_parent = False
            subject.parent_id = None
            subject.episode_nos = item.get("episode_nos")
            subject.is_active = True
            if staged:
                subject.subject_version = int(item["subject_version"])
                subject.enrich_version = int(item["enrich_version"])
            kept_ids.add(subject.id)
        else:
            subject = Subject(
                id=staged_id or new_id(),
                script_id=script_id,
                type=item["type"],
                name=item["name"],
                description=item.get("description"),
                is_child=bool(item.get("is_child", True)),
                is_parent=False,
                parent_id=None,
                episode_nos=item.get("episode_nos"),
                is_active=True,
                review_status="not_designed",
                subject_version=int(item.get("subject_version") or 1),
                enrich_version=int(item.get("enrich_version") or 1),
            )
            db.add(subject)
            new_subjects.append(subject)
            kept_ids.add(subject.id)
    # 删除未在本轮合并结果中保留的旧主体
    for old in old_subjects:
        if old.id in kept_ids:
            continue
        if old.id in referenced_ids:
            old.is_active = False
            old.is_parent = False
            old.parent_id = None
        else:
            await db.delete(old)
    await db.flush()
    return new_subjects


async def replace_parent_subjects(
    db: AsyncSession, script_id: str, parents: list[dict]
) -> list[ParentSubject]:
    """整体替换父主体并回写子主体 parent_id；子主体始终不是父主体。"""
    children_result = await db.execute(
        select(Subject).where(
            Subject.script_id == script_id,
            Subject.is_active.is_(True),
        )
    )
    children = list(children_result.scalars().all())
    child_ids = {child.id for child in children}
    assigned_ids: set[str] = set()
    for item in parents:
        ids = item.get("child_ids") or []
        if int(item.get("child_count", 0)) != len(ids) or not set(ids) <= child_ids:
            raise ValueError("parent child_ids 与 child_count 或剧本主体不一致")
        if assigned_ids.intersection(ids):
            raise ValueError("子主体不能同时归属多个父主体")
        assigned_ids.update(ids)

    # 删除旧父主体
    old_result = await db.execute(
        select(ParentSubject).where(ParentSubject.script_id == script_id)
    )
    for old in old_result.scalars().all():
        await db.delete(old)

    # 重置子主体的父标记
    for child in children:
        child.is_parent = False
        child.parent_id = None

    new_parents: list[ParentSubject] = []
    for item in parents:
        parent = ParentSubject(
            id=new_id(),
            script_id=script_id,
            name=item["name"],
            type=item["type"],
            description=item.get("description"),
            child_count=int(item.get("child_count", 0)),
        )
        db.add(parent)
        new_parents.append(parent)
        # 回写子主体引用
        parent_child_ids: set[str] = set(item.get("child_ids") or [])
        for child in children:
            if child.id in parent_child_ids:
                child.is_child = True
                child.is_parent = False
                child.parent_id = parent.id
    await db.flush()
    return new_parents
