"""剧本/分集/主体数据访问层。

- list_scripts: 支持 scope personal/team，团队项目带 current_user_project_role
- save_episode: 带乐观锁（episode_version + client_save_seq），冲突 409
- get_breakdown: 返回 {episode, subjects, shots, shot_groups}
"""

import hashlib
import logging
from typing import Any, cast

from fastapi import HTTPException
from sqlalchemy import delete, false, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, ForbiddenError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
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

logger = logging.getLogger(__name__)

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
    )
    db.add(script)
    # 团队项目：创建者为 owner 成员
    if scope == "team":
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
    title: str,
    *,
    commit: bool = True,
) -> Script:
    """重命名剧本，并允许调用方把审计纳入同一事务。"""
    script.title = title
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


_STORAGE_KEY_FIELDS = {
    "storage_key",
    "storagekey",
    "video_storage_key",
    "output_storage_key",
    "last_frame",
    "first_frame",
    "reference_frame",
}


def _collect_storage_keys(value: Any, destination: set[str]) -> None:
    """从任务输出或镜头帧 JSON 中递归提取对象存储键。"""
    if isinstance(value, dict):
        for field, item in value.items():
            normalized_field = str(field).replace("-", "_").lower()
            if normalized_field in _STORAGE_KEY_FIELDS and isinstance(item, str):
                destination.add(item)
            _collect_storage_keys(item, destination)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _collect_storage_keys(item, destination)


async def _enqueue_storage_deletes(
    db: AsyncSession,
    storage_keys: set[str],
    *,
    asset_ids_by_key: dict[str, str] | None = None,
) -> None:
    """在业务事务中先阻断关联晋升，再幂等写入物理对象删除任务。"""
    from app.providers.storage import validate_key_prefix
    from app.services.media_storage_jobs import cancel_promotes_for_delete, enqueue_delete

    normalized_asset_ids: dict[str, str] = {}
    for key, asset_id in (asset_ids_by_key or {}).items():
        try:
            normalized_asset_ids[validate_key_prefix(key)] = asset_id
        except ValueError:
            # 存量非法 key 无法安全交给存储驱动；不应因此阻断业务资源删除。
            logger.warning("跳过不受支持的存量媒体键 asset_id=%s", asset_id)

    normalized_keys: set[str] = set()
    for raw_key in storage_keys:
        try:
            normalized_keys.add(validate_key_prefix(raw_key))
        except ValueError:
            logger.warning("跳过不受支持的存量媒体键")

    promote_keys = await cancel_promotes_for_delete(
        db,
        asset_ids=set((asset_ids_by_key or {}).values()),
        final_keys=normalized_keys,
    )
    for raw_key in promote_keys:
        try:
            normalized_keys.add(validate_key_prefix(raw_key))
        except ValueError:
            logger.warning("跳过不受支持的晋升任务媒体键")

    for key in sorted(normalized_keys):
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        await enqueue_delete(
            db,
            idempotency_key=f"lifecycle-delete:{digest}",
            source_key=key,
            asset_id=normalized_asset_ids.get(key),
        )


async def _collect_script_storage_keys(
    db: AsyncSession,
    script_id: str,
) -> tuple[set[str], dict[str, str]]:
    """收集删除整个剧本时归属于该剧本的全部已知媒体对象。"""
    keys: set[str] = set()
    asset_ids_by_key: dict[str, str] = {}

    asset_rows = (
        await db.execute(
            select(AssetEntry.id, AssetEntry.storage_key).where(
                AssetEntry.script_id == script_id
            )
        )
    ).all()
    for asset_id, storage_key in asset_rows:
        if storage_key:
            keys.add(storage_key)
            asset_ids_by_key[storage_key] = asset_id

    episode_keys = (
        await db.execute(
            select(Episode.script_file_storage_key).where(Episode.script_id == script_id)
        )
    ).scalars().all()
    keys.update(key for key in episode_keys if key)

    for model, field in (
        (GenerationJob, GenerationJob.output_json),
        (VideoGenerationJob, VideoGenerationJob.output_json),
        (TimelineRenderJob, TimelineRenderJob.output_json),
    ):
        values = (
            await db.execute(select(field).where(model.script_id == script_id))
        ).scalars().all()
        for value in values:
            _collect_storage_keys(value, keys)

    material_ids = select(Materials.id).where(Materials.script_id == script_id)
    canvas_values = (
        await db.execute(
            select(CanvasNode.data).where(CanvasNode.material_id.in_(material_ids))
        )
    ).scalars().all()
    for value in canvas_values:
        _collect_storage_keys(value, keys)

    render_keys = (
        await db.execute(
            select(TimelineRenderJob.output_storage_key).where(
                TimelineRenderJob.script_id == script_id
            )
        )
    ).scalars().all()
    keys.update(key for key in render_keys if key)

    frame_values = (
        await db.execute(
            select(ShotGroup.reference_frames).where(ShotGroup.script_id == script_id)
        )
    ).scalars().all()
    for value in frame_values:
        _collect_storage_keys(value, keys)
    return keys, asset_ids_by_key


async def _collect_episode_storage_keys(
    db: AsyncSession,
    *,
    script_id: str,
    episode_id: str,
    deleted_subject_ids: set[str],
) -> tuple[set[str], dict[str, str]]:
    """收集删除单集时会失去业务引用的媒体对象。"""
    keys: set[str] = set()
    asset_ids_by_key: dict[str, str] = {}

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
            if storage_key:
                keys.add(storage_key)
                asset_ids_by_key[storage_key] = asset_id

    episode_key = (
        await db.execute(
            select(Episode.script_file_storage_key).where(
                Episode.script_id == script_id,
                Episode.episode_id == episode_id,
            )
        )
    ).scalar_one_or_none()
    if episode_key:
        keys.add(episode_key)

    for model, field in (
        (GenerationJob, GenerationJob.output_json),
        (VideoGenerationJob, VideoGenerationJob.output_json),
        (TimelineRenderJob, TimelineRenderJob.output_json),
    ):
        values = (
            await db.execute(select(field).where(model.episode_id == episode_id))
        ).scalars().all()
        for value in values:
            _collect_storage_keys(value, keys)

    render_keys = (
        await db.execute(
            select(TimelineRenderJob.output_storage_key).where(
                TimelineRenderJob.episode_id == episode_id
            )
        )
    ).scalars().all()
    keys.update(key for key in render_keys if key)

    frame_values = (
        await db.execute(
            select(ShotGroup.reference_frames).where(
                ShotGroup.episode_id == episode_id
            )
        )
    ).scalars().all()
    for value in frame_values:
        _collect_storage_keys(value, keys)
    return keys, asset_ids_by_key


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
            await _count(
                db,
                PipelineJob,
                PipelineJob.script_id == script_id,
                PipelineJob.status.in_(["pending", "ready", "retry", "running"]),
            ),
            await _count(
                db,
                StoryboardJob,
                StoryboardJob.script_id == script_id,
                StoryboardJob.status.in_(["pending", "ready", "retry", "running"]),
            ),
            await _count(
                db,
                GenerationJob,
                GenerationJob.script_id == script_id,
                GenerationJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
            ),
            await _count(
                db,
                VideoGenerationJob,
                VideoGenerationJob.script_id == script_id,
                VideoGenerationJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
            ),
            await _count(
                db,
                TimelineRenderJob,
                TimelineRenderJob.script_id == script_id,
                TimelineRenderJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
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


async def delete_script(db: AsyncSession, script: Script, *, commit: bool = True) -> None:
    """按外键拓扑硬删除剧本及派生数据。"""
    script_id = script.id
    material_ids = select(Materials.id).where(Materials.script_id == script_id)
    subject_ids = select(Subject.id).where(Subject.script_id == script_id)
    asset_ids = select(AssetEntry.id).where(AssetEntry.script_id == script_id)
    shot_group_ids = select(ShotGroup.shot_group_id).where(ShotGroup.script_id == script_id)

    storage_keys, asset_ids_by_key = await _collect_script_storage_keys(db, script_id)
    await _enqueue_storage_deletes(
        db,
        storage_keys,
        asset_ids_by_key=asset_ids_by_key,
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
    shot_group_ids = select(ShotGroup.shot_group_id).where(
        ShotGroup.episode_id == episode_id
    )
    job_count = sum(
        (
            await _count(db, PipelineJob, PipelineJob.episode_id == episode_id),
            await _count(db, StoryboardJob, StoryboardJob.episode_id == episode_id),
            await _count(db, GenerationJob, GenerationJob.episode_id == episode_id),
            await _count(db, TimelineRenderJob, TimelineRenderJob.episode_id == episode_id),
        )
    )
    active_job_count = sum(
        (
            await _count(
                db,
                PipelineJob,
                PipelineJob.episode_id == episode_id,
                PipelineJob.status.in_(["pending", "ready", "retry", "running"]),
            ),
            await _count(
                db,
                StoryboardJob,
                StoryboardJob.episode_id == episode_id,
                StoryboardJob.status.in_(["pending", "ready", "retry", "running"]),
            ),
            await _count(
                db,
                GenerationJob,
                GenerationJob.episode_id == episode_id,
                GenerationJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
            ),
            await _count(
                db,
                VideoGenerationJob,
                VideoGenerationJob.episode_id == episode_id,
                VideoGenerationJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
            ),
            await _count(
                db,
                TimelineRenderJob,
                TimelineRenderJob.episode_id == episode_id,
                TimelineRenderJob.status.in_(["pending", "ready", "retry", "starting", "processing"]),
            ),
        )
    )
    return LifecycleDependencySummary(
        subject_count=len(related_subject_ids),
        asset_count=(
            await _count(db, AssetEntry, AssetEntry.subject_id.in_(related_subject_ids))
            if related_subject_ids
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

    episode = await _load_episode_for_script(db, script_id, episode_id)
    subjects = list(
        (
            await db.execute(select(Subject).where(Subject.script_id == script_id))
        ).scalars().all()
    )
    related = [s for s in subjects if episode.episode_no in (s.episode_nos or [])]
    deleted_subject_ids = {
        subject.id
        for subject in related
        if not [
            no
            for no in (subject.episode_nos or [])
            if no != episode.episode_no
        ]
    }
    shot_group_ids = select(ShotGroup.shot_group_id).where(
        ShotGroup.episode_id == episode_id
    )

    storage_keys, asset_ids_by_key = await _collect_episode_storage_keys(
        db,
        script_id=script_id,
        episode_id=episode_id,
        deleted_subject_ids=deleted_subject_ids,
    )
    await _enqueue_storage_deletes(
        db,
        storage_keys,
        asset_ids_by_key=asset_ids_by_key,
    )

    for model, criterion in (
        (VideoTimelineArrangement, VideoTimelineArrangement.episode_id == episode_id),
        (TimelineRenderJob, TimelineRenderJob.episode_id == episode_id),
        (VideoGenerationJob, VideoGenerationJob.episode_id == episode_id),
        (StoryboardJob, StoryboardJob.episode_id == episode_id),
        (GenerationJob, GenerationJob.episode_id == episode_id),
        (Shot, Shot.shot_group_id.in_(shot_group_ids)),
        (ShotGroup, ShotGroup.episode_id == episode_id),
        (Scene, Scene.episode_id == episode_id),
        (PipelineJob, PipelineJob.episode_id == episode_id),
    ):
        await db.execute(delete(model).where(criterion))

    # 只有该分集引用的主体可安全删除；跨集主体保留资产，仅移除当前集号。
    for subject in related:
        remaining = [no for no in (subject.episode_nos or []) if no != episode.episode_no]
        if remaining:
            subject.episode_nos = remaining
        else:
            # 防御性清理没有 episode_id 的遗留图片任务。
            await db.execute(
                delete(GenerationJob).where(GenerationJob.subject_id == subject.id)
            )
            await asset_crud.delete_subject(db, subject)

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
