"""剧本/分集/主体数据访问层。

- list_scripts: 支持 scope personal/team，团队项目带 current_user_project_role
- save_episode: 带乐观锁（episode_version + client_save_seq），冲突 409
- get_breakdown: 返回 {episode, subjects, shots, shot_groups}
"""

from typing import Any, cast

from fastapi import HTTPException
from sqlalchemy import false, func, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, ForbiddenError
from app.core.ids import new_id
from app.models.episode import Episode
from app.models.parent_subject import ParentSubject
from app.models.project_member import ProjectMember
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.user import User

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
    await db.commit()
    await db.refresh(script)
    return script


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
    *,
    commit: bool = True,
) -> Episode:
    """带乐观锁的分集保存。

    WHERE episode_version=? AND client_save_seq=?，affected==0 则 409。
    成功后 episode_version 与 client_save_seq 均递增。
    """
    await _load_episode_for_script(db, script_id, episode_id)

    # 原子更新，避免并发覆盖
    from sqlalchemy import update

    values: dict = {
        "episode_version": episode_version + 1,
        "client_save_seq": client_save_seq + 1,
    }
    if script_text is not None:
        values["script_text"] = script_text
    if title is not None:
        values["title"] = title

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
    result = cast(CursorResult[Any], await db.execute(stmt))
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
