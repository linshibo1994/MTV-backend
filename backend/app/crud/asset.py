"""资产库数据访问层 — 主体/资产条目/候选/画布项目的 CRUD 操作。

纯数据访问，不含状态机与并发控制逻辑（见 app.services.review）。
"""

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.errors import ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.materials import Materials
from app.models.parent_subject import ParentSubject
from app.models.script import Script
from app.models.subject import Subject
from app.schemas.asset import (
    AssetEntryOut,
    CandidateOut,
    ParentSubjectOut,
    SubjectAssetOut,
    normalize_subject_type,
    review_status_label,
    subject_type_label,
)
from app.services.cover import generate_svg_cover

# =====================================================================
# Script 辅助
# =====================================================================


async def get_script(db: AsyncSession, script_id: str) -> Script | None:
    """按 ID 查剧本。"""
    result = await db.execute(select(Script).where(Script.id == script_id))
    return result.scalar_one_or_none()


# =====================================================================
# 主体列表 / 父主体
# =====================================================================


async def list_subjects(
    db: AsyncSession, script_id: str, limit: int = 30, offset: int = 0
) -> tuple[list[Subject], int]:
    """主体列表（含 total）。

    Subject 模型的 asset_entries / image_entry 已配置 lazy="selectin"，
    查询时自动急加载；parent_subject 为默认 lazy，此处显式 selectinload。
    """
    count_q = select(func.count()).select_from(Subject).where(
        Subject.script_id == script_id
    )
    total = (await db.execute(count_q)).scalar_one()

    items_q = (
        select(Subject)
        .options(selectinload(Subject.parent_subject))
        .where(Subject.script_id == script_id)
        # 按父主体分组排序（02§3.2.2 主体表格行按父主体分组）：
        # parent_id NULLS LAST 让有父的子形态按父聚集，同父内按创建时间，
        # 降低前端拼接成本。父主体（parent_id 为 NULL）排末尾。
        .order_by(
            Subject.parent_id.asc().nullslast(),
            Subject.created_at.asc(),
        )
        .limit(limit)
        .offset(offset)
    )
    items = list((await db.execute(items_q)).scalars().all())
    return items, int(total)


async def list_parent_subjects(
    db: AsyncSession, script_id: str
) -> list[ParentSubject]:
    """父主体列表。"""
    q = (
        select(ParentSubject)
        .where(ParentSubject.script_id == script_id)
        .order_by(ParentSubject.created_at.asc())
    )
    return list((await db.execute(q)).scalars().all())


# =====================================================================
# 主体单条 / 更新 / 删除
# =====================================================================


async def get_subject(db: AsyncSession, subject_id: str) -> Subject | None:
    """按 ID 查主体（急加载 parent + asset_entries + image_entry）。"""
    q = (
        select(Subject)
        .options(selectinload(Subject.parent_subject))
        .where(Subject.id == subject_id)
    )
    return (await db.execute(q)).scalar_one_or_none()


async def get_subject_for_update(
    db: AsyncSession, subject_id: str
) -> Subject | None:
    """行级锁查主体 — SELECT ... FOR UPDATE，防两审核者并发覆盖。"""
    q = (
        select(Subject)
        .options(selectinload(Subject.parent_subject))
        .where(Subject.id == subject_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return (await db.execute(q)).scalar_one_or_none()


async def update_subject(db: AsyncSession, subject: Subject, data: dict) -> Subject:
    """部分更新主体字段。"""
    versioned_fields = {"name", "description", "type", "episode_nos"}
    should_bump_version = any(
        key in versioned_fields
        and value is not None
        and hasattr(subject, key)
        and getattr(subject, key) != value
        for key, value in data.items()
    )
    for key, value in data.items():
        if value is not None and hasattr(subject, key):
            setattr(subject, key, value)
    if should_bump_version:
        subject.subject_version = int(subject.subject_version or 1) + 1
    await db.flush()
    return subject


async def delete_subject(db: AsyncSession, subject: Subject) -> None:
    """按即时外键顺序删除主体、资产和父主体关联。"""
    entries_q = select(AssetEntry).where(AssetEntry.subject_id == subject.id)
    entries = list((await db.execute(entries_q)).scalars().all())

    # Subject.image_entry_id 和 AssetEntry.superseded_by_id 都会阻断条目删除。
    # parent_id 指向 ParentSubject，不是 Subject 自引用，不能误清理其他主体。
    parent = await db.get(ParentSubject, subject.parent_id) if subject.parent_id else None
    entry_ids = [entry.id for entry in entries]
    subject.image_entry_id = None
    for entry in entries:
        entry.superseded_by_id = None
    if entry_ids:
        external_refs = list(
            (
                await db.execute(
                    select(AssetEntry).where(
                        AssetEntry.superseded_by_id.in_(entry_ids),
                        AssetEntry.subject_id != subject.id,
                    )
                )
            ).scalars().all()
        )
        for entry in external_refs:
            entry.set_status("deleted")
    await db.flush()

    for entry in entries:
        await db.delete(entry)
    await db.flush()

    if parent is not None:
        parent.child_count = max(0, parent.child_count - 1)
    await db.delete(subject)
    await db.flush()


# =====================================================================
# 候选 / 资产条目
# =====================================================================


async def list_candidates(
    db: AsyncSession, subject_id: str
) -> tuple[list[AssetEntry], list[AssetEntry]]:
    """返回 (image_candidates, audio_candidates) — 未通过审核的候选。"""
    q = (
        select(AssetEntry)
        .join(Subject, AssetEntry.subject_id == Subject.id)
        .where(
            AssetEntry.subject_id == subject_id,
            AssetEntry.script_id == Subject.script_id,
            AssetEntry.status == "candidate",
        )
        .order_by(AssetEntry.created_at.asc())
    )
    entries = list((await db.execute(q)).scalars().all())
    images = [e for e in entries if e.kind == "image"]
    audios = [e for e in entries if e.kind == "audio"]
    return images, audios


async def get_candidate(
    db: AsyncSession, candidate_id: str
) -> AssetEntry | None:
    """按 ID 查资产条目（候选）。"""
    q = select(AssetEntry).where(AssetEntry.id == candidate_id)
    return (await db.execute(q)).scalar_one_or_none()


async def add_candidate(
    db: AsyncSession,
    script_id: str,
    subject_id: str,
    kind: str,
    storage_key: str,
    mime_type: str,
    size_bytes: int,
    source: str,
    sha256: str | None = None,
    subject: Subject | None = None,
) -> AssetEntry:
    """新增候选资产条目，并推进图片主体的首候选审核状态。"""
    if subject is None:
        subject = await get_subject(db, subject_id)
    if subject is not None:
        if subject.id != subject_id or subject.script_id != script_id:
            raise ForbiddenError("主体不属于当前剧本")
    else:
        raise NotFoundError("主体不存在")
    entry = AssetEntry(
        id=new_id(),
        script_id=script_id,
        subject_id=subject_id,
        kind=kind,
        storage_key=storage_key,
        mime_type=mime_type,
        size_bytes=size_bytes,
        source=source,
        sha256=sha256,
        status="candidate",
        is_approved=False,
        superseded=False,
    )
    db.add(entry)
    if kind == "image" and subject.review_status == "not_designed":
        subject.review_status = "pending"
    if (
        kind == "audio"
        and getattr(subject, "type", None) == "sound"
        and subject.review_status == "not_designed"
    ):
        subject.review_status = "pending"
    await db.flush()
    return entry


async def approve_image(
    db: AsyncSession, subject: Subject, candidate: AssetEntry
) -> None:
    """设候选为 image_entry，review_status=approved。

    替换已有正式图片时，将旧 image_entry 标记为 superseded（AssetEntry 维度），
    不删除、不取消 is_approved（保留为历史版本），仅切换 image_entry_id 指向新候选。
    """
    old_entry_id = subject.image_entry_id
    if old_entry_id is not None and old_entry_id != candidate.id:
        # subject.image_entry 经 selectin 已急加载
        old_entry = subject.image_entry
        if old_entry is not None:
            old_entry.set_status("superseded", superseded_by_id=candidate.id)
    subject.image_entry_id = candidate.id
    candidate.set_status("approved")
    subject.review_status = "approved"
    await db.flush()


async def approve_audio(
    db: AsyncSession, subject: Subject, candidate: AssetEntry
) -> None:
    """挂载 audio_entries — 设候选 is_approved=True 即进入 audio_entries。"""
    candidate.set_status("approved")
    await db.flush()


async def remove_candidate(
    db: AsyncSession, candidate: AssetEntry
) -> None:
    """移出候选队列 — 将该候选标记为 deleted，保留生命周期记录。"""
    candidate.set_status("deleted")
    await db.flush()


async def count_entries(db: AsyncSession, script_id: str) -> int:
    """资产条目总数（列表页取计数）。"""
    q = select(func.count()).select_from(AssetEntry).where(
        AssetEntry.script_id == script_id
    )
    return int((await db.execute(q)).scalar_one())


async def list_entries(
    db: AsyncSession, script_id: str, limit: int = 30, offset: int = 0
) -> tuple[list[AssetEntry], int]:
    """资产条目列表（含 total）。"""
    total = await count_entries(db, script_id)
    q = (
        select(AssetEntry)
        .where(AssetEntry.script_id == script_id)
        .order_by(AssetEntry.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    items = list((await db.execute(q)).scalars().all())
    return items, total


# =====================================================================
# 画布项目 ensure
# =====================================================================


async def ensure_materials(
    db: AsyncSession, script: Script
) -> tuple[Materials, bool]:
    """幂等创建/返回画布项目 — 1 剧本 1 materials。返回 (materials, created)。"""
    q = select(Materials).where(Materials.script_id == script.id)
    existing = (await db.execute(q)).scalar_one_or_none()
    if existing is not None:
        return existing, False
    materials = Materials(
        id=new_id(),
        name=script.title,
        script_id=script.id,
    )
    try:
        # 唯一冲突只应回滚这次插入，不能丢弃调用方已完成的无关写入。
        async with db.begin_nested():
            db.add(materials)
            await db.flush()
    except IntegrityError:
        # 唯一约束是并发正确性的最终裁判；保存点回滚后重查胜者。
        existing = (await db.execute(q)).scalar_one_or_none()
        if existing is None:
            raise
        return existing, False
    return materials, True


async def get_episode_id_map(
    db: AsyncSession, script_id: str
) -> dict[int, str]:
    """返回 {episode_no: episode_id} 映射，供主体序列化填充 episode_ids。

    契约 02§3.2.3 / 04§1.7 要求 SubjectAssetOut 含 episodes/episode_ids/episode_nos
    三字段；episode_nos 由主体 JSONB 直接给出，episode_ids 需按集号反查分集表。
    """
    q = select(Episode).where(Episode.script_id == script_id)
    rows = list((await db.execute(q)).scalars().all())
    return {r.episode_no: r.episode_id for r in rows}


# =====================================================================
# 序列化辅助 — 从 ORM 对象构建出参 schema
# =====================================================================


def _entry_to_out(entry: AssetEntry | None) -> AssetEntryOut | None:
    if entry is None:
        return None
    return serialize_entry(entry)


def _entry_status(entry: AssetEntry) -> str:
    """读取生命周期；兼容新增 status 前的旧 ORM 行。"""
    status = getattr(entry, "status", None)
    if status:
        return status
    if getattr(entry, "superseded", False):
        return "superseded"
    return "approved" if entry.is_approved else "candidate"


def serialize_entry(entry: AssetEntry) -> AssetEntryOut:
    """从 ORM AssetEntry 构建出参（非可选版本）。"""
    return AssetEntryOut(
        asset_id=entry.id,
        storage_key=entry.storage_key,
        mime_type=entry.mime_type,
        size_bytes=entry.size_bytes,
        source=entry.source,
        sha256=entry.sha256,
        kind=entry.kind,
        status=_entry_status(entry),
        is_approved=entry.is_approved,
        superseded=getattr(entry, "superseded", False),
        superseded_by_id=getattr(entry, "superseded_by_id", None),
        created_at=entry.created_at,
        updated_at=entry.updated_at,
    )


def _entry_to_candidate(entry: AssetEntry) -> CandidateOut:
    return CandidateOut(
        candidate_id=entry.id,
        storage_key=entry.storage_key,
        mime_type=entry.mime_type,
        size_bytes=entry.size_bytes,
        source=entry.source,
        sha256=entry.sha256,
        kind=entry.kind,
        status="pending",
    )


def serialize_subject(
    subject: Subject,
    episode_id_map: dict[int, str] | None = None,
) -> SubjectAssetOut:
    """从 ORM Subject 构建出参（需已急加载 asset_entries / image_entry / parent_subject）。

    asset_entries 包含该主体所有资产条目（图片+声音，已通过+未通过），
    据此计算 pending_candidate_count / image_entry / audio_entries。

    episode_id_map: {episode_no: episode_id} 映射，由调用方预查后传入，
    用于填充 episodes/episode_ids 兼容字段（契约 02§3.2.3）。可不传。
    """
    all_entries: list[AssetEntry] = list(subject.asset_entries or [])

    # image_entry 优先取 subject.image_entry_id 关联对象
    image_entry_obj = subject.image_entry  # selectin 已加载
    if image_entry_obj is None and subject.image_entry_id:
        # 兜底：从 asset_entries 中查找
        image_entry_obj = next(
            (e for e in all_entries if e.id == subject.image_entry_id), None
        )

    # 待审候选数 = 未通过图片条目数
    pending_count = sum(
        1 for e in all_entries if e.kind == "image" and _entry_status(e) == "candidate"
    )

    # 已通过声音条目
    audio_entries = [
        e for e in all_entries if e.kind == "audio" and _entry_status(e) == "approved"
    ]

    raw_type = subject.type
    norm_type = normalize_subject_type(raw_type)

    parent_name: str | None = None
    if subject.parent_subject is not None:
        parent_name = subject.parent_subject.name

    episode_nos = list(subject.episode_nos or [])
    # episodes: 集号字符串数组（兼容层冗余字段，语义=出现的集号）
    episodes = [str(n) for n in episode_nos]
    # episode_ids: 按 episode_no 反查的分集 ID 数组
    episode_ids: list[str] = []
    if episode_id_map:
        episode_ids = [episode_id_map[n] for n in episode_nos if n in episode_id_map]

    return SubjectAssetOut(
        subject_id=subject.id,
        subject_name=subject.name,
        subject_type=norm_type,
        subject_type_label=subject_type_label(norm_type),
        subject_description=subject.description,
        episodes=episodes,
        episode_ids=episode_ids,
        episode_nos=episode_nos,
        parent_id=subject.parent_id,
        parent_name=parent_name,
        is_parent=subject.is_parent,
        review_status=subject.review_status,
        review_status_label=review_status_label(subject.review_status),
        pending_candidate_count=pending_count,
        image_entry=_entry_to_out(image_entry_obj),
        # T10.3 资产库列表封面 — 无正式图片时用确定性 SVG 占位图兜底（02§3.2.2）
        cover_url=(
            None
            if image_entry_obj is not None
            else generate_svg_cover(subject.name)
        ),
        audio_entries=[serialize_entry(e) for e in audio_entries],
        whitelist_status=subject.whitelist_status,
        created_at=subject.created_at,
        updated_at=subject.updated_at,
    )


def serialize_parent_subject(ps: ParentSubject) -> ParentSubjectOut:
    """从 ORM ParentSubject 构建出参。"""
    return ParentSubjectOut(
        subject_id=ps.id,
        name=ps.name,
        type=normalize_subject_type(ps.type),
        description=ps.description,
        child_count=ps.child_count,
        created_at=ps.created_at,
        updated_at=ps.updated_at,
    )
