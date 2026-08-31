"""资产库数据访问层 — 主体/资产条目/候选/画布项目的 CRUD 操作。

纯数据访问，不含状态机与并发控制逻辑（见 app.services.review）。
"""

from datetime import UTC, datetime

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.errors import BadRequestError, ConflictError, ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.models.asset_entry import AssetEntry
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_upload import MediaUpload
from app.models.parent_subject import ParentSubject
from app.models.script import Script
from app.models.subject import Subject
from app.models.subject_change_suggestion import SubjectChangeSuggestion
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
from app.services.storage_references import (
    StorageReferenceExclusions,
    acquire_storage_reference_barrier,
    collect_storage_keys,
    decide_storage_key_deletes,
    enqueue_storage_deletes,
    normalize_storage_keys,
)
from app.services.subject_ordering import sort_scene_subjects
from app.services.task_dispatch import cancel_outbox_task

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
    items = sort_scene_subjects(list((await db.execute(items_q)).scalars().all()))
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
    return sort_scene_subjects(list((await db.execute(q)).scalars().all()))


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


async def get_subject_for_identity_update(
    db: AsyncSession,
    script_id: str,
    subject_id: str,
) -> Subject | None:
    """按 Script→Subject 固定锁序加载身份写入目标，避免并发重名与删除死锁。"""
    await db.execute(
        select(Script.id).where(Script.id == script_id).with_for_update()
    )
    return await get_subject_for_update(db, subject_id)


def _stored_subject_type(subject_type: str) -> str:
    """将 API 的 background 同义值归一为数据库使用的 scene。"""
    return "scene" if subject_type in {"scene", "background"} else subject_type


async def _validate_subject_episode_nos(
    db: AsyncSession,
    *,
    script_id: str,
    episode_nos: list[int],
) -> list[int]:
    """验证集号均为正整数、唯一且真实属于当前剧本。"""
    if not episode_nos:
        raise BadRequestError("主体必须至少属于一个分集")
    if any(type(episode_no) is not int or episode_no <= 0 for episode_no in episode_nos):
        raise BadRequestError("所属集号必须为正整数")
    if len(episode_nos) != len(set(episode_nos)):
        raise BadRequestError("所属集号不得重复")

    requested_episode_nos = sorted(episode_nos)
    episode_rows = await db.execute(
        select(Episode.episode_no).where(
            Episode.script_id == script_id,
            Episode.episode_no.in_(requested_episode_nos),
        )
    )
    existing_episode_nos = set(episode_rows.scalars().all())
    missing_episode_nos = [
        episode_no
        for episode_no in requested_episode_nos
        if episode_no not in existing_episode_nos
    ]
    if missing_episode_nos:
        missing = ", ".join(str(episode_no) for episode_no in missing_episode_nos)
        raise BadRequestError(f"所属集号不存在: {missing}")
    return requested_episode_nos


async def _ensure_subject_name_type_unique(
    db: AsyncSession,
    *,
    script_id: str,
    subject_type: str,
    name: str,
    pipeline_name: str | None = None,
    exclude_subject_id: str | None = None,
) -> None:
    """校验展示名称与稳定身份在同类主体中均唯一。"""
    stored_type = _stored_subject_type(subject_type)
    equivalent_types = {"scene", "background"} if stored_type == "scene" else {stored_type}
    identity_conditions = [Subject.name == name]
    if pipeline_name is not None:
        identity_conditions.append(Subject.pipeline_name == pipeline_name)
    query = select(Subject.id).where(
        Subject.script_id == script_id,
        Subject.type.in_(equivalent_types),
        or_(*identity_conditions),
    )
    if exclude_subject_id is not None:
        query = query.where(Subject.id != exclude_subject_id)
    duplicate = (await db.execute(query.limit(1))).scalar_one_or_none()
    if duplicate is not None:
        raise ConflictError("同剧本下已存在同名同类主体")


async def create_subject(
    db: AsyncSession,
    *,
    script_id: str,
    subject_type: str,
    name: str,
    description: str,
    episode_nos: list[int],
) -> Subject:
    """手动新增主体，并保证同一剧本内的同名同类唯一。

    对剧本行加锁使同一剧本的手动新增串行化；在锁内同时验证分集归属和
    重复主体，避免两个并发请求同时通过“先查后写”。
    """
    await db.execute(
        select(Script.id).where(Script.id == script_id).with_for_update()
    )

    requested_episode_nos = await _validate_subject_episode_nos(
        db,
        script_id=script_id,
        episode_nos=episode_nos,
    )
    stored_type = _stored_subject_type(subject_type)
    await _ensure_subject_name_type_unique(
        db,
        script_id=script_id,
        subject_type=stored_type,
        name=name,
    )

    subject = Subject(
        id=new_id(),
        script_id=script_id,
        type=stored_type,
        name=name,
        pipeline_name=name,
        description=description,
        subject_version=1,
        name_locked=True,
        description_locked=True,
        is_child=True,
        is_parent=False,
        parent_id=None,
        episode_nos=requested_episode_nos,
        is_active=True,
        review_status="not_designed",
    )
    db.add(subject)
    await db.flush()
    return subject


async def update_subject(
    db: AsyncSession,
    subject: Subject,
    data: dict,
    *,
    expected_subject_version: int | None = None,
) -> Subject:
    """部分更新主体；人工修改名称/描述时默认锁定并递增版本。"""
    current_version = int(subject.subject_version or 1)
    if (
        expected_subject_version is not None
        and current_version != expected_subject_version
    ):
        raise ConflictError("主体版本已变化，请刷新后重试")

    if "type" in data and data["type"] is not None:
        data["type"] = _stored_subject_type(data["type"])
    identity_changed = (
        "name" in data
        and data["name"] is not None
        and data["name"] != subject.name
    ) or (
        "type" in data
        and data["type"] is not None
        and data["type"] != subject.type
    )
    if identity_changed:
        prospective_name = data.get("name", subject.name)
        prospective_type = data.get("type", subject.type)
        await _ensure_subject_name_type_unique(
            db,
            script_id=subject.script_id,
            subject_type=prospective_type,
            name=prospective_name,
            pipeline_name=subject.pipeline_name,
            exclude_subject_id=subject.id,
        )
    if "episode_nos" in data and data["episode_nos"] is not None:
        data["episode_nos"] = await _validate_subject_episode_nos(
            db,
            script_id=subject.script_id,
            episode_nos=data["episode_nos"],
        )

    if "name" in data and data["name"] != subject.name:
        data.setdefault("name_locked", True)
    if "description" in data and data["description"] != subject.description:
        data.setdefault("description_locked", True)

    versioned_fields = {
        "name",
        "description",
        "type",
        "episode_nos",
        "name_locked",
        "description_locked",
    }
    should_bump_version = any(
        key in versioned_fields
        and hasattr(subject, key)
        and getattr(subject, key) != value
        for key, value in data.items()
    )
    for key, value in data.items():
        if hasattr(subject, key) and value is not None:
            setattr(subject, key, value)
    if should_bump_version:
        subject.subject_version = current_version + 1
    await db.flush()
    return subject


async def list_subject_suggestions(
    db: AsyncSession,
    script_id: str,
    subject_id: str,
    status: str | None = "pending",
) -> tuple[list[SubjectChangeSuggestion], int]:
    """列出主体差异建议，默认仅返回待处理项。"""
    conditions = [
        SubjectChangeSuggestion.script_id == script_id,
        SubjectChangeSuggestion.subject_id == subject_id,
    ]
    if status is not None:
        conditions.append(SubjectChangeSuggestion.status == status)
    result = await db.execute(
        select(SubjectChangeSuggestion)
        .where(*conditions)
        .order_by(SubjectChangeSuggestion.created_at.desc())
    )
    items = list(result.scalars().all())
    return items, len(items)


async def persist_subject_suggestions(
    db: AsyncSession,
    script_id: str,
    staged_subjects: list[dict],
    *,
    source_run_id: str | None,
) -> list[SubjectChangeSuggestion]:
    """幂等保存 merge 暂存结果中的锁字段差异建议。"""
    created: list[SubjectChangeSuggestion] = []
    for item in staged_subjects:
        subject_id = item.get("id")
        target_version = item.get("subject_version")
        if not isinstance(subject_id, str) or not isinstance(target_version, int):
            continue
        for suggestion in item.get("field_suggestions") or []:
            field = suggestion.get("field")
            if field not in {"name", "description"}:
                continue
            current_value = suggestion.get("current_value")
            suggested_value = suggestion.get("suggested_value")
            pending = list(
                (
                    await db.execute(
                        select(SubjectChangeSuggestion).where(
                            SubjectChangeSuggestion.subject_id == subject_id,
                            SubjectChangeSuggestion.field == field,
                            SubjectChangeSuggestion.status == "pending",
                        )
                    )
                )
                .scalars()
                .all()
            )
            duplicate = next(
                (
                    row
                    for row in pending
                    if row.base_subject_version == target_version
                ),
                None,
            )
            if duplicate is not None:
                continue
            resolved_at = datetime.now(UTC)
            for stale in pending:
                stale.status = "ignored"
                stale.resolved_at = resolved_at
            if pending:
                # 先释放部分唯一索引中的旧 pending 槽位，再插入新版本建议。
                await db.flush()
            row = SubjectChangeSuggestion(
                id=new_id(),
                script_id=script_id,
                subject_id=subject_id,
                field=field,
                current_value=current_value,
                suggested_value=suggested_value,
                base_subject_version=target_version,
                source_run_id=source_run_id,
                status="pending",
            )
            db.add(row)
            created.append(row)
    await db.flush()
    return created


async def resolve_subject_suggestion(
    db: AsyncSession,
    subject: Subject,
    suggestion_id: str,
    *,
    action: str,
    expected_subject_version: int,
    actor_user_id: str,
) -> SubjectChangeSuggestion:
    """行锁串行化接受/忽略，接受时以主体版本栅栏防止覆盖新人工值。"""
    result = await db.execute(
        select(SubjectChangeSuggestion)
        .where(SubjectChangeSuggestion.id == suggestion_id)
        .with_for_update()
    )
    suggestion = result.scalar_one_or_none()
    if suggestion is None:
        raise NotFoundError("差异建议不存在")
    if (
        suggestion.script_id != subject.script_id
        or suggestion.subject_id != subject.id
    ):
        raise ForbiddenError("差异建议不属于当前主体")
    if suggestion.status != "pending":
        raise ConflictError("差异建议已处理")

    current_version = int(subject.subject_version or 1)
    if current_version != expected_subject_version:
        raise ConflictError("主体版本已变化，请刷新后重试")
    if action == "accept":
        if suggestion.base_subject_version != current_version:
            raise ConflictError("差异建议已过期，请忽略后等待新建议")
        if suggestion.field == "name":
            if not suggestion.suggested_value:
                raise ConflictError("主体名称建议不能为空")
            await _ensure_subject_name_type_unique(
                db,
                script_id=subject.script_id,
                subject_type=subject.type,
                name=suggestion.suggested_value,
                pipeline_name=subject.pipeline_name,
                exclude_subject_id=subject.id,
            )
            subject.name = suggestion.suggested_value
            subject.name_locked = True
        elif suggestion.field == "description":
            subject.description = suggestion.suggested_value
            subject.description_locked = True
        else:
            raise ConflictError("差异建议字段无效")
        subject.subject_version = current_version + 1
        suggestion.status = "accepted"
    elif action == "ignore":
        suggestion.status = "ignored"
    else:
        raise ConflictError("差异建议操作无效")
    suggestion.resolved_by_user_id = actor_user_id
    suggestion.resolved_at = datetime.now(UTC)
    await db.flush()
    return suggestion


async def delete_subject(
    db: AsyncSession,
    subject: Subject,
    *,
    protected_storage_keys: set[str] | None = None,
    parent_delete_prepared: bool = False,
) -> None:
    """按即时外键顺序删除主体、资产和父主体关联。"""
    await acquire_storage_reference_barrier(db)
    entries_q = select(AssetEntry).where(AssetEntry.subject_id == subject.id)
    entries = list((await db.execute(entries_q)).scalars().all())
    entry_ids = [entry.id for entry in entries]
    external_refs: list[AssetEntry] = []
    if entry_ids:
        external_refs = list(
            (
                await db.execute(
                    select(AssetEntry)
                    .where(
                        AssetEntry.superseded_by_id.in_(entry_ids),
                        AssetEntry.subject_id != subject.id,
                    )
                )
            ).scalars().all()
        )

    generation_jobs: list[GenerationJob] = []
    if not parent_delete_prepared:
        generation_jobs = list(
            (
                await db.execute(
                    select(GenerationJob)
                    .where(GenerationJob.subject_id == subject.id)
                    .order_by(GenerationJob.job_id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
        active_count = sum(
            job.status in {"starting", "processing"} for job in generation_jobs
        )
        if active_count:
            raise ConflictError(
                f"仍有 {active_count} 个活动图片生成任务，"
                "请先取消或等待任务进入终态后再删除"
            )
        financial_debt_count = sum(
            job.status == "failed"
            and job.error_type == "financial_cleanup"
            and job.retryable is True
            for job in generation_jobs
        )
        if financial_debt_count:
            raise ConflictError(
                f"仍有 {financial_debt_count} 个图片生成任务存在可重试的财务清理债务，"
                "请等待后台清理完成后再删除"
            )
        for job in generation_jobs:
            await cancel_outbox_task(
                db,
                task_type="generation.generate_images",
                job_id=job.job_id,
                attempt=job.attempt,
            )

    if not parent_delete_prepared:
        uploads = list(
            (
                await db.execute(
                    select(MediaUpload)
                    .where(MediaUpload.subject_id == subject.id)
                    .order_by(MediaUpload.id.asc())
                    .with_for_update()
                )
            ).scalars().all()
        )
        candidate_keys: set[str] = set()
        asset_ids_by_key: dict[str, str] = {}
        upload_ids_by_key: dict[str, str] = {}
        for entry in entries:
            collect_storage_keys(entry.storage_key, candidate_keys)
            asset_ids_by_key[entry.storage_key] = entry.id
        for entry in external_refs:
            collect_storage_keys(entry.storage_key, candidate_keys)
            asset_ids_by_key[entry.storage_key] = entry.id
        for job in generation_jobs:
            collect_storage_keys(job.output_json, candidate_keys)
            collect_storage_keys(job.metrics, candidate_keys)
        for upload in uploads:
            collect_storage_keys(upload.temp_storage_key, candidate_keys)
            collect_storage_keys(upload.final_storage_key, candidate_keys)
            upload_ids_by_key[upload.temp_storage_key] = upload.id
            upload_ids_by_key[upload.final_storage_key] = upload.id

        exclusions = StorageReferenceExclusions(
            asset_entry_ids={
                entry.id
                for entry in (*entries, *external_refs)
            },
            media_upload_ids={upload.id for upload in uploads},
            generation_job_ids={job.job_id for job in generation_jobs},
        )
        decision = await decide_storage_key_deletes(
            db,
            candidate_keys,
            exclusions=exclusions,
        )
        protected_keys = set(decision.protected_keys)
        protected_keys.update(
            normalize_storage_keys(protected_storage_keys or set())
        )
        deletion_keys = set(decision.candidate_keys - protected_keys)
        now = datetime.now(UTC)
        for upload in uploads:
            final_key = next(
                iter(normalize_storage_keys({upload.final_storage_key})),
                "",
            )
            temp_key = next(
                iter(normalize_storage_keys({upload.temp_storage_key})),
                "",
            )
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

    # Subject.image_entry_id 和 AssetEntry.superseded_by_id 都会阻断条目删除。
    # parent_id 指向 ParentSubject，不是 Subject 自引用，不能误清理其他主体。
    parent = await db.get(ParentSubject, subject.parent_id) if subject.parent_id else None
    subject.image_entry_id = None
    for entry in entries:
        entry.superseded_by_id = None
    for entry in external_refs:
        entry.set_status("deleted")
    await db.flush()

    for entry in entries:
        await db.delete(entry)
    await db.flush()

    for job in generation_jobs:
        await db.delete(job)
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
    episode_map, _ = await get_episode_context_maps(db, script_id)
    return episode_map


async def get_episode_context_maps(
    db: AsyncSession,
    script_id: str,
) -> tuple[dict[int, str], dict[int, dict[str, object]]]:
    """返回主体序列化所需的分集映射，保留空上下文映射以兼容既有调用方。"""
    rows = (
        await db.execute(
            select(
                Episode.episode_no,
                Episode.episode_id,
            ).where(Episode.script_id == script_id)
        )
    ).all()
    episode_map = {int(episode_no): episode_id for episode_no, episode_id in rows}
    return episode_map, {}


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

    norm_type = normalize_subject_type(subject.type)

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
        name_locked=bool(getattr(subject, "name_locked", False)),
        description_locked=bool(getattr(subject, "description_locked", False)),
        subject_version=int(getattr(subject, "subject_version", 1) or 1),
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
