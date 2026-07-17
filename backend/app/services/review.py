"""资产审核服务 — 状态机 + 并发控制 + 替换影响检查。

审核状态机: not_designed -> pending -> approved
- not_designed: 未设计/无候选
- pending: 有候选待审
- approved: 已通过图片（image_entry 已设置）

并发控制: 两审核者同时操作同一主体时，后提交者通过 SELECT FOR UPDATE
防覆盖，确保不覆盖新正式资产。

替换被引用资产前检查 shot_group.materials 引用并返回影响摘要。
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.crud import asset as asset_crud
from app.models.asset_entry import AssetEntry
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.schemas.asset import (
    ImpactDetailOut,
    ImpactSummaryOut,
    SubjectAssetOut,
)
from app.schemas.materials import build_subject_prompt as _build_subject_prompt

# ---- 状态机常量 ----

STATUS_NOT_DESIGNED = "not_designed"
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"

# materials JSONB 各类别键
_MATERIALS_CATEGORIES = ("character", "background", "prop", "other")


def build_subject_prompt(
    subject_name: str, subject_type: str, subject_description: str
) -> str:
    """通过审核服务暴露统一 prompt helper。"""
    return _build_subject_prompt(subject_name, subject_type, subject_description)


def material_references_subject(
    materials: dict | None,
    *,
    subject_id: str | None,
    asset_entry_id: str | None,
    subject_name: str | None,
) -> tuple[bool, str | None]:
    """按稳定主体/资产 ID 判断材料引用，兼容旧的名称字符串格式。"""
    if not materials:
        return False, None
    for category in _MATERIALS_CATEGORIES:
        for item in materials.get(category, []) or []:
            if isinstance(item, dict):
                has_stable_id = "subject_id" in item or "asset_entry_id" in item
                subject_matches = (
                    not subject_id
                    or "subject_id" not in item
                    or item.get("subject_id") == subject_id
                )
                asset_matches = (
                    not asset_entry_id
                    or "asset_entry_id" not in item
                    or item.get("asset_entry_id") == asset_entry_id
                )
                if subject_matches and asset_matches and (
                    item.get("subject_id") == subject_id
                    or item.get("asset_entry_id") == asset_entry_id
                ):
                    return True, category
                if has_stable_id:
                    continue
                if subject_name and item.get("name") == subject_name:
                    return True, category
            elif subject_name and item == subject_name:
                return True, category
    return False, None


def _audio_names(audio_elements) -> list[str]:
    """解析镜头组音频引用中的主体名，兼容列表和分组字典。"""
    if not audio_elements:
        return []
    if isinstance(audio_elements, dict):
        merged: list = []
        for key in ("character", "characters", "names", "roles", "sounds", "sound"):
            values = audio_elements.get(key)
            if isinstance(values, list):
                merged.extend(values)
        audio_elements = merged
    if not isinstance(audio_elements, list):
        return []
    names: list[str] = []
    seen: set[str] = set()
    for item in audio_elements:
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict):
            name = str(item.get("name") or item.get("character") or item.get("role") or "")
        else:
            name = ""
        name = name.strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _candidate_belongs_to_subject(subject: Subject, candidate: AssetEntry) -> bool:
    return candidate.subject_id == subject.id and candidate.script_id == subject.script_id


async def _episode_map(db: AsyncSession, script_id: str | None) -> dict[int, str]:
    """加载 {episode_no: episode_id} 映射，供主体序列化填充 episode_ids。"""
    if not script_id:
        return {}
    return await asset_crud.get_episode_id_map(db, script_id)


def _validate_approve_image(subject: Subject, candidate: AssetEntry) -> None:
    """校验主体当前状态是否允许通过图片。

    状态机（08§2.3）:
    - not_designed: 仅在存在候选时可视为 pending，允许通过（首次设为 image_entry）
    - pending: 允许通过
    - approved: 允许通过新版本（旧 image_entry 转 superseded）

    并发冲突（AC-AL04）由 approve_image_with_lock 经 expected_image_entry_id
    乐观锁信号检测，此处仅做状态合法性校验。
    """
    status = subject.review_status
    if status == STATUS_NOT_DESIGNED:
        # not_designed 同时承载"未设计/未通过"，存在候选即可通过
        return
    if status == STATUS_PENDING:
        return
    if status == STATUS_APPROVED:
        # 通过新版本替换旧正式资产（supersede）
        return
    raise ConflictError(f"当前审核状态 {status} 不允许通过图片")


def _validate_remove_candidate(subject: Subject, is_approved_asset: bool) -> None:
    """校验移出候选队列的合法性。

    已通过的正式资产（image_entry 或挂载的 audio_entry）不允许通过
    "移出候选队列"删除（02§3.3.2 该按钮仅对 pending 候选提供）。
    图片需先通过新版本替换；声音需走专用取消审核流程。
    """
    if is_approved_asset:
        raise ConflictError("已通过的正式资产不可移出候选队列，请先通过新版本替换或取消审核")


# =====================================================================
# 影响检查 — 搜索引用该主体的镜头组
# =====================================================================


async def get_replace_impact(
    db: AsyncSession, subject_id: str
) -> ImpactSummaryOut:
    """替换影响摘要预检查（AC-AL05）— 公开供 API 端点调用。

    读取主体当前 image_entry_id（不加锁，仅预检查），若主体已有正式图片，
    搜索引用该主体名的镜头组，返回影响摘要供前端展示确认。
    无正式图片或无引用时返回 referenced=False。
    """
    subject = await asset_crud.get_subject(db, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.image_entry_id is None and subject.type != "sound":
        return ImpactSummaryOut(referenced=False, shot_group_count=0, details=[])
    if subject.type == "sound":
        return await search_shot_groups_referencing_audio_subject(
            db,
            subject.script_id,
            subject.name,
        )
    return await search_shot_groups_referencing_subject(
        db,
        subject.script_id,
        subject.name,
        subject_id=subject.id,
        asset_entry_id=subject.image_entry_id,
    )


async def search_shot_groups_referencing_audio_subject(
    db: AsyncSession,
    script_id: str,
    subject_name: str,
) -> ImpactSummaryOut:
    """搜索 shot_groups.audio_elements 中引用的声音主体。"""
    if not subject_name:
        return ImpactSummaryOut(referenced=False, shot_group_count=0, details=[])
    q = select(ShotGroup).where(ShotGroup.script_id == script_id).order_by(ShotGroup.scene_no.asc())
    rows = list((await db.execute(q)).scalars().all())

    details: list[ImpactDetailOut] = []
    for sg in rows:
        if subject_name not in _audio_names(sg.audio_elements):
            continue
        details.append(
            ImpactDetailOut(
                shot_group_id=sg.shot_group_id,
                scene_no=sg.scene_no,
                group_no=sg.group_no,
                episode_id=sg.episode_id,
                materials_category="audio",
            )
        )
    return ImpactSummaryOut(
        referenced=bool(details),
        shot_group_count=len(details),
        details=details,
    )


async def search_shot_groups_referencing_subject(
    db: AsyncSession,
    script_id: str,
    subject_name: str,
    *,
    subject_id: str | None = None,
    asset_entry_id: str | None = None,
) -> ImpactSummaryOut:
    """搜索 shot_groups.materials JSONB 中引用该主体名的镜头组。

    materials 结构: {character: [...], background: [...], prop: [...], other: [...]}
    各数组元素为主体名（字符串）。搜索所有类别中是否包含该主体名。

    返回影响摘要，供替换正式资产前展示。
    """
    if not subject_name and not subject_id and not asset_entry_id:
        return ImpactSummaryOut(referenced=False, shot_group_count=0, details=[])
    q = select(ShotGroup).where(ShotGroup.script_id == script_id).order_by(ShotGroup.scene_no.asc())
    rows = list((await db.execute(q)).scalars().all())

    details: list[ImpactDetailOut] = []
    for sg in rows:
        referenced, category_found = material_references_subject(
            sg.materials,
            subject_id=subject_id,
            asset_entry_id=asset_entry_id,
            subject_name=subject_name,
        )
        if not referenced:
            continue
        details.append(
            ImpactDetailOut(
                shot_group_id=sg.shot_group_id,
                scene_no=sg.scene_no,
                group_no=sg.group_no,
                episode_id=sg.episode_id,
                materials_category=category_found or "other",
            )
        )

    return ImpactSummaryOut(
        referenced=len(details) > 0,
        shot_group_count=len(details),
        details=details,
    )


# =====================================================================
# 审核操作 — 带 FOR UPDATE 并发控制
# =====================================================================


async def approve_image_with_lock(
    db: AsyncSession,
    subject_id: str,
    candidate_id: str,
    *,
    expected_image_entry_id: str | None = None,
    confirm: bool = False,
) -> tuple[SubjectAssetOut, ImpactSummaryOut | None]:
    """通过图片 — 设为 image_entry，review_status=approved。

    流程:
    1. SELECT FOR UPDATE 锁定主体行，防两审核者并发覆盖
    2. 校验候选归属与类型、状态机合法性
    3. 乐观锁冲突检测（AC-AL04）：若 expected_image_entry_id 与锁定后的
       image_entry_id 不符，说明另一审核者已先通过新正式资产，raise 409
    4. 若替换已有 image_entry，检查 shot_groups.materials 引用并返回影响摘要
    5. AC-AL05 两步确认：替换被引用的正式资产时，confirm=False 仅返回影响摘要
       不执行（dry-run），confirm=True 才执行替换
    6. 设 image_entry_id + is_approved + review_status=approved，旧正式资产标 superseded
    7. 提交事务
    """
    # 1. 行级锁
    subject = await asset_crud.get_subject_for_update(db, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")

    candidate = await asset_crud.get_candidate(db, candidate_id)
    if candidate is None:
        raise NotFoundError("候选资产不存在")

    # 2. 校验
    if not _candidate_belongs_to_subject(subject, candidate):
        raise ForbiddenError("候选资产不属于该主体")
    if candidate.kind != "image":
        raise ConflictError("该候选不是图片类型，无法通过为 image_entry")
    if candidate.status != "candidate":
        await db.rollback()
        raise ConflictError("只有候选状态的图片可以批准")

    # 首次审批也由服务端当前状态裁决。行锁使并发请求按顺序观察该状态：
    # 第一请求提交后，第二请求无需客户端令牌也会看到正式图并返回 409。
    if expected_image_entry_id is None and subject.image_entry_id is not None:
        await db.rollback()
        raise ConflictError("已有正式资产，请刷新后重试")

    # 3. 乐观锁并发冲突检测（AC-AL04）
    #    客户端传入 expected_image_entry_id（其当前看到的正式图片 ID）；
    #    若锁定后 image_entry_id 已被并发改为他人候选，拒绝覆盖。
    if (
        expected_image_entry_id is not None
        and subject.image_entry_id is not None
        and subject.image_entry_id != expected_image_entry_id
    ):
        await db.rollback()
        raise ConflictError("已有新正式资产，请刷新后重试")

    _validate_approve_image(subject, candidate)

    # 4. 替换影响检查
    impact: ImpactSummaryOut | None = None
    is_replace = (
        subject.image_entry_id is not None
        and subject.image_entry_id != candidate.id
    )
    if is_replace:
        # 替换已有正式图片 — 搜索引用该主体的镜头组
        impact = await search_shot_groups_referencing_subject(
            db,
            subject.script_id,
            subject.name,
            subject_id=subject.id,
            asset_entry_id=subject.image_entry_id,
        )
        # 5. AC-AL05 两步确认：被引用且未确认时仅返回摘要，不执行替换
        if impact.referenced and not confirm:
            await db.rollback()
            fresh = await asset_crud.get_subject(db, subject_id)
            target = fresh if fresh is not None else subject
            return (
                asset_crud.serialize_subject(
                    target, await _episode_map(db, target.script_id)
                ),
                impact,
            )

    # 6. 数据变更
    await asset_crud.approve_image(db, subject, candidate)

    # 7. 提交
    await db.commit()

    # 刷新序列化（重新加载以获取最新关联状态）
    fresh = await asset_crud.get_subject(db, subject_id)
    target = fresh if fresh is not None else subject
    return (
        asset_crud.serialize_subject(target, await _episode_map(db, target.script_id)),
        impact,
    )


async def approve_audio_with_lock(
    db: AsyncSession, subject_id: str, candidate_id: str
) -> SubjectAssetOut:
    """通过声音 — 挂载 audio_entries。

    流程:
    1. SELECT FOR UPDATE 锁定主体行
    2. 校验候选归属与类型
    3. 设 is_approved=True（进入 audio_entries）
    4. 提交事务
    """
    subject = await asset_crud.get_subject_for_update(db, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")

    candidate = await asset_crud.get_candidate(db, candidate_id)
    if candidate is None:
        raise NotFoundError("候选资产不存在")

    if not _candidate_belongs_to_subject(subject, candidate):
        raise ForbiddenError("候选资产不属于该主体")
    if candidate.kind != "audio":
        raise ConflictError("该候选不是声音类型，无法通过为 audio_entry")
    if candidate.status != "candidate":
        await db.rollback()
        raise ConflictError("只有候选状态的声音可以批准")

    await asset_crud.approve_audio(db, subject, candidate)
    await db.commit()

    fresh = await asset_crud.get_subject(db, subject_id)
    target = fresh if fresh is not None else subject
    return asset_crud.serialize_subject(target, await _episode_map(db, target.script_id))


async def remove_candidate_with_recalc(
    db: AsyncSession, subject_id: str, candidate_id: str
) -> SubjectAssetOut:
    """移出候选队列 — 删除候选资产条目，并重算审核状态。

    流程:
    1. SELECT FOR UPDATE 锁定主体行
    2. 校验候选归属，且非正式 image_entry
    3. 删除候选
    4. 若无剩余待审候选且 review_status==pending，回退为 not_designed
    5. 提交事务
    """
    subject = await asset_crud.get_subject_for_update(db, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")

    candidate = await asset_crud.get_candidate(db, candidate_id)
    if candidate is None:
        raise NotFoundError("候选资产不存在")

    if not _candidate_belongs_to_subject(subject, candidate):
        raise ForbiddenError("候选资产不属于该主体")

    if candidate.status != "candidate":
        raise ConflictError("只有候选状态的资产可以移出候选队列")

    await asset_crud.remove_candidate(db, candidate)

    # 重算状态：pending 且无剩余待审候选 -> not_designed
    if subject.review_status == STATUS_PENDING:
        images, audios = await asset_crud.list_candidates(db, subject.id)
        remaining = audios if subject.type == "sound" else images
        if len(remaining) == 0:
            subject.review_status = (
                STATUS_APPROVED if subject.image_entry_id else STATUS_NOT_DESIGNED
            )
            await db.flush()

    await db.commit()

    fresh = await asset_crud.get_subject(db, subject_id)
    target = fresh if fresh is not None else subject
    return asset_crud.serialize_subject(target, await _episode_map(db, target.script_id))


# =====================================================================
# 状态机辅助 — 供外部模块（如 T6 生图 worker）调用
# =====================================================================


async def mark_pending_if_has_candidates(
    db: AsyncSession, subject: Subject
) -> bool:
    """若有未通过图片候选且状态为 not_designed，则置为 pending。

    供生图成功后调用（T6 generation worker），将主体从 not_designed -> pending。
    返回是否发生状态转换。
    """
    if subject.review_status != STATUS_NOT_DESIGNED:
        return False
    images, _ = await asset_crud.list_candidates(db, subject.id)
    if len(images) > 0:
        subject.review_status = STATUS_PENDING
        await db.flush()
        return True
    return False
