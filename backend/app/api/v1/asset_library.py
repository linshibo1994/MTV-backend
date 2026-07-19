"""资产库模块 API 路由。

端点清单:
  GET    /scripts/{script_id}/asset/entries                        资产条目（列表页取计数）
  GET    /scripts/{script_id}/asset-library/subjects               主体列表
  POST   /scripts/{script_id}/asset-library/subjects               手动新增主体
  GET    /scripts/{script_id}/asset-library/parent-subjects       父主体列表
  PATCH  /scripts/{script_id}/asset-library/subjects/{sid}        编辑主体
  DELETE /scripts/{script_id}/asset-library/subjects/{sid}        删除主体
  GET    /scripts/{script_id}/asset-library/subjects/{sid}/candidates  候选图片+声音列表
  POST   /scripts/{script_id}/asset-library/subjects/{sid}/approve-image
  POST   /scripts/{script_id}/asset-library/subjects/{sid}/approve-audio
  DELETE /scripts/{script_id}/asset-library/subjects/{sid}/candidates/{cid}
  GET    /scripts/{script_id}/canvas-project/ensure               幂等创建/返回 materials_id

prefix 由 Phase C 统一挂载（/v3api），本文件 router 不设 prefix。
"""

from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ForbiddenError, NotFoundError
from app.core.permissions import Action, check_script_access
from app.crud import asset as asset_crud
from app.models.user import User
from app.schemas.asset import (
    ApproveAudioIn,
    ApproveImageIn,
    ApproveResultOut,
    AssetEntryListOut,
    CandidateListOut,
    CanvasProjectEnsureOut,
    ImpactSummaryOut,
    ParentSubjectListOut,
    SubjectAssetOut,
    SubjectChangeSuggestionListOut,
    SubjectChangeSuggestionOut,
    SubjectCreateIn,
    SubjectListOut,
    SubjectSuggestionResolveIn,
    SubjectUpdateIn,
)
from app.services import review as review_service
from app.services.audit import best_effort_log as audit_log
from app.services.audit import log as strict_audit_log

router = APIRouter()


# =====================================================================
# 辅助 — 加载剧本并校验访问权限
# =====================================================================


async def _load_script_or_403(
    db: AsyncSession, script_id: str, user: User, action: Action = Action.READ
):
    """加载剧本，校验当前用户访问权限。不存在 -> 404，无权 -> 403/401。

    调用方按操作语义传 action=Action.READ（查询类）/Action.EDIT（写操作类）。
    """
    script = await asset_crud.get_script(db, script_id)
    if script is None:
        raise NotFoundError("剧本不存在")
    await check_script_access(db, user, script, action=action)
    return script


async def _load_subject_for_script(
    db: AsyncSession, script_id: str, subject_id: str
):
    """加载主体：真不存在为 404，已存在但不归属当前剧本为 403。"""
    subject = await asset_crud.get_subject(db, subject_id)
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != script_id:
        raise ForbiddenError("主体不属于当前剧本")
    return subject


# =====================================================================
# 资产条目（列表页取计数）
# =====================================================================


@router.get("/scripts/{script_id}/asset/entries", response_model=AssetEntryListOut)
async def list_asset_entries(
    script_id: str,
    limit: int = Query(30, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> AssetEntryListOut:
    """资产条目列表 — 列表页用 limit=1&offset=0 拿 total 计数。"""
    await _load_script_or_403(db, script_id, user, action=Action.READ)
    items, total = await asset_crud.list_entries(db, script_id, limit, offset)
    return AssetEntryListOut(
        items=[asset_crud.serialize_entry(e) for e in items],
        total=total,
    )


# =====================================================================
# 主体列表 / 父主体
# =====================================================================


@router.get(
    "/scripts/{script_id}/asset-library/subjects", response_model=SubjectListOut
)
async def list_subjects(
    script_id: str,
    limit: int = Query(30, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectListOut:
    """主体列表 — 含 image_entry/audio_entries/pending_candidate_count/review_status_label。"""
    await _load_script_or_403(db, script_id, user, action=Action.READ)
    subjects, total = await asset_crud.list_subjects(db, script_id, limit, offset)
    episode_map = await asset_crud.get_episode_id_map(db, script_id)
    return SubjectListOut(
        items=[asset_crud.serialize_subject(s, episode_map) for s in subjects],
        total=total,
    )


@router.post(
    "/scripts/{script_id}/asset-library/subjects",
    response_model=SubjectAssetOut,
    status_code=201,
)
async def create_subject(
    script_id: str,
    payload: SubjectCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectAssetOut:
    """手动新增角色、场景、道具或声音主体。"""
    await _load_script_or_403(db, script_id, user, action=Action.EDIT)
    subject = await asset_crud.create_subject(
        db,
        script_id=script_id,
        subject_type=payload.type,
        name=payload.name,
        description=payload.description,
        episode_nos=[int(episode_no) for episode_no in payload.episode_nos],
    )
    # 主体与审计共用同一事务：审计失败时请求依赖统一 rollback，不能留下无审计主体。
    await strict_audit_log(
        db,
        actor=user,
        action="subject_create",
        resource_type="subject",
        resource_id=subject.id,
        before=None,
        after={
            "script_id": script_id,
            "type": subject.type,
            "name": subject.name,
            "description": subject.description,
            "episode_nos": list(subject.episode_nos or []),
            "review_status": subject.review_status,
        },
    )
    await db.commit()

    fresh = await asset_crud.get_subject(db, subject.id)
    target = fresh if fresh is not None else subject
    return asset_crud.serialize_subject(
        target,
        await asset_crud.get_episode_id_map(db, script_id),
    )


@router.get(
    "/scripts/{script_id}/asset-library/parent-subjects",
    response_model=ParentSubjectListOut,
)
async def list_parent_subjects(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ParentSubjectListOut:
    """父主体列表 — 父主体分组（child_count 形态数）。"""
    await _load_script_or_403(db, script_id, user, action=Action.READ)
    parents = await asset_crud.list_parent_subjects(db, script_id)
    return ParentSubjectListOut(
        items=[asset_crud.serialize_parent_subject(p) for p in parents]
    )


# =====================================================================
# 主体编辑 / 删除
# =====================================================================


@router.patch(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}",
    response_model=SubjectAssetOut,
)
async def update_subject(
    script_id: str,
    subject_id: str,
    payload: SubjectUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectAssetOut:
    """编辑主体 — name/description/type/episode_nos。"""
    await _load_script_or_403(db, script_id, user, action=Action.EDIT)

    subject = await asset_crud.get_subject_for_identity_update(
        db, script_id, subject_id
    )
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != script_id:
        raise ForbiddenError("主体不属于当前剧本")
    before = {
        "name": subject.name,
        "description": subject.description,
        "name_locked": bool(subject.name_locked),
        "description_locked": bool(subject.description_locked),
        "subject_version": int(subject.subject_version or 1),
    }

    update_data = payload.model_dump(exclude_none=True)
    expected_subject_version = update_data.pop("expected_subject_version", None)
    await asset_crud.update_subject(
        db,
        subject,
        update_data,
        expected_subject_version=expected_subject_version,
    )
    await audit_log(
        db,
        actor=user,
        action="subject_update",
        resource_type="subject",
        resource_id=subject_id,
        before=before,
        after={
            "name": subject.name,
            "description": subject.description,
            "name_locked": bool(subject.name_locked),
            "description_locked": bool(subject.description_locked),
            "subject_version": int(subject.subject_version or 1),
        },
    )
    await db.commit()

    fresh = await asset_crud.get_subject(db, subject_id)
    target = fresh if fresh is not None else subject
    return asset_crud.serialize_subject(target, await asset_crud.get_episode_id_map(db, script_id))


@router.get(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions",
    response_model=SubjectChangeSuggestionListOut,
)
async def list_subject_suggestions(
    script_id: str,
    subject_id: str,
    status: Literal["pending", "accepted", "ignored", "all"] = Query("pending"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectChangeSuggestionListOut:
    """查询人工锁字段的流水线差异建议。"""
    await _load_script_or_403(db, script_id, user, action=Action.READ)
    await _load_subject_for_script(db, script_id, subject_id)
    items, total = await asset_crud.list_subject_suggestions(
        db,
        script_id,
        subject_id,
        None if status == "all" else status,
    )
    return SubjectChangeSuggestionListOut(
        items=[SubjectChangeSuggestionOut.model_validate(item) for item in items],
        total=total,
    )


@router.post(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/suggestions/{suggestion_id}/resolve",
    response_model=SubjectChangeSuggestionOut,
)
async def resolve_subject_suggestion(
    script_id: str,
    subject_id: str,
    suggestion_id: str,
    payload: SubjectSuggestionResolveIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectChangeSuggestionOut:
    """接受或忽略差异建议；接受使用主体版本栅栏防并发覆盖。"""
    await _load_script_or_403(db, script_id, user, action=Action.EDIT)
    subject = await asset_crud.get_subject_for_identity_update(
        db, script_id, subject_id
    )
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != script_id:
        raise ForbiddenError("主体不属于当前剧本")
    before = {
        "name": subject.name,
        "description": subject.description,
        "subject_version": int(subject.subject_version or 1),
    }
    suggestion = await asset_crud.resolve_subject_suggestion(
        db,
        subject,
        suggestion_id,
        action=payload.action,
        expected_subject_version=payload.expected_subject_version,
        actor_user_id=user.user_id,
    )
    await audit_log(
        db,
        actor=user,
        action=f"subject_suggestion_{payload.action}",
        resource_type="subject_change_suggestion",
        resource_id=suggestion_id,
        before={**before, "suggestion_status": "pending"},
        after={
            "name": subject.name,
            "description": subject.description,
            "subject_version": int(subject.subject_version or 1),
            "suggestion_status": suggestion.status,
        },
    )
    await db.commit()
    return SubjectChangeSuggestionOut.model_validate(suggestion)


@router.delete(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}",
)
async def delete_subject(
    script_id: str,
    subject_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict:
    """删除主体 — 连带清理其资产条目。"""
    await _load_script_or_403(db, script_id, user, action=Action.EDIT)

    subject = await asset_crud.get_subject_for_identity_update(
        db, script_id, subject_id
    )
    if subject is None:
        raise NotFoundError("主体不存在")
    if subject.script_id != script_id:
        raise ForbiddenError("主体不属于当前剧本")
    before = {
        "name": subject.name,
        "type": subject.type,
        "review_status": subject.review_status,
        "image_entry_id": subject.image_entry_id,
    }

    try:
        await asset_crud.delete_subject(db, subject)
        await strict_audit_log(
            db,
            actor=user,
            action="subject_delete",
            resource_type="subject",
            resource_id=subject_id,
            before=before,
            after=None,
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return {"detail": "已删除"}


# =====================================================================
# 候选列表 / 审核
# =====================================================================


@router.get(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates",
    response_model=CandidateListOut,
)
async def list_candidates(
    script_id: str,
    subject_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> CandidateListOut:
    """候选图片+声音列表 — 未通过审核的候选。"""
    await _load_script_or_403(db, script_id, user, action=Action.READ)

    await _load_subject_for_script(db, script_id, subject_id)

    images, audios = await asset_crud.list_candidates(db, subject_id)
    return CandidateListOut(
        image_candidates=[asset_crud._entry_to_candidate(e) for e in images],
        audio_candidates=[asset_crud._entry_to_candidate(e) for e in audios],
    )


@router.post(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image",
    response_model=ApproveResultOut,
)
async def approve_image(
    script_id: str,
    subject_id: str,
    payload: ApproveImageIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ApproveResultOut:
    """通过图片 — 设为 image_entry，review_status=approved。

    并发控制（AC-AL04）：expected_image_entry_id 乐观锁信号，后提交者收到 409。
    替换被引用资产两步确认（AC-AL05）：confirm=False 仅返回影响摘要不执行，
    confirm=True 执行替换。两审核者并发用 SELECT FOR UPDATE 防覆盖。
    """
    await _load_script_or_403(db, script_id, user, action=Action.REVIEW)

    subject = await _load_subject_for_script(db, script_id, subject_id)
    before_image_entry_id = subject.image_entry_id

    subject_out, impact = await review_service.approve_image_with_lock(
        db,
        subject_id,
        payload.candidate_id,
        expected_image_entry_id=payload.expected_image_entry_id,
        confirm=payload.confirm,
    )
    approved_entry = subject_out.image_entry
    if approved_entry is not None and approved_entry.asset_id == payload.candidate_id:
        await audit_log(
            db,
            actor=user,
            action="asset_approve_image",
            resource_type="asset_entry",
            resource_id=payload.candidate_id,
            before={"image_entry_id": before_image_entry_id},
            after={"image_entry_id": payload.candidate_id},
            commit=True,
        )

    return ApproveResultOut(
        subject=subject_out,
        impact=impact,
    )


@router.get(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/replace-impact",
    response_model=ImpactSummaryOut,
)
async def get_replace_impact(
    script_id: str,
    subject_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ImpactSummaryOut:
    """替换影响摘要预检查（AC-AL05）— 替换正式资产前先展示影响并要求确认。

    返回引用该主体的镜头组列表；前端据此提示用户确认后再调 approve-image
    （confirm=True）执行替换。
    """
    await _load_script_or_403(db, script_id, user, action=Action.REVIEW)

    await _load_subject_for_script(db, script_id, subject_id)

    return await review_service.get_replace_impact(db, subject_id)


@router.post(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-audio",
    response_model=SubjectAssetOut,
)
async def approve_audio(
    script_id: str,
    subject_id: str,
    payload: ApproveAudioIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectAssetOut:
    """通过声音 — 挂载 audio_entries。"""
    await _load_script_or_403(db, script_id, user, action=Action.REVIEW)

    await _load_subject_for_script(db, script_id, subject_id)

    result = await review_service.approve_audio_with_lock(
        db, subject_id, payload.candidate_id
    )
    await audit_log(
        db,
        actor=user,
        action="asset_approve_audio",
        resource_type="asset_entry",
        resource_id=payload.candidate_id,
        before={"subject_id": subject_id, "status": "candidate"},
        after={"subject_id": subject_id, "status": "approved"},
        commit=True,
    )
    return result


@router.delete(
    "/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates/{candidate_id}",
    response_model=SubjectAssetOut,
)
async def remove_candidate(
    script_id: str,
    subject_id: str,
    candidate_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectAssetOut:
    """移出候选队列 — 删除候选资产条目，重算审核状态。"""
    await _load_script_or_403(db, script_id, user, action=Action.REVIEW)

    await _load_subject_for_script(db, script_id, subject_id)

    result = await review_service.remove_candidate_with_recalc(
        db, subject_id, candidate_id
    )
    await audit_log(
        db,
        actor=user,
        action="asset_candidate_delete",
        resource_type="asset_entry",
        resource_id=candidate_id,
        before={"subject_id": subject_id, "status": "candidate"},
        after={"status": "deleted"},
        commit=True,
    )
    return result


# =====================================================================
# 画布项目 ensure
# =====================================================================


@router.get(
    "/scripts/{script_id}/canvas-project/ensure",
    response_model=CanvasProjectEnsureOut,
)
async def ensure_canvas_project(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> CanvasProjectEnsureOut:
    """幂等创建/返回 materials_id — 不存在则创建 Materials。"""
    script = await _load_script_or_403(db, script_id, user, action=Action.EDIT)
    materials, created = await asset_crud.ensure_materials(db, script)
    await db.commit()
    return CanvasProjectEnsureOut(
        materials_id=materials.id,
        name=materials.name,
        script_id=script.id,
        created=created,
    )
