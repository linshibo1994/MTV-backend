"""Coverage Audition 导演方案 API。"""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ForbiddenError
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.scene_audition import SceneAudition
from app.models.user import User
from app.schemas.scene_audition import (
    SceneAuditionConfirmIn,
    SceneAuditionGenerateIn,
    SceneAuditionListOut,
    SceneAuditionOut,
)
from app.services.audit import log as audit_log
from app.services.scene_auditions import confirm_audition, list_auditions, queue_audition

router = APIRouter()


@router.get(
    "/scripts/{script_id}/episodes/{episode_id}/scene-auditions",
    response_model=SceneAuditionListOut,
)
async def get_scene_auditions(
    script_id: str,
    episode_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SceneAuditionListOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    items, total = await list_auditions(db, script_id=script_id, episode_id=episode_id)
    return SceneAuditionListOut(
        items=[SceneAuditionOut.model_validate(item) for item in items],
        total=total,
    )


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/scene-auditions/generate",
    response_model=SceneAuditionOut,
    status_code=202,
)
async def post_generate_scene_audition(
    script_id: str,
    episode_id: str,
    payload: SceneAuditionGenerateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SceneAuditionOut:
    """通过事务 Outbox 异步生成 2–3 个方案，客户端轮询列表。"""

    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.RUN_AI)
    row = await queue_audition(
        db,
        script_id=script_id,
        episode_id=episode_id,
        **payload.model_dump(),
    )
    await audit_log(
        db,
        actor=user,
        action="scene_audition_generate",
        resource_type="scene_audition",
        resource_id=row.id,
        after={
            "scene_no": row.scene_no,
            "input_version": row.input_version,
            "attempt_count": row.attempt_count,
        },
    )
    await db.commit()
    await db.refresh(row)
    return SceneAuditionOut.model_validate(row)


@router.post(
    "/scripts/{script_id}/episodes/{episode_id}/scene-auditions/{audition_id}/confirm",
    response_model=SceneAuditionOut,
)
async def post_confirm_scene_audition(
    script_id: str,
    episode_id: str,
    audition_id: str,
    payload: SceneAuditionConfirmIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SceneAuditionOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.REVIEW)
    ownership = (
        await db.execute(
            select(SceneAudition.id).where(
                SceneAudition.id == audition_id,
                SceneAudition.script_id == script_id,
                SceneAudition.episode_id == episode_id,
            )
        )
    ).scalar_one_or_none()
    if ownership is None:
        raise ForbiddenError("导演方案不属于当前分集")
    row = await confirm_audition(
        db,
        audition_id=audition_id,
        plan_id=payload.plan_id,
        expected_input_version=payload.expected_input_version,
        actor_user_id=user.user_id,
    )
    await audit_log(
        db,
        actor=user,
        action="scene_audition_confirm",
        resource_type="scene_audition",
        resource_id=row.id,
        after={"selected_plan_id": row.selected_plan_id, "input_version": row.input_version},
    )
    await db.commit()
    await db.refresh(row)
    return SceneAuditionOut.model_validate(row)
